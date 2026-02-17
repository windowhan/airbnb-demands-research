"""
Airbnb API Key & GraphQL Hash 자동 추출기

두 가지 방법으로 추출:
1. httpx: 가볍고 빠름. Airbnb HTML + JS 번들에서 정규식으로 추출.
2. Playwright: 브라우저 기반. httpx 실패 시 fallback.

로그인 불필요 - 홈페이지 접속만으로 충분합니다.
추출된 값은 data/.api_credentials.json에 캐시됩니다.
"""

import asyncio
import json
import logging
import re
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

CACHE_FILE = Path(__file__).resolve().parent.parent / "data" / ".api_credentials.json"
CACHE_MAX_AGE_HOURS = 72  # 3일마다 갱신

# 관심 있는 GraphQL operation 이름들
TARGET_OPS = [
    "StaysSearch",
    "PdpAvailabilityCalendar",
    "StaysPdpSections",
    "StaysDetailPagePresentation",
    "ExploreSearch",
]

# 최소한 이 해시들이 있어야 크롤러가 정상 작동
REQUIRED_OPS = {"StaysSearch", "PdpAvailabilityCalendar", "StaysPdpSections"}


def _load_cache() -> dict[str, Any] | None:
    """캐시된 credentials를 로드합니다."""
    if not CACHE_FILE.exists():
        return None

    try:
        data = json.loads(CACHE_FILE.read_text())
        cached_at = data.get("cached_at", 0)
        age_hours = (time.time() - cached_at) / 3600

        if age_hours > CACHE_MAX_AGE_HOURS:
            logger.info("API credentials cache expired (%.1f hours old)", age_hours)
            return None

        if not data.get("api_key"):
            return None

        logger.info("Loaded cached API credentials (%.1f hours old)", age_hours)
        return data
    except (json.JSONDecodeError, KeyError):
        return None


def _save_cache(data: dict[str, Any]):
    """credentials를 캐시 파일에 저장합니다."""
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    data["cached_at"] = time.time()
    CACHE_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False))
    logger.info("Saved API credentials to cache: %s", CACHE_FILE)


# ─── 방법 1: httpx 기반 (가벼움, 브라우저 불필요) ───────────────────

def _extract_api_key_from_html(html: str) -> str:
    """HTML 소스에서 API 키를 추출합니다."""
    # 패턴 1: "key":"d306zoyjsyarp7ifhu67rjxn52tv0t20" 형태
    patterns = [
        r'"key"\s*:\s*"([a-z0-9]{32,})"',
        r'"api_key"\s*:\s*"([a-z0-9]{32,})"',
        r'"AIRBNB_API_KEY"\s*:\s*"([a-z0-9]{32,})"',
        r'x-airbnb-api-key["\s:]+([a-z0-9]{32,})',
    ]
    for pattern in patterns:
        match = re.search(pattern, html, re.IGNORECASE)
        if match:
            return match.group(1)
    return ""


def _extract_hashes_from_text(text: str) -> dict[str, str]:
    """텍스트에서 GraphQL operation hash를 추출합니다."""
    hashes = {}
    for op in TARGET_OPS:
        patterns = [
            # Airbnb 번들: name:'StaysSearch'...operationId:'hex64'
            rf"name:\s*'{op}'[^}}]{{0,300}}operationId:\s*'([a-f0-9]{{64}})'",
            # "operationName":"StaysSearch" 근처의 sha256Hash
            rf'"{op}"[^}}]{{0,500}}"sha256Hash"\s*:\s*"([a-f0-9]{{64}})"',
            rf'"sha256Hash"\s*:\s*"([a-f0-9]{{64}})"[^}}]{{0,500}}"{op}"',
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                hashes[op] = match.group(1)
                break
    return hashes


async def _extract_via_httpx() -> dict[str, Any]:
    """httpx로 Airbnb 페이지를 가져와서 API 키 + 해시를 추출합니다."""
    import httpx

    credentials = {"api_key": "", "hashes": {}}

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
    }

    async with httpx.AsyncClient(
        timeout=30.0,
        follow_redirects=True,
        headers=headers,
    ) as client:
        # ── Phase 1: 검색 페이지에서 API 키 + StaysSearch 해시 ──
        logger.info("[httpx] Fetching Airbnb Seoul search page...")
        resp = await client.get("https://www.airbnb.co.kr/s/Seoul/homes")

        if resp.status_code != 200:
            logger.warning("[httpx] Got status %d from Airbnb", resp.status_code)
            return credentials

        html = resp.text
        logger.info("[httpx] Got %d bytes of HTML", len(html))

        # HTML에서 API 키 추출
        api_key = _extract_api_key_from_html(html)
        if api_key:
            credentials["api_key"] = api_key
            logger.info("[httpx] Found API key from HTML: %s...%s", api_key[:8], api_key[-4:])

        # 인라인 스크립트에서 해시 추출
        hashes = _extract_hashes_from_text(html)
        credentials["hashes"].update(hashes)
        if hashes:
            logger.info("[httpx] Found %d hashes from inline scripts", len(hashes))

        # JS 번들 스캔
        await _scan_js_bundles(client, html, credentials)

        # ── Phase 2: 리스팅 페이지에서 PDP/Calendar 해시 ──
        missing = REQUIRED_OPS - set(credentials["hashes"].keys())
        if missing:
            logger.info("[httpx] Missing hashes: %s. Scanning listing page...", missing)
            await _scan_listing_page(client, html, credentials)

    return credentials


async def _scan_js_bundles(client, html: str, credentials: dict):
    """HTML에 포함된 JS 번들을 스캔하여 API 키와 해시를 추출합니다."""
    js_urls = re.findall(
        r'https://a0\.muscache\.com/[^\"\'\s]+\.js',
        html,
    )
    js_urls += [
        f"https://www.airbnb.co.kr{m}"
        for m in re.findall(r'"(/_next/static/[^"]+\.js)"', html)
    ]
    js_urls = list(dict.fromkeys(js_urls))
    logger.info("[httpx] Found %d JS bundle URLs to scan", len(js_urls))

    for js_url in js_urls[:40]:
        try:
            js_resp = await client.get(js_url)
            if js_resp.status_code != 200:
                continue

            js_text = js_resp.text

            if not credentials["api_key"]:
                key = _extract_api_key_from_html(js_text)
                if key:
                    credentials["api_key"] = key
                    logger.info("[httpx] Found API key from JS: %s...%s", key[:8], key[-4:])

            new_hashes = _extract_hashes_from_text(js_text)
            for op, h in new_hashes.items():
                if op not in credentials["hashes"]:
                    credentials["hashes"][op] = h
                    logger.info("[httpx] Found hash from JS: %s = %s...", op, h[:16])

            if credentials["api_key"] and not (REQUIRED_OPS - set(credentials["hashes"].keys())):
                break

        except Exception as e:
            logger.debug("[httpx] Error fetching %s: %s", js_url[:60], e)
            continue


async def _scan_listing_page(client, search_html: str, credentials: dict):
    """리스팅 페이지와 lazy-loaded 번들에서 PDP/Calendar 해시를 추출합니다.

    PdpAvailabilityCalendar 해시는 검색 페이지 번들에 없고,
    리스팅 페이지의 RoomCalendarModalWrapper 등 lazy-loaded 번들에 있습니다.
    """
    # 검색 결과에서 리스팅 ID 추출 (다양한 패턴 시도)
    listing_id = None

    # 패턴 1: /rooms/숫자
    m = re.search(r'/rooms/(\d{5,})', search_html)
    if m:
        listing_id = m.group(1)

    # 패턴 2: base64 인코딩된 DemandStayListing ID
    if not listing_id:
        m = re.search(r'RGVtYW5kU3RheUxpc3Rpbmc6([A-Za-z0-9+/=]+)', search_html)
        if m:
            try:
                import base64
                decoded = base64.b64decode("RGVtYW5kU3RheUxpc3Rpbmc6" + m.group(1)).decode()
                listing_id = decoded.split(":")[-1]
            except Exception:
                pass

    # 패턴 3: propertyId
    if not listing_id:
        m = re.search(r'"propertyId"\s*:\s*"?(\d{5,})', search_html)
        if m:
            listing_id = m.group(1)

    # Fallback: 서울 인기 리스팅 페이지 직접 접근
    if not listing_id:
        listing_url = "https://www.airbnb.co.kr/s/Seoul/homes"
        # 검색 API를 통해 리스팅 ID를 얻을 수도 있지만,
        # 간단하게 popular listing을 직접 방문
        listing_url = "https://www.airbnb.co.kr/rooms/1394835192052627372"
        logger.info("[httpx] Using fallback listing URL")
    else:
        listing_url = f"https://www.airbnb.co.kr/rooms/{listing_id}"

    logger.info("[httpx] Fetching listing page: %s", listing_url)

    try:
        resp = await client.get(listing_url)
        if resp.status_code != 200:
            logger.warning("[httpx] Got status %d from listing page", resp.status_code)
            return
    except Exception as e:
        logger.warning("[httpx] Error fetching listing page: %s", e)
        return

    listing_html = resp.text
    logger.info("[httpx] Got %d bytes of listing HTML", len(listing_html))

    # 리스팅 페이지 인라인 해시
    hashes = _extract_hashes_from_text(listing_html)
    for op, h in hashes.items():
        if op not in credentials["hashes"]:
            credentials["hashes"][op] = h
            logger.info("[httpx] Found hash from listing HTML: %s = %s...", op, h[:16])

    # 리스팅 페이지 JS 번들 스캔
    await _scan_js_bundles(client, listing_html, credentials)

    # lazy-loaded 번들 스캔 (Calendar, PDP 관련)
    missing = REQUIRED_OPS - set(credentials["hashes"].keys())
    if missing:
        await _scan_lazy_bundles(client, listing_html, credentials)


async def _scan_lazy_bundles(client, html: str, credentials: dict):
    """asyncRequire 번들에서 참조하는 lazy-loaded JS를 스캔합니다.

    PdpAvailabilityCalendar는 RoomCalendarModalWrapper 번들에 있습니다.
    """
    # asyncRequire에서 참조하는 Calendar/PDP 관련 번들 URL 추출
    lazy_patterns = [
        r'(https://a0\.muscache\.com/[^\"\'\s]*RoomCalendar[^\"\'\s]*\.js)',
        r'(https://a0\.muscache\.com/[^\"\'\s]*AvailabilityCalendar[^\"\'\s]*\.js)',
        r'(https://a0\.muscache\.com/[^\"\'\s]*PdpPlatformRoute[^\"\'\s]*\.js)',
    ]

    # HTML에서 직접 찾기
    lazy_urls = []
    for pattern in lazy_patterns:
        lazy_urls.extend(re.findall(pattern, html))

    # asyncRequire 번들에서도 찾기
    async_req_match = re.search(
        r'(https://a0\.muscache\.com/[^\"\'\s]*asyncRequire[^\"\'\s]*\.js)', html
    )
    if async_req_match:
        try:
            ar_resp = await client.get(async_req_match.group(1))
            if ar_resp.status_code == 200:
                ar_text = ar_resp.text
                # Calendar/PDP 관련 번들 경로 추출
                for frag in ["RoomCalendar", "AvailabilityCalendar", "PdpPlatformRoute"]:
                    for m in re.finditer(
                        rf'"([^"]*{frag}[^"]*\.js)"', ar_text
                    ):
                        path = m.group(1)
                        if path.startswith("http"):
                            lazy_urls.append(path)
                        else:
                            lazy_urls.append(
                                f"https://a0.muscache.com/airbnb/static/packages/web/{path}"
                            )
        except Exception as e:
            logger.debug("[httpx] Error scanning asyncRequire: %s", e)

    lazy_urls = list(dict.fromkeys(lazy_urls))
    logger.info("[httpx] Found %d lazy-loaded bundles to scan", len(lazy_urls))

    for url in lazy_urls[:20]:
        try:
            resp = await client.get(url)
            if resp.status_code != 200:
                continue

            new_hashes = _extract_hashes_from_text(resp.text)
            for op, h in new_hashes.items():
                if op not in credentials["hashes"]:
                    credentials["hashes"][op] = h
                    logger.info("[httpx] Found hash from lazy bundle: %s = %s...", op, h[:16])

            if not (REQUIRED_OPS - set(credentials["hashes"].keys())):
                break

        except Exception as e:
            logger.debug("[httpx] Error fetching lazy bundle: %s", e)
            continue


# ─── 방법 2: Playwright 기반 (브라우저 렌더링) ───────────────────

async def _extract_via_playwright(headless: bool = True) -> dict[str, Any]:
    """Playwright로 Airbnb에서 API key + GraphQL hash를 추출합니다."""
    from playwright.async_api import async_playwright

    credentials = {"api_key": "", "hashes": {}}

    async with async_playwright() as p:
        launch_kwargs = {
            "headless": headless,
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ],
        }

        # 설치된 chromium 바이너리를 직접 찾기
        pw_cache = Path.home() / ".cache" / "ms-playwright"
        if pw_cache.exists():
            for candidate in sorted(
                pw_cache.glob("chromium-*/chrome-linux/chrome"), reverse=True
            ):
                launch_kwargs["executable_path"] = str(candidate)
                logger.info("Using chromium at: %s", candidate)
                break

        browser = await p.chromium.launch(**launch_kwargs)

        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            locale="ko-KR",
            timezone_id="Asia/Seoul",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
        )

        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', { get: () => false });"
        )

        page = await context.new_page()

        # 네트워크 요청 가로채기
        async def on_request(request):
            url = request.url
            if "/api/v3/" in url or "StaysSearch" in url:
                hdrs = request.headers
                api_key = hdrs.get("x-airbnb-api-key", "")
                if api_key and not credentials["api_key"]:
                    credentials["api_key"] = api_key
                    logger.info("Captured API key: %s...%s", api_key[:8], api_key[-4:])

                try:
                    from urllib.parse import parse_qs, urlparse
                    parsed = urlparse(url)
                    params = parse_qs(parsed.query)
                    op_name = params.get("operationName", [""])[0]
                    if "extensions" in params:
                        ext = json.loads(params["extensions"][0])
                        sha_hash = ext.get("persistedQuery", {}).get("sha256Hash", "")
                        if sha_hash and op_name:
                            credentials["hashes"][op_name] = sha_hash
                            logger.info("Captured hash for %s: %s...", op_name, sha_hash[:16])
                except Exception as e:
                    logger.debug("Error parsing request params: %s", e)

        page.on("request", on_request)

        try:
            logger.info("Step 1: Loading Airbnb Seoul search page...")
            await page.goto(
                "https://www.airbnb.co.kr/s/Seoul/homes",
                wait_until="networkidle",
                timeout=30000,
            )
            await page.wait_for_timeout(3000)

            # JS 컨텍스트에서 API 키 추출
            if not credentials["api_key"]:
                logger.info("Step 2: Extracting API key from JS context...")
                api_key_from_js = await page.evaluate("""
                    () => {
                        const nextData = document.getElementById('__NEXT_DATA__');
                        if (nextData) {
                            const match = nextData.textContent.match(/"key":"([a-z0-9]+)"/);
                            if (match) return match[1];
                        }
                        if (window.__airbnb_bootstrapped_data__) {
                            const data = JSON.stringify(window.__airbnb_bootstrapped_data__);
                            const match = data.match(/"key":"([a-z0-9]+)"/);
                            if (match) return match[1];
                        }
                        return '';
                    }
                """)
                if api_key_from_js:
                    credentials["api_key"] = api_key_from_js
                    logger.info("Extracted API key from JS: %s...%s",
                                api_key_from_js[:8], api_key_from_js[-4:])

            # 스크롤로 추가 API 호출 유도
            if len(credentials["hashes"]) < 1:
                for _ in range(3):
                    await page.evaluate("window.scrollBy(0, 800)")
                    await page.wait_for_timeout(2000)

            # 숙소 페이지 방문 (캘린더 해시)
            if "PdpAvailabilityCalendar" not in credentials["hashes"]:
                listing_link = await page.evaluate("""
                    () => {
                        const links = document.querySelectorAll('a[href*="/rooms/"]');
                        for (const link of links) {
                            const href = link.getAttribute('href');
                            if (href && href.match(/\\/rooms\\/\\d+/)) return href;
                        }
                        return '';
                    }
                """)
                if listing_link:
                    full_url = (listing_link if listing_link.startswith("http")
                                else f"https://www.airbnb.co.kr{listing_link}")
                    await page.goto(full_url, wait_until="networkidle", timeout=30000)
                    await page.wait_for_timeout(3000)

        except Exception as e:
            logger.error("Playwright extraction error: %s", e)
        finally:
            await browser.close()

    return credentials


# ─── 메인 추출 함수 ─────────────────────────────────────────

async def extract_api_credentials(headless: bool = True,
                                   force_refresh: bool = False) -> dict[str, Any]:
    """
    Airbnb에서 API key + GraphQL hash를 추출합니다.

    httpx를 먼저 시도하고, 실패하면 Playwright fallback.

    Returns:
        {"api_key": "d306...", "hashes": {...}, "cached_at": ...}
    """
    if not force_refresh:
        cached = _load_cache()
        if cached:
            return cached

    # 방법 1: httpx (가볍고 빠름)
    logger.info("Attempting extraction via httpx...")
    credentials = await _extract_via_httpx()

    # httpx로 API 키를 못 찾으면 Playwright 시도
    if not credentials.get("api_key"):
        logger.info("httpx extraction failed. Trying Playwright...")
        try:
            credentials = await _extract_via_playwright(headless=headless)
        except Exception as e:
            logger.error("Playwright extraction also failed: %s", e)

    # 결과 저장
    if credentials.get("api_key"):
        _save_cache(credentials)
        logger.info(
            "Extraction complete: api_key=%s, %d operation hashes",
            f"{credentials['api_key'][:8]}...{credentials['api_key'][-4:]}",
            len(credentials.get("hashes", {})),
        )
    else:
        logger.error(
            "Failed to extract API key from both httpx and Playwright. "
            "Try with --visible to debug, or set AIRBNB_API_KEY manually."
        )

    return credentials


# ─── 동기 헬퍼 ──────────────────────────────────────────────

def get_cached_credentials() -> dict[str, Any] | None:
    """캐시된 credentials를 반환합니다 (동기 함수)."""
    return _load_cache()


def get_api_key_sync() -> str:
    """API 키를 동기적으로 반환합니다."""
    cached = _load_cache()
    if cached and cached.get("api_key"):
        return cached["api_key"]
    credentials = asyncio.run(extract_api_credentials())
    return credentials.get("api_key", "")


def get_operation_hash(operation_name: str) -> str:
    """특정 GraphQL operation의 sha256Hash를 반환합니다."""
    cached = _load_cache()
    if cached:
        return cached.get("hashes", {}).get(operation_name, "")
    return ""


# ─── CLI ─────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    headless = "--visible" not in sys.argv
    force = "--force" in sys.argv

    print(f"\nAirbnb API Credentials Extractor")
    print(f"  headless: {headless}")
    print(f"  force refresh: {force}")
    print()

    creds = asyncio.run(extract_api_credentials(headless=headless, force_refresh=force))

    print(f"\n{'='*50}")
    print(f"  API Key: {creds.get('api_key', 'NOT FOUND')}")
    print(f"  Operation Hashes:")
    for op, h in creds.get("hashes", {}).items():
        print(f"    {op}: {h[:32]}...")
    print(f"{'='*50}\n")
