"""
Airbnb API Key & GraphQL Hash 자동 추출기

Playwright를 사용하여 Airbnb 웹사이트에서:
1. X-Airbnb-API-Key (클라이언트 공개 키)
2. GraphQL persistedQuery sha256Hash 값들 (StaysSearch, PdpAvailabilityCalendar 등)
을 자동으로 추출합니다.

로그인 불필요 - 홈페이지 접속 + 검색 한 번이면 충분합니다.
추출된 값은 .api_credentials.json에 캐시됩니다.
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


async def extract_api_credentials(headless: bool = True,
                                   force_refresh: bool = False) -> dict[str, Any]:
    """
    Playwright로 Airbnb에서 API key + GraphQL hash를 추출합니다.

    Args:
        headless: 브라우저를 헤드리스로 실행할지 여부
        force_refresh: 캐시 무시하고 강제 재추출

    Returns:
        {
            "api_key": "d306...",
            "hashes": {
                "StaysSearch": "abc123...",
                "PdpAvailabilityCalendar": "def456...",
                "StayListing": "ghi789...",
                ...
            },
            "cached_at": 1234567890.0
        }
    """
    # 캐시 확인
    if not force_refresh:
        cached = _load_cache()
        if cached:
            return cached

    logger.info("Extracting API credentials from Airbnb (headless=%s)...", headless)

    from playwright.async_api import async_playwright

    credentials = {
        "api_key": "",
        "hashes": {},
    }

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ],
        )

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

        # WebDriver 플래그 제거
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => false });
        """)

        page = await context.new_page()

        # 네트워크 요청 가로채기
        api_requests_captured = []

        async def on_request(request):
            url = request.url
            if "/api/v3/" in url or "StaysSearch" in url:
                headers = request.headers
                api_key = headers.get("x-airbnb-api-key", "")
                if api_key and not credentials["api_key"]:
                    credentials["api_key"] = api_key
                    logger.info("Captured API key: %s...%s", api_key[:8], api_key[-4:])

                # URL에서 extensions 파라미터 추출 (sha256Hash 포함)
                try:
                    from urllib.parse import parse_qs, urlparse
                    parsed = urlparse(url)
                    params = parse_qs(parsed.query)

                    op_name = ""
                    if "operationName" in params:
                        op_name = params["operationName"][0]

                    if "extensions" in params:
                        ext = json.loads(params["extensions"][0])
                        sha_hash = ext.get("persistedQuery", {}).get("sha256Hash", "")
                        if sha_hash and op_name:
                            credentials["hashes"][op_name] = sha_hash
                            logger.info("Captured hash for %s: %s...", op_name, sha_hash[:16])
                except Exception as e:
                    logger.debug("Error parsing request params: %s", e)

                api_requests_captured.append(url)

        page.on("request", on_request)

        try:
            # 1. Airbnb 서울 검색 페이지 접속 (검색 API가 자동 호출됨)
            logger.info("Step 1: Loading Airbnb Seoul search page...")
            await page.goto(
                "https://www.airbnb.co.kr/s/Seoul/homes",
                wait_until="networkidle",
                timeout=30000,
            )
            await page.wait_for_timeout(3000)

            # 2. API 키가 안 잡혔으면 JS 번들에서 직접 추출
            if not credentials["api_key"]:
                logger.info("Step 2: Extracting API key from JS context...")
                api_key_from_js = await page.evaluate("""
                    () => {
                        // 방법 1: __NEXT_DATA__ 에서 추출
                        const nextData = document.getElementById('__NEXT_DATA__');
                        if (nextData) {
                            const text = nextData.textContent;
                            const match = text.match(/"key":"([a-z0-9]+)"/);
                            if (match) return match[1];
                        }

                        // 방법 2: 전역 변수에서 추출
                        if (window.__airbnb_bootstrapped_data__) {
                            const data = JSON.stringify(window.__airbnb_bootstrapped_data__);
                            const match = data.match(/"key":"([a-z0-9]+)"/);
                            if (match) return match[1];
                        }

                        // 방법 3: meta 태그에서 추출
                        const metas = document.querySelectorAll('meta');
                        for (const meta of metas) {
                            const content = meta.getAttribute('content') || '';
                            if (content.match(/^[a-z0-9]{32,}$/)) return content;
                        }

                        return '';
                    }
                """)
                if api_key_from_js:
                    credentials["api_key"] = api_key_from_js
                    logger.info("Extracted API key from JS: %s...%s",
                                api_key_from_js[:8], api_key_from_js[-4:])

            # 3. 해시가 부족하면 페이지 스크롤로 추가 API 호출 유도
            if len(credentials["hashes"]) < 1:
                logger.info("Step 3: Scrolling to trigger more API calls...")
                for _ in range(3):
                    await page.evaluate("window.scrollBy(0, 800)")
                    await page.wait_for_timeout(2000)

            # 4. 개별 숙소 페이지 접속 (캘린더/상세 해시 추출)
            if "PdpAvailabilityCalendar" not in credentials["hashes"]:
                logger.info("Step 4: Visiting a listing page for calendar hash...")
                # 검색 결과에서 첫 번째 숙소 링크 찾기
                listing_link = await page.evaluate("""
                    () => {
                        const links = document.querySelectorAll('a[href*="/rooms/"]');
                        for (const link of links) {
                            const href = link.getAttribute('href');
                            if (href && href.match(/\\/rooms\\/\\d+/)) {
                                return href;
                            }
                        }
                        return '';
                    }
                """)

                if listing_link:
                    full_url = listing_link if listing_link.startswith("http") \
                        else f"https://www.airbnb.co.kr{listing_link}"
                    logger.info("Visiting listing: %s", full_url[:60])
                    await page.goto(full_url, wait_until="networkidle", timeout=30000)
                    await page.wait_for_timeout(3000)

                    # 캘린더 열기 시도
                    try:
                        calendar_btn = page.locator('[data-testid="availability-calendar"],'
                                                     'button:has-text("날짜"),'
                                                     'button:has-text("체크인")')
                        if await calendar_btn.count() > 0:
                            await calendar_btn.first.click()
                            await page.wait_for_timeout(2000)
                    except Exception:
                        pass

            # 5. JS 번들에서 해시 추출 (백업 방법)
            if len(credentials["hashes"]) < 2:
                logger.info("Step 5: Scanning JS bundles for GraphQL hashes...")
                hashes_from_js = await _extract_hashes_from_scripts(page)
                for op_name, hash_val in hashes_from_js.items():
                    if op_name not in credentials["hashes"]:
                        credentials["hashes"][op_name] = hash_val
                        logger.info("Found hash from JS bundle: %s = %s...",
                                    op_name, hash_val[:16])

        except Exception as e:
            logger.error("Error during extraction: %s", e)
        finally:
            await browser.close()

    # 결과 검증 및 저장
    if credentials["api_key"]:
        _save_cache(credentials)
        logger.info(
            "Extraction complete: api_key=%s, %d operation hashes captured",
            f"{credentials['api_key'][:8]}...{credentials['api_key'][-4:]}",
            len(credentials["hashes"]),
        )
    else:
        logger.error(
            "Failed to extract API key. Airbnb may have changed its structure. "
            "Try with headless=False to debug visually."
        )

    return credentials


async def _extract_hashes_from_scripts(page) -> dict[str, str]:
    """
    페이지의 JS 소스코드에서 GraphQL operation hash를 추출합니다.

    Airbnb의 webpack 번들에는 persistedQuery hash가 하드코딩되어 있습니다.
    """
    hashes = {}

    # 관심 있는 operation 이름들
    target_ops = [
        "StaysSearch",
        "PdpAvailabilityCalendar",
        "StayListing",
        "StaysPdpSections",
        "ExploreSearch",
    ]

    try:
        # 방법 1: 인라인 스크립트 + __NEXT_DATA__에서 추출
        all_scripts_text = await page.evaluate("""
            () => {
                const scripts = document.querySelectorAll('script');
                let text = '';
                for (const s of scripts) {
                    if (s.textContent && s.textContent.length > 100) {
                        text += s.textContent + '\\n';
                    }
                }
                return text.substring(0, 500000);  // 500KB 제한
            }
        """)

        for op in target_ops:
            # 패턴: "operationName":"StaysSearch"..."sha256Hash":"abc123"
            patterns = [
                rf'"{op}"[^}}]{{0,500}}"sha256Hash"\s*:\s*"([a-f0-9]{{64}})"',
                rf'"sha256Hash"\s*:\s*"([a-f0-9]{{64}})"[^}}]{{0,500}}"{op}"',
                rf'{op}[^"]*"[^"]*"([a-f0-9]{{64}})"',
            ]
            for pattern in patterns:
                match = re.search(pattern, all_scripts_text)
                if match:
                    hashes[op] = match.group(1)
                    break

        # 방법 2: 외부 JS 파일 내용 검색 (로드된 것만)
        if len(hashes) < 2:
            js_urls = await page.evaluate("""
                () => {
                    return Array.from(document.querySelectorAll('script[src]'))
                        .map(s => s.src)
                        .filter(src => src.includes('_next') || src.includes('chunk'));
                }
            """)

            for js_url in js_urls[:5]:  # 최대 5개 번들만 확인
                try:
                    response = await page.request.get(js_url)
                    if response.ok:
                        content = await response.text()
                        for op in target_ops:
                            if op in hashes:
                                continue
                            for pattern in [
                                rf'"{op}"[^}}]{{0,500}}"sha256Hash"\s*:\s*"([a-f0-9]{{64}})"',
                                rf'"sha256Hash"\s*:\s*"([a-f0-9]{{64}})"[^}}]{{0,500}}"{op}"',
                            ]:
                                match = re.search(pattern, content)
                                if match:
                                    hashes[op] = match.group(1)
                                    break
                except Exception:
                    continue

    except Exception as e:
        logger.debug("Error extracting hashes from scripts: %s", e)

    return hashes


def get_cached_credentials() -> dict[str, Any] | None:
    """캐시된 credentials를 반환합니다 (동기 함수)."""
    return _load_cache()


def get_api_key_sync() -> str:
    """
    API 키를 동기적으로 반환합니다.
    캐시가 있으면 캐시에서, 없으면 새로 추출합니다.
    """
    cached = _load_cache()
    if cached and cached.get("api_key"):
        return cached["api_key"]

    # 새로 추출
    credentials = asyncio.run(extract_api_credentials())
    return credentials.get("api_key", "")


def get_operation_hash(operation_name: str) -> str:
    """특정 GraphQL operation의 sha256Hash를 반환합니다."""
    cached = _load_cache()
    if cached:
        return cached.get("hashes", {}).get(operation_name, "")
    return ""


# CLI로 직접 실행 가능
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
