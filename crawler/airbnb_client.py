"""
Airbnb 내부 API 클라이언트

Airbnb GraphQL API v3를 사용하여 데이터를 가져옵니다.
- StaysSearch: 검색 결과 (숙소 목록)
- PdpAvailabilityCalendar: 캘린더 (가용성/가격)
- StayListing: 숙소 상세 정보

TLS 지문 위장을 위해 curl_cffi를 사용합니다.
API 키와 GraphQL hash는 api_key_extractor로 자동 추출됩니다.
"""

import base64
import hashlib
import json
import logging
import random
from datetime import date, timedelta
from typing import Any

from config.settings import (
    AIRBNB_API_BASE,
    AIRBNB_API_KEY,
    CURRENCY,
    DEFAULT_GUESTS,
    SEARCH_RADIUS_KM,
    USER_AGENTS,
)
from crawler.api_key_extractor import get_cached_credentials, get_operation_hash
from crawler.proxy_manager import ProxyManager
from crawler.rate_limiter import BlockType, RateLimiter

logger = logging.getLogger(__name__)

# Airbnb API 엔드포인트
API_URL = f"{AIRBNB_API_BASE}/api/v3"
SEARCH_OPERATION = "StaysSearch"
CALENDAR_OPERATION = "PdpAvailabilityCalendar"
LISTING_OPERATION = "StaysPdpSections"


def _build_headers(api_key: str) -> dict[str, str]:
    """Airbnb API 요청에 필요한 헤더를 생성합니다."""
    ua = random.choice(USER_AGENTS)
    return {
        "User-Agent": ua,
        "Accept": "application/json",
        "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
        "Content-Type": "application/json",
        "X-Airbnb-API-Key": api_key,
        "X-Airbnb-Currency": CURRENCY,
        "X-Airbnb-Locale": "ko",
        "Referer": f"{AIRBNB_API_BASE}/s/Seoul/homes",
        "Origin": AIRBNB_API_BASE,
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Ch-Ua-Platform": '"Windows"',
    }


class AirbnbClient:
    """
    Airbnb API 클라이언트.

    RateLimiter + ProxyManager를 내장하여 차단 방지를 자동 처리합니다.
    """

    def __init__(self, api_key: str = "",
                 rate_limiter: RateLimiter | None = None,
                 proxy_manager: ProxyManager | None = None):
        self._api_key = api_key or AIRBNB_API_KEY
        self._rate_limiter = rate_limiter or RateLimiter.from_config()
        self._proxy_manager = proxy_manager or ProxyManager.from_config()
        self._http_client = None

        # 환경변수에 키가 없으면 캐시에서 자동 로드
        if not self._api_key:
            cached = get_cached_credentials()
            if cached and cached.get("api_key"):
                self._api_key = cached["api_key"]
                logger.info("Loaded API key from cache: %s...%s",
                            self._api_key[:8], self._api_key[-4:])

        if not self._api_key:
            logger.warning(
                "AIRBNB_API_KEY is not set. "
                "Run 'python -m crawler.api_key_extractor' to auto-extract, "
                "or set AIRBNB_API_KEY environment variable."
            )

    async def _ensure_client(self):
        """HTTP 클라이언트를 지연 초기화합니다."""
        if self._http_client is None:
            try:
                from curl_cffi.requests import AsyncSession
                self._http_client = AsyncSession(impersonate="chrome")
                logger.info("Using curl_cffi for TLS fingerprint impersonation")
            except ImportError:
                import httpx
                self._http_client = httpx.AsyncClient(
                    timeout=30.0,
                    follow_redirects=True,
                )
                logger.warning(
                    "curl_cffi not available. Using httpx (TLS fingerprint may be detected). "
                    "Install with: pip install curl_cffi"
                )

    async def _request(self, url: str, params: dict | None = None,
                       max_retries: int = 3) -> dict[str, Any] | None:
        """
        Rate limit + 프록시 + 재시도를 적용한 GET 요청.

        Returns:
            성공 시 JSON dict, 실패 시 None
        """
        await self._ensure_client()

        for attempt in range(max_retries):
            await self._rate_limiter.wait()

            headers = _build_headers(self._api_key)
            proxy = self._proxy_manager.get_proxy()

            try:
                kwargs = {"headers": headers, "params": params}
                if proxy:
                    kwargs["proxy"] = proxy

                # curl_cffi vs httpx 분기
                if hasattr(self._http_client, "impersonate"):
                    response = await self._http_client.get(url, **kwargs)
                    status = response.status_code
                    text = response.text
                else:
                    # httpx
                    if proxy:
                        kwargs["proxies"] = {"all://": proxy}
                        del kwargs["proxy"]
                    response = await self._http_client.get(url, **kwargs)
                    status = response.status_code
                    text = response.text

                # 차단 감지
                block_type = self._rate_limiter.detect_block(status, text)
                if block_type != BlockType.NONE:
                    self._rate_limiter.report_failure(block_type)
                    if proxy:
                        self._proxy_manager.report_blocked()
                    logger.warning(
                        "Request blocked (attempt %d/%d, type=%s, status=%d)",
                        attempt + 1, max_retries, block_type.value, status,
                    )
                    continue

                # 성공
                self._rate_limiter.report_success()
                if proxy:
                    self._proxy_manager.report_success()

                data = json.loads(text)
                return data

            except json.JSONDecodeError:
                logger.error("Invalid JSON response (status=%d)", status)
                self._rate_limiter.report_failure()
                continue
            except Exception as e:
                logger.error("Request error (attempt %d/%d): %s", attempt + 1, max_retries, e)
                self._rate_limiter.report_failure()
                continue

        logger.error("All %d retries exhausted for URL: %s", max_retries, url[:100])
        return None

    async def search_stays(self, lat: float, lng: float,
                           checkin: date | None = None,
                           checkout: date | None = None,
                           guests: int = DEFAULT_GUESTS,
                           cursor: str | None = None) -> dict[str, Any] | None:
        """
        역 좌표 중심으로 숙소를 검색합니다.

        Args:
            lat, lng: 검색 중심 좌표
            checkin, checkout: 체크인/체크아웃 날짜 (없으면 내일~모레)
            guests: 게스트 수
            cursor: 페이지네이션 커서

        Returns:
            검색 결과 dict (숙소 목록, 가격, 총 개수 등)
        """
        if checkin is None:
            checkin = date.today() + timedelta(days=1)
        if checkout is None:
            checkout = checkin + timedelta(days=1)

        # 반경을 위도/경도 오프셋으로 변환 (대략적)
        lat_offset = SEARCH_RADIUS_KM / 111.0
        lng_offset = SEARCH_RADIUS_KM / (111.0 * 0.85)  # 서울 위도 보정

        treatment_flags = [
            "feed_map_decouple_m11_treatment",
            "recommended_amenities_2024_treatment_b",
            "filter_redesign_2024_treatment",
            "filter_reordering_2024_roomtype_treatment",
            "p2_category_bar_removal_treatment",
            "selected_filters_2024_treatment",
            "recommended_filters_2024_treatment_b",
        ]

        # 지도/리스트 공통 rawParams
        base_params = [
            {"filterName": "adults", "filterValues": [str(guests)]},
            {"filterName": "cdnCacheSafe", "filterValues": ["false"]},
            {"filterName": "checkin", "filterValues": [checkin.isoformat()]},
            {"filterName": "checkout", "filterValues": [checkout.isoformat()]},
            {"filterName": "ne_lat", "filterValues": [str(lat + lat_offset)]},
            {"filterName": "ne_lng", "filterValues": [str(lng + lng_offset)]},
            {"filterName": "sw_lat", "filterValues": [str(lat - lat_offset)]},
            {"filterName": "sw_lng", "filterValues": [str(lng - lng_offset)]},
            {"filterName": "refinementPaths", "filterValues": ["/homes"]},
            {"filterName": "screenSize", "filterValues": ["large"]},
            {"filterName": "tabId", "filterValues": ["home_tab"]},
            {"filterName": "version", "filterValues": ["1.8.8"]},
            {"filterName": "search_type", "filterValues": ["filter_change"]},
        ]
        if cursor:
            base_params.append({"filterName": "cursor", "filterValues": [cursor]})

        # 리스트용 rawParams (itemsPerGrid 추가)
        list_params = base_params + [
            {"filterName": "itemsPerGrid", "filterValues": ["18"]},
        ]

        params = {
            "operationName": SEARCH_OPERATION,
            "locale": "ko",
            "currency": CURRENCY,
            "variables": json.dumps({
                "aiSearchEnabled": False,
                "isLeanTreatment": False,
                "skipExtendedSearchParams": False,
                "staysMapSearchRequestV2": {
                    "metadataOnly": False,
                    "rawParams": base_params,
                    "requestedPageType": "STAYS_SEARCH",
                    "treatmentFlags": treatment_flags,
                },
                "staysSearchRequest": {
                    "maxMapItems": 9999,
                    "metadataOnly": False,
                    "rawParams": list_params,
                    "requestedPageType": "STAYS_SEARCH",
                    "treatmentFlags": treatment_flags,
                },
            }),
            "extensions": json.dumps({
                "persistedQuery": {
                    "version": 1,
                    "sha256Hash": get_operation_hash(SEARCH_OPERATION),
                },
            }),
        }

        url = f"{API_URL}/{SEARCH_OPERATION}"
        return await self._request(url, params)

    async def get_calendar(self, listing_id: str,
                           month: int, year: int,
                           count: int = 3) -> dict[str, Any] | None:
        """
        숙소의 캘린더(가용성/가격)를 조회합니다.

        Args:
            listing_id: Airbnb 숙소 ID
            month, year: 시작 월/년
            count: 조회할 월 수 (기본 3개월)
        """
        params = {
            "operationName": CALENDAR_OPERATION,
            "locale": "ko",
            "currency": CURRENCY,
            "variables": json.dumps({
                "request": {
                    "count": count,
                    "listingId": listing_id,
                    "month": month,
                    "year": year,
                },
            }),
            "extensions": json.dumps({
                "persistedQuery": {
                    "version": 1,
                    "sha256Hash": get_operation_hash(CALENDAR_OPERATION),
                },
            }),
        }

        url = f"{API_URL}/{CALENDAR_OPERATION}"
        return await self._request(url, params)

    async def get_listing_detail(self, listing_id: str) -> dict[str, Any] | None:
        """
        숙소 상세 정보를 조회합니다 (StaysPdpSections API).

        Args:
            listing_id: Airbnb 숙소 ID (숫자 문자열)
        """
        # ID를 base64 인코딩
        stay_id = base64.b64encode(
            f"StayListing:{listing_id}".encode()
        ).decode()
        demand_id = base64.b64encode(
            f"DemandStayListing:{listing_id}".encode()
        ).decode()

        variables = {
            "categoryTag": None,
            "demandStayListingId": demand_id,
            "federatedSearchId": None,
            "id": stay_id,
            "includeGpDescriptionFragment": True,
            "includeGpHighlightsFragment": True,
            "includeGpNavFragment": True,
            "includeGpNavMobileFragment": True,
            "includeGpReportToAirbnbFragment": True,
            "includeGpReviewsEmptyFragment": True,
            "includeGpReviewsFragment": True,
            "includeGpTitleFragment": True,
            "includeHotelFragments": True,
            "includePdpMigrationDescriptionFragment": False,
            "includePdpMigrationHighlightsFragment": False,
            "includePdpMigrationNavFragment": False,
            "includePdpMigrationNavMobileFragment": False,
            "includePdpMigrationReportToAirbnbFragment": False,
            "includePdpMigrationReviewsEmptyFragment": False,
            "includePdpMigrationReviewsFragment": False,
            "includePdpMigrationTitleFragment": False,
            "p3ImpressionId": f"p3_{int(__import__('time').time())}_crawl",
            "pdpSectionsRequest": {
                "adults": str(DEFAULT_GUESTS),
                "amenityFilters": None,
                "bypassTargetings": False,
                "categoryTag": None,
                "causeId": None,
                "checkIn": None,
                "checkOut": None,
                "children": None,
                "disasterId": None,
                "discountedGuestFeeVersion": None,
                "federatedSearchId": None,
                "forceBoostPriorityMessageType": None,
                "hostPreview": False,
                "infants": None,
                "interactionType": None,
                "layouts": ["SIDEBAR", "SINGLE_COLUMN"],
                "p3ImpressionId": f"p3_{int(__import__('time').time())}_crawl",
                "pdpTypeOverride": None,
                "pets": 0,
                "photoId": None,
                "preview": False,
                "previousStateCheckIn": None,
                "previousStateCheckOut": None,
                "priceDropSource": None,
                "privateBooking": False,
                "promotionUuid": None,
                "relaxedAmenityIds": None,
                "searchId": None,
                "sectionIds": None,
                "selectedCancellationPolicyId": None,
                "selectedRatePlanId": None,
                "splitStays": None,
                "staysBookingMigrationEnabled": False,
                "translateUgc": None,
                "useNewSectionWrapperApi": False,
            },
            "photoId": None,
        }

        params = {
            "operationName": LISTING_OPERATION,
            "locale": "ko",
            "currency": CURRENCY,
            "variables": json.dumps(variables),
            "extensions": json.dumps({
                "persistedQuery": {
                    "version": 1,
                    "sha256Hash": get_operation_hash(LISTING_OPERATION),
                },
            }),
        }

        url = f"{API_URL}/{LISTING_OPERATION}"
        return await self._request(url, params)

    async def close(self):
        """HTTP 클라이언트를 닫습니다."""
        if self._http_client:
            await self._http_client.close() if hasattr(self._http_client, "close") else None
            self._http_client = None

    def get_stats(self) -> dict:
        """Rate Limiter + 프록시 상태를 반환합니다."""
        return {
            "rate_limiter": self._rate_limiter.get_stats(),
            "proxy_manager": self._proxy_manager.get_stats(),
        }

    def compute_response_hash(self, data: dict) -> str:
        """응답 데이터의 해시를 계산합니다 (중복 감지용)."""
        raw = json.dumps(data, sort_keys=True)
        return hashlib.sha256(raw.encode()).hexdigest()[:16]
