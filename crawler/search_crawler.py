"""
검색 크롤러 - 역 주변 숙소 목록 수집

각 지하철역 좌표를 기준으로 Airbnb 검색을 실행하고,
결과를 SearchSnapshot과 Listing 테이블에 저장합니다.
"""

import logging
import re
import statistics
from datetime import date, datetime, timedelta
from typing import Any

from crawler.airbnb_client import AirbnbClient
from models.database import session_scope
from models.schema import CrawlLog, Listing, SearchSnapshot, Station

logger = logging.getLogger(__name__)


class SearchCrawler:
    """역 주변 Airbnb 숙소 검색 크롤러."""

    def __init__(self, client: AirbnbClient):
        self._client = client

    async def crawl_station(self, station: Station,
                            checkin: date | None = None,
                            checkout: date | None = None) -> dict[str, Any] | None:
        """
        단일 역에 대해 검색을 실행하고 결과를 DB에 저장합니다.

        Returns:
            저장된 스냅샷 요약 정보 또는 None
        """
        if checkin is None:
            checkin = date.today() + timedelta(days=1)
        if checkout is None:
            checkout = checkin + timedelta(days=1)

        logger.info("Searching near %s (%s) [%s ~ %s]",
                     station.name, station.line, checkin, checkout)

        data = await self._client.search_stays(
            lat=station.latitude,
            lng=station.longitude,
            checkin=checkin,
            checkout=checkout,
        )

        if data is None:
            logger.warning("No data returned for station %s", station.name)
            return None

        return self._save_results(station, data, checkin, checkout)

    def _save_results(self, station: Station, data: dict,
                      checkin: date, checkout: date) -> dict[str, Any]:
        """검색 결과를 파싱하여 DB에 저장합니다."""
        listings_data = self._extract_listings(data)
        prices = [l["price"] for l in listings_data if l.get("price")]

        snapshot_info = {
            "station": station.name,
            "total": len(listings_data),
            "avg_price": statistics.mean(prices) if prices else 0,
            "min_price": min(prices) if prices else 0,
            "max_price": max(prices) if prices else 0,
        }

        with session_scope() as session:
            # 검색 스냅샷 저장
            snapshot = SearchSnapshot(
                station_id=station.id,
                crawled_at=datetime.utcnow(),
                total_listings=len(listings_data),
                avg_price=snapshot_info["avg_price"],
                min_price=snapshot_info["min_price"],
                max_price=snapshot_info["max_price"],
                median_price=statistics.median(prices) if prices else 0,
                available_count=sum(1 for l in listings_data if l.get("available", True)),
                checkin_date=checkin,
                checkout_date=checkout,
                raw_response_hash=self._client.compute_response_hash(data),
            )
            session.add(snapshot)

            # 숙소 정보 upsert
            for item in listings_data:
                airbnb_id = item.get("id")
                if not airbnb_id:
                    continue

                existing = session.query(Listing).filter_by(airbnb_id=str(airbnb_id)).first()
                if existing:
                    existing.last_seen = datetime.utcnow()
                    if item.get("price"):
                        existing.base_price = item["price"]
                else:
                    listing = Listing(
                        airbnb_id=str(airbnb_id),
                        name=item.get("name", ""),
                        room_type=item.get("room_type", ""),
                        latitude=item.get("lat"),
                        longitude=item.get("lng"),
                        nearest_station_id=station.id,
                        base_price=item.get("price"),
                        rating=item.get("rating"),
                        review_count=item.get("review_count"),
                        first_seen=datetime.utcnow(),
                        last_seen=datetime.utcnow(),
                    )
                    session.add(listing)

        logger.info("Saved snapshot: %s → %d listings (avg ₩%.0f)",
                     station.name, snapshot_info["total"], snapshot_info["avg_price"])
        return snapshot_info

    def _extract_listings(self, data: dict) -> list[dict]:
        """
        Airbnb API 응답에서 숙소 목록을 추출합니다.

        주의: Airbnb API 구조가 변경되면 이 메서드만 수정하면 됩니다.

        현재 응답 구조 (2026.02 기준):
        data.presentation.staysSearch.results.searchResults[] = {
            __typename: "StaySearchResult",
            propertyId: "1234567",
            title: "숙소 이름",
            avgRatingLocalized: "4.89",
            structuredDisplayPrice: { primaryLine: { discountedPrice: "₩119,824" } },
            demandStayListing: { roomTypeCategory: "entire_home", ... },
        }
        """
        listings = []

        try:
            results = (
                data.get("data", {})
                .get("presentation", {})
                .get("staysSearch", {})
                .get("results", {})
                .get("searchResults", [])
            )

            for result in results:
                # 2026 현재 구조: StaySearchResult
                demand = result.get("demandStayListing", {}) or {}
                location = demand.get("location", {}) or {}
                coord = location.get("coordinate", {}) or {}

                # ID: demandStayListing.id는 base64 인코딩됨
                # "RGVtYW5kU3RheUxpc3Rpbmc6MTIzNDU2Nzg=" → 숫자 ID 추출
                raw_id = (
                    result.get("propertyId")
                    or self._decode_listing_id(demand.get("id", ""))
                )

                # 이름: nameLocalized 객체에서 추출
                name_obj = result.get("nameLocalized")
                name = ""
                if isinstance(name_obj, dict):
                    name = name_obj.get("localizedStringWithTranslationPreference", "")
                elif isinstance(name_obj, str):
                    name = name_obj

                item = {
                    "id": raw_id,
                    "name": name,
                    "room_type": demand.get("roomTypeCategory", ""),
                    "lat": coord.get("latitude"),
                    "lng": coord.get("longitude"),
                    "price": self._extract_price_v2(result),
                    "rating": self._parse_rating(result.get("avgRatingLocalized")),
                    "review_count": demand.get("reviewsCount"),
                    "available": True,
                }

                # listing 서브 객체가 있는 경우 (구버전 호환)
                if not item["id"]:
                    listing_data = result.get("listing", {})
                    if listing_data:
                        item["id"] = listing_data.get("id")
                        item["name"] = listing_data.get("name")
                        item["room_type"] = listing_data.get("roomTypeCategory")
                        item["lat"] = listing_data.get("coordinate", {}).get("latitude")
                        item["lng"] = listing_data.get("coordinate", {}).get("longitude")
                        item["rating"] = listing_data.get("avgRating")
                        item["review_count"] = listing_data.get("reviewsCount")
                        pricing = result.get("pricingQuote", {})
                        item["price"] = self._extract_price(pricing)

                if item["id"]:
                    listings.append(item)

        except (KeyError, TypeError, AttributeError) as e:
            logger.error("Failed to parse search results: %s", e)
            listings = self._extract_listings_fallback(data)

        return listings

    def _extract_listings_fallback(self, data: dict) -> list[dict]:
        """API 구조 변경 시 대체 파싱 (재귀 탐색)."""
        listings = []

        def _find_listings(obj, depth=0):
            if depth > 10:
                return
            if isinstance(obj, dict):
                if "id" in obj and "name" in obj and ("coordinate" in obj or "lat" in obj):
                    listings.append({
                        "id": obj.get("id"),
                        "name": obj.get("name"),
                        "room_type": obj.get("roomTypeCategory", obj.get("room_type", "")),
                        "lat": obj.get("coordinate", {}).get("latitude") if isinstance(obj.get("coordinate"), dict) else obj.get("lat"),
                        "lng": obj.get("coordinate", {}).get("longitude") if isinstance(obj.get("coordinate"), dict) else obj.get("lng"),
                        "price": obj.get("price", {}).get("amount") if isinstance(obj.get("price"), dict) else obj.get("price"),
                        "rating": obj.get("avgRating"),
                        "review_count": obj.get("reviewsCount"),
                    })
                else:
                    for v in obj.values():
                        _find_listings(v, depth + 1)
            elif isinstance(obj, list):
                for item in obj:
                    _find_listings(item, depth + 1)

        _find_listings(data)
        if listings:
            logger.info("Fallback parser found %d listings", len(listings))
        return listings

    @staticmethod
    def _decode_listing_id(encoded_id: str) -> str | None:
        """Base64 인코딩된 Airbnb ID에서 숫자 ID를 추출합니다.

        "RGVtYW5kU3RheUxpc3Rpbmc6MTA1MTMxMjQ2ODQ4NzYxNTU1OQ=="
        → decode → "DemandStayListing:1051312468487615559"
        → "1051312468487615559"
        """
        if not encoded_id:
            return None
        try:
            import base64
            decoded = base64.b64decode(encoded_id).decode("utf-8")
            # "DemandStayListing:1234567" → "1234567"
            if ":" in decoded:
                return decoded.split(":")[-1]
            return decoded
        except Exception:
            return None

    @staticmethod
    def _extract_price_v2(result: dict) -> float | None:
        """2026 구조에서 가격을 추출합니다.

        structuredDisplayPrice.primaryLine.discountedPrice: "₩119,824"
        또는 structuredDisplayPrice.primaryLine.price: "₩119,824"
        """
        try:
            sdp = result.get("structuredDisplayPrice", {})
            primary = sdp.get("primaryLine", {})

            # discountedPrice 또는 price
            price_str = (
                primary.get("discountedPrice")
                or primary.get("price")
                or primary.get("accessibilityLabel", "")
            )
            if not price_str:
                return None

            # "₩119,824" → 119824.0
            nums = re.sub(r"[^\d]", "", price_str)
            if nums:
                return float(nums)
        except (ValueError, TypeError, AttributeError):
            pass
        return None

    @staticmethod
    def _extract_price(pricing: dict) -> float | None:
        """구버전 pricingQuote에서 가격을 추출합니다."""
        try:
            price = (
                pricing.get("price", {})
                .get("total", {})
                .get("amount")
            )
            if price:
                return float(price)
            price = pricing.get("priceString", "").replace(",", "").replace("₩", "")
            if price:
                return float(price)
        except (ValueError, TypeError, AttributeError):
            pass
        return None

    @staticmethod
    def _parse_rating(rating_str: str | None) -> float | None:
        """문자열 평점을 float로 변환합니다. "4.89" → 4.89"""
        if not rating_str:
            return None
        try:
            return float(rating_str)
        except (ValueError, TypeError):
            return None

    async def crawl_all_stations(self, stations: list[Station],
                                 checkin: date | None = None,
                                 checkout: date | None = None) -> list[dict]:
        """모든 대상 역을 순차적으로 크롤링합니다."""
        results = []
        crawl_log = CrawlLog(
            job_type="search",
            started_at=datetime.utcnow(),
            total_requests=len(stations),
            successful_requests=0,
            failed_requests=0,
        )

        for station in stations:
            try:
                result = await self.crawl_station(station, checkin, checkout)
                if result:
                    results.append(result)
                    crawl_log.successful_requests += 1
                else:
                    crawl_log.failed_requests += 1
            except Exception as e:
                logger.error("Error crawling station %s: %s", station.name, e)
                crawl_log.failed_requests += 1

        crawl_log.finished_at = datetime.utcnow()
        crawl_log.status = "success" if crawl_log.failed_requests == 0 else "partial"

        with session_scope() as session:
            session.add(crawl_log)

        logger.info(
            "Search crawl complete: %d/%d stations successful",
            crawl_log.successful_requests, len(stations),
        )
        return results
