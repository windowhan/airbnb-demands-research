"""
숙소 상세 크롤러 - 개별 숙소의 상세 정보 수집

주 1회 실행하여 숙소 메타데이터(방 유형, 편의시설, 호스트 정보 등)를
업데이트합니다. 옵션 B/C에서 활성화됩니다.

2026.02 기준: StaysPdpSections API 사용.
"""

import logging
import re
from datetime import datetime
from typing import Any

from crawler.airbnb_client import AirbnbClient
from models.database import session_scope
from models.schema import CrawlLog, Listing

logger = logging.getLogger(__name__)


class ListingCrawler:
    """숙소 상세 정보 크롤러."""

    def __init__(self, client: AirbnbClient):
        self._client = client

    async def crawl_listing_detail(self, listing: Listing) -> bool:
        """
        단일 숙소의 상세 정보를 가져와 DB를 업데이트합니다.

        Returns:
            성공 여부
        """
        logger.debug("Fetching detail for listing %s", listing.airbnb_id)

        data = await self._client.get_listing_detail(listing.airbnb_id)
        if data is None:
            return False

        detail = self._extract_detail(data)
        if detail:
            self._update_listing(listing, detail)
            return True
        return False

    def _extract_detail(self, data: dict) -> dict[str, Any] | None:
        """API 응답에서 숙소 상세 정보를 추출합니다.

        2026.02 기준 StaysPdpSections 응답 구조:
        data.presentation.stayProductDetailPage.sections.sections[] = [
            { sectionComponentType: "BOOK_IT_SIDEBAR",
              section: { maxGuestCapacity: 2, descriptionItems: [...] } },
            { sectionComponentType: "MEET_YOUR_HOST",
              section: { cardData: { userId: "...", ratingAverage: 4.7, ... } } },
            { sectionComponentType: "AMENITIES_DEFAULT",
              section: { previewAmenitiesGroups: [...] } },
            { sectionComponentType: "AVAILABILITY_CALENDAR_DEFAULT",
              section: { descriptionItems: [{ title: "공동 주택 전체" }, ...] } },
            ...
        ]
        """
        try:
            sections = (
                data.get("data", {})
                .get("presentation", {})
                .get("stayProductDetailPage", {})
                .get("sections", {})
                .get("sections", [])
            )

            if not sections:
                logger.warning("No sections found in StaysPdpSections response")
                return None

            detail: dict[str, Any] = {}

            for section in sections:
                section_type = section.get("sectionComponentType", "")
                sec = section.get("section") or {}

                # BOOK_IT_SIDEBAR: 최대 게스트, 숙소 개요
                if section_type == "BOOK_IT_SIDEBAR":
                    if sec.get("maxGuestCapacity"):
                        detail["max_guests"] = sec["maxGuestCapacity"]
                    # descriptionItems에서 방 유형 추출
                    self._parse_description_items(sec, detail)

                # AVAILABILITY_CALENDAR: descriptionItems에 방 정보
                elif "AVAILABILITY_CALENDAR" in section_type:
                    self._parse_description_items(sec, detail)

                # MEET_YOUR_HOST: 호스트 정보
                elif section_type == "MEET_YOUR_HOST":
                    card = sec.get("cardData", {})
                    if card:
                        host_id = card.get("userId", "")
                        if host_id:
                            detail["host_id"] = self._decode_user_id(host_id)
                        if card.get("ratingAverage"):
                            detail["rating"] = card["ratingAverage"]
                        for stat in card.get("stats", []):
                            if stat.get("type") == "REVIEW_COUNT":
                                try:
                                    detail["review_count"] = int(stat.get("value", 0))
                                except (ValueError, TypeError):
                                    pass

                # POLICIES: 정책 (체크인/아웃 시간, 게스트 수 등)
                elif section_type == "POLICIES_DEFAULT":
                    for rule in sec.get("houseRules", []):
                        title = rule.get("title", "")
                        guests_match = re.search(r"게스트 정원\s*(\d+)", title)
                        if guests_match and "max_guests" not in detail:
                            detail["max_guests"] = int(guests_match.group(1))

                # OVERVIEW (구버전 호환)
                elif "OVERVIEW" in section_type:
                    if sec.get("roomTypeCategory"):
                        detail["room_type"] = sec["roomTypeCategory"]
                    if sec.get("bedrooms") is not None:
                        detail["bedrooms"] = sec["bedrooms"]
                    if sec.get("bathrooms") is not None:
                        detail["bathrooms"] = sec["bathrooms"]
                    if sec.get("personCapacity"):
                        detail["max_guests"] = sec["personCapacity"]

                # HOST_PROFILE (구버전 호환)
                elif "HOST_PROFILE" in section_type:
                    host = sec
                    user_id = host.get("hostAvatar", {}).get("userId")
                    if user_id:
                        detail["host_id"] = str(user_id)

            return detail if detail else None

        except (KeyError, TypeError, AttributeError) as e:
            logger.error("Failed to parse listing detail: %s", e)
            return None

    @staticmethod
    def _parse_description_items(section: dict, detail: dict):
        """섹션의 descriptionItems에서 방 정보를 추출합니다.

        예: [{"title": "공동 주택 전체"}, {"title": "침대 1개"}, {"title": "욕실 1개"}]
        """
        for item in (section.get("descriptionItems") or []):
            title = item.get("title", "")

            # 방 유형: "공동 주택 전체", "개인실", "호텔 객실" 등
            if "room_type" not in detail:
                if "전체" in title:
                    detail["room_type"] = "entire_home"
                elif "개인실" in title:
                    detail["room_type"] = "private_room"
                elif "다인실" in title or "공유" in title:
                    detail["room_type"] = "shared_room"
                elif "호텔" in title:
                    detail["room_type"] = "hotel"

            # 침실 수
            bed_match = re.search(r"침실\s*(\d+)", title)
            if bed_match and "bedrooms" not in detail:
                detail["bedrooms"] = int(bed_match.group(1))

            # 침대 수 (침실이 없으면 침대로 대체)
            bed_count = re.search(r"침대\s*(\d+)", title)
            if bed_count and "bedrooms" not in detail:
                detail["bedrooms"] = int(bed_count.group(1))

            # 욕실 수
            bath_match = re.search(r"욕실\s*(\d+)", title)
            if bath_match and "bathrooms" not in detail:
                detail["bathrooms"] = int(bath_match.group(1))

    @staticmethod
    def _decode_user_id(encoded_id: str) -> str:
        """Base64 인코딩된 사용자 ID를 디코딩합니다.

        "RGVtYW5kVXNlcjo2ODM0NTY5NDk=" → "DemandUser:683456949" → "683456949"
        """
        try:
            import base64
            decoded = base64.b64decode(encoded_id).decode("utf-8")
            if ":" in decoded:
                return decoded.split(":")[-1]
            return decoded
        except Exception:
            return encoded_id

    def _update_listing(self, listing: Listing, detail: dict):
        """DB의 숙소 정보를 업데이트합니다."""
        with session_scope() as session:
            db_listing = session.query(Listing).filter_by(id=listing.id).first()
            if not db_listing:
                return

            if detail.get("room_type"):
                db_listing.room_type = detail["room_type"]
            if detail.get("bedrooms") is not None:
                db_listing.bedrooms = detail["bedrooms"]
            if detail.get("bathrooms") is not None:
                db_listing.bathrooms = detail["bathrooms"]
            if detail.get("max_guests") is not None:
                db_listing.max_guests = detail["max_guests"]
            if detail.get("host_id"):
                db_listing.host_id = str(detail["host_id"])
            if detail.get("rating") is not None:
                db_listing.rating = detail["rating"]
            if detail.get("review_count") is not None:
                db_listing.review_count = detail["review_count"]
            db_listing.last_seen = datetime.utcnow()

        logger.debug("Updated listing %s detail", listing.airbnb_id)

    async def crawl_all_listings(self, listings: list[Listing]) -> dict:
        """여러 숙소의 상세 정보를 순차적으로 크롤링합니다."""
        crawl_log = CrawlLog(
            job_type="listing",
            started_at=datetime.utcnow(),
            total_requests=len(listings),
        )

        for listing in listings:
            try:
                success = await self.crawl_listing_detail(listing)
                if success:
                    crawl_log.successful_requests += 1
                else:
                    crawl_log.failed_requests += 1
            except Exception as e:
                logger.error("Error fetching detail for %s: %s",
                             listing.airbnb_id, e)
                crawl_log.failed_requests += 1

        crawl_log.finished_at = datetime.utcnow()
        crawl_log.status = "success" if crawl_log.failed_requests == 0 else "partial"

        with session_scope() as session:
            session.add(crawl_log)

        summary = {
            "total": len(listings),
            "success": crawl_log.successful_requests,
            "failed": crawl_log.failed_requests,
        }

        logger.info("Listing detail crawl complete: %d/%d",
                     summary["success"], summary["total"])
        return summary
