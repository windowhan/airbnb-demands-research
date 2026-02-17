"""
숙소 상세 크롤러 - 개별 숙소의 상세 정보 수집

주 1회 실행하여 숙소 메타데이터(방 유형, 편의시설, 호스트 정보 등)를
업데이트합니다. 옵션 B/C에서 활성화됩니다.
"""

import logging
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
        """API 응답에서 숙소 상세 정보를 추출합니다."""
        try:
            sections = (
                data.get("data", {})
                .get("presentation", {})
                .get("stayProductDetailPage", {})
                .get("sections", {})
                .get("sections", [])
            )

            detail: dict[str, Any] = {}

            for section in sections:
                section_type = section.get("sectionComponentType", "")

                # 기본 정보
                if "OVERVIEW" in section_type:
                    overview = section.get("section", {})
                    detail["room_type"] = overview.get("roomTypeCategory")
                    detail["bedrooms"] = overview.get("bedrooms")
                    detail["bathrooms"] = overview.get("bathrooms")
                    detail["max_guests"] = overview.get("personCapacity")

                # 호스트 정보
                if "HOST_PROFILE" in section_type:
                    host = section.get("section", {})
                    detail["host_id"] = host.get("hostAvatar", {}).get("userId")

            return detail if detail else None

        except (KeyError, TypeError, AttributeError) as e:
            logger.error("Failed to parse listing detail: %s", e)
            return None

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
