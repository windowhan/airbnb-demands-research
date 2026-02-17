"""
캘린더 크롤러 - 숙소별 가용성/가격 데이터 수집

각 숙소의 향후 90일 캘린더를 조회하여
날짜별 예약 가능 여부와 가격을 기록합니다.
이전 스냅샷과 비교하여 실제 예약(가능→불가)을 추정합니다.
"""

import logging
from datetime import date, datetime
from typing import Any

from crawler.airbnb_client import AirbnbClient
from models.database import session_scope
from models.schema import CalendarSnapshot, CrawlLog, Listing

logger = logging.getLogger(__name__)


class CalendarCrawler:
    """숙소 캘린더 크롤러."""

    def __init__(self, client: AirbnbClient):
        self._client = client

    async def crawl_listing_calendar(self, listing: Listing) -> list[dict] | None:
        """
        단일 숙소의 캘린더를 크롤링합니다.

        Returns:
            날짜별 가용성 데이터 리스트 또는 None
        """
        today = date.today()
        logger.debug("Fetching calendar for listing %s", listing.airbnb_id)

        data = await self._client.get_calendar(
            listing_id=listing.airbnb_id,
            month=today.month,
            year=today.year,
            count=3,  # 3개월치
        )

        if data is None:
            return None

        days = self._extract_calendar_days(data)
        if days:
            self._save_calendar(listing, days)
        return days

    def _extract_calendar_days(self, data: dict) -> list[dict]:
        """API 응답에서 날짜별 데이터를 추출합니다."""
        days = []

        try:
            months = (
                data.get("data", {})
                .get("merlin", {})
                .get("pdpAvailabilityCalendar", {})
                .get("calendarMonths", [])
            )

            for month_data in months:
                for day_data in month_data.get("days", []):
                    cal_date = day_data.get("calendarDate")
                    if not cal_date:
                        continue

                    price_data = day_data.get("price", {})
                    price = None
                    if price_data:
                        price = price_data.get("amount")
                        if price is not None:
                            price = float(price)

                    days.append({
                        "date": cal_date,  # "YYYY-MM-DD"
                        "available": day_data.get("available", False),
                        "price": price,
                        "min_nights": day_data.get("minNights"),
                    })

        except (KeyError, TypeError, AttributeError) as e:
            logger.error("Failed to parse calendar data: %s", e)
            days = self._extract_calendar_fallback(data)

        return days

    def _extract_calendar_fallback(self, data: dict) -> list[dict]:
        """API 구조 변경 시 대체 파싱."""
        days = []

        def _find_days(obj, depth=0):
            if depth > 10:
                return
            if isinstance(obj, dict):
                if "calendarDate" in obj and "available" in obj:
                    price_data = obj.get("price", {})
                    days.append({
                        "date": obj["calendarDate"],
                        "available": obj["available"],
                        "price": float(price_data["amount"]) if isinstance(price_data, dict) and "amount" in price_data else None,
                        "min_nights": obj.get("minNights"),
                    })
                else:
                    for v in obj.values():
                        _find_days(v, depth + 1)
            elif isinstance(obj, list):
                for item in obj:
                    _find_days(item, depth + 1)

        _find_days(data)
        return days

    def _save_calendar(self, listing: Listing, days: list[dict]):
        """캘린더 데이터를 DB에 저장합니다."""
        now = datetime.utcnow()

        with session_scope() as session:
            snapshots = []
            for day in days:
                try:
                    cal_date = date.fromisoformat(day["date"])
                except (ValueError, TypeError):
                    continue

                snapshot = CalendarSnapshot(
                    listing_id=listing.id,
                    crawled_at=now,
                    date=cal_date,
                    available=day.get("available", False),
                    price=day.get("price"),
                    min_nights=day.get("min_nights"),
                )
                snapshots.append(snapshot)

            session.bulk_save_objects(snapshots)

        logger.debug("Saved %d calendar days for listing %s",
                     len(snapshots), listing.airbnb_id)

    async def crawl_all_listings(self, listings: list[Listing]) -> dict:
        """
        여러 숙소의 캘린더를 순차적으로 크롤링합니다.

        Returns:
            크롤링 결과 요약 dict
        """
        crawl_log = CrawlLog(
            job_type="calendar",
            started_at=datetime.utcnow(),
            total_requests=len(listings),
        )

        for listing in listings:
            try:
                result = await self.crawl_listing_calendar(listing)
                if result:
                    crawl_log.successful_requests += 1
                else:
                    crawl_log.failed_requests += 1
            except Exception as e:
                logger.error("Error fetching calendar for %s: %s",
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

        logger.info("Calendar crawl complete: %d/%d listings",
                     summary["success"], summary["total"])
        return summary
