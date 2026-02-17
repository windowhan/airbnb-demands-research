"""CalendarCrawler 단위 테스트."""

from datetime import date, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from crawler.calendar_crawler import CalendarCrawler
from models.schema import CalendarSnapshot, CrawlLog, Listing, Station


# ─── _extract_calendar_days ───────────────────────────────────────────


class TestExtractCalendarDays:
    """_extract_calendar_days 메서드 테스트."""

    def test_extracts_all_three_days(
        self, mock_airbnb_client, sample_calendar_response
    ):
        """sample_calendar_response에서 3일(1 unavailable + 2 available)을 추출한다."""
        crawler = CalendarCrawler(mock_airbnb_client)
        days = crawler._extract_calendar_days(sample_calendar_response)
        assert len(days) == 3

    def test_first_day_unavailable(
        self, mock_airbnb_client, sample_calendar_response
    ):
        """첫 번째 날은 available=False이고 가격은 None이다."""
        crawler = CalendarCrawler(mock_airbnb_client)
        days = crawler._extract_calendar_days(sample_calendar_response)
        first = days[0]
        assert first["date"] == "2026-02-01"
        assert first["available"] is False
        assert first["bookable"] is None
        assert first["price"] is None
        assert first["min_nights"] == 1

    def test_second_day_available_with_price(
        self, mock_airbnb_client, sample_calendar_response
    ):
        """두 번째 날은 available=True이고 가격이 100000이다."""
        crawler = CalendarCrawler(mock_airbnb_client)
        days = crawler._extract_calendar_days(sample_calendar_response)
        second = days[1]
        assert second["date"] == "2026-02-18"
        assert second["available"] is True
        assert second["bookable"] is True
        assert second["price"] == 100000.0
        assert second["min_nights"] == 2

    def test_third_day_available_no_price(
        self, mock_airbnb_client, sample_calendar_response
    ):
        """세 번째 날은 available=True이지만 가격이 None이다."""
        crawler = CalendarCrawler(mock_airbnb_client)
        days = crawler._extract_calendar_days(sample_calendar_response)
        third = days[2]
        assert third["date"] == "2026-02-19"
        assert third["available"] is True
        assert third["bookable"] is True
        assert third["price"] is None

    def test_empty_calendar_months(self, mock_airbnb_client):
        """calendarMonths가 빈 리스트이면 빈 결과를 반환한다."""
        crawler = CalendarCrawler(mock_airbnb_client)
        data = {
            "data": {
                "merlin": {
                    "pdpAvailabilityCalendar": {"calendarMonths": []}
                }
            }
        }
        days = crawler._extract_calendar_days(data)
        assert days == []

    def test_empty_data(self, mock_airbnb_client):
        """빈 dict에서는 빈 리스트를 반환한다."""
        crawler = CalendarCrawler(mock_airbnb_client)
        days = crawler._extract_calendar_days({})
        assert days == []

    def test_missing_calendar_date_skipped(self, mock_airbnb_client):
        """calendarDate가 없는 항목은 건너뛴다."""
        crawler = CalendarCrawler(mock_airbnb_client)
        data = {
            "data": {
                "merlin": {
                    "pdpAvailabilityCalendar": {
                        "calendarMonths": [
                            {
                                "days": [
                                    {"available": True, "price": None},
                                    {"calendarDate": "2026-03-01", "available": True},
                                ]
                            }
                        ]
                    }
                }
            }
        }
        days = crawler._extract_calendar_days(data)
        assert len(days) == 1
        assert days[0]["date"] == "2026-03-01"

    def test_multiple_months(self, mock_airbnb_client):
        """여러 달의 데이터를 모두 추출한다."""
        crawler = CalendarCrawler(mock_airbnb_client)
        data = {
            "data": {
                "merlin": {
                    "pdpAvailabilityCalendar": {
                        "calendarMonths": [
                            {
                                "month": 2,
                                "year": 2026,
                                "days": [
                                    {"calendarDate": "2026-02-01", "available": True},
                                ],
                            },
                            {
                                "month": 3,
                                "year": 2026,
                                "days": [
                                    {"calendarDate": "2026-03-01", "available": False},
                                    {"calendarDate": "2026-03-02", "available": True},
                                ],
                            },
                        ]
                    }
                }
            }
        }
        days = crawler._extract_calendar_days(data)
        assert len(days) == 3


# ─── _extract_calendar_fallback ───────────────────────────────────────


class TestExtractCalendarFallback:
    """_extract_calendar_fallback 메서드 테스트."""

    def test_finds_days_recursively(self, mock_airbnb_client):
        """재귀 탐색으로 중첩된 캘린더 날짜를 찾아낸다."""
        crawler = CalendarCrawler(mock_airbnb_client)
        data = {
            "wrapper": {
                "inner": [
                    {
                        "calendarDate": "2026-04-01",
                        "available": True,
                        "bookable": True,
                        "price": {"localPriceFormatted": "₩80,000"},
                        "minNights": 1,
                    },
                    {
                        "calendarDate": "2026-04-02",
                        "available": False,
                        "bookable": None,
                        "price": None,
                        "minNights": 2,
                    },
                ]
            }
        }
        days = crawler._extract_calendar_fallback(data)
        assert len(days) == 2
        assert days[0]["date"] == "2026-04-01"
        assert days[0]["available"] is True
        assert days[0]["price"] == 80000.0
        assert days[1]["date"] == "2026-04-02"
        assert days[1]["available"] is False
        assert days[1]["price"] is None

    def test_fallback_empty_data(self, mock_airbnb_client):
        """빈 데이터에서는 빈 리스트를 반환한다."""
        crawler = CalendarCrawler(mock_airbnb_client)
        assert crawler._extract_calendar_fallback({}) == []

    def test_fallback_respects_depth_limit(self, mock_airbnb_client):
        """깊이 제한(10)을 초과하면 탐색을 중단한다."""
        crawler = CalendarCrawler(mock_airbnb_client)
        obj = {"calendarDate": "2026-05-01", "available": True}
        for _ in range(12):
            obj = {"child": obj}
        days = crawler._extract_calendar_fallback(obj)
        assert days == []


# ─── _parse_calendar_price ────────────────────────────────────────────


class TestParseCalendarPrice:
    """_parse_calendar_price 정적 메서드 테스트."""

    def test_formatted_korean_won(self):
        """'₩100,000' -> 100000.0"""
        result = CalendarCrawler._parse_calendar_price(
            {"localPriceFormatted": "₩100,000"}
        )
        assert result == 100000.0

    def test_amount_field(self):
        """{"amount": 50000} -> 50000.0"""
        result = CalendarCrawler._parse_calendar_price({"amount": 50000})
        assert result == 50000.0

    def test_amount_as_string(self):
        """amount가 문자열이어도 파싱한다."""
        result = CalendarCrawler._parse_calendar_price({"amount": "75000"})
        assert result == 75000.0

    def test_none_returns_none(self):
        """None -> None"""
        assert CalendarCrawler._parse_calendar_price(None) is None

    def test_empty_dict_returns_none(self):
        """빈 dict -> None"""
        assert CalendarCrawler._parse_calendar_price({}) is None

    def test_null_formatted_price(self):
        """localPriceFormatted가 None이면 None을 반환한다."""
        result = CalendarCrawler._parse_calendar_price(
            {"localPriceFormatted": None}
        )
        assert result is None

    def test_not_a_dict_returns_none(self):
        """dict가 아닌 값은 None을 반환한다."""
        assert CalendarCrawler._parse_calendar_price("₩100,000") is None
        assert CalendarCrawler._parse_calendar_price(12345) is None

    def test_amount_takes_priority_over_formatted(self):
        """amount 필드가 있으면 localPriceFormatted보다 우선한다."""
        result = CalendarCrawler._parse_calendar_price({
            "amount": 50000,
            "localPriceFormatted": "₩100,000",
        })
        assert result == 50000.0

    def test_invalid_amount_falls_through_to_formatted(self):
        """amount가 유효하지 않으면 localPriceFormatted로 대체한다."""
        result = CalendarCrawler._parse_calendar_price({
            "amount": "not_a_number",
            "localPriceFormatted": "₩60,000",
        })
        assert result == 60000.0


# ─── _save_calendar ───────────────────────────────────────────────────


class TestSaveCalendar:
    """_save_calendar 메서드 테스트."""

    def test_saves_calendar_snapshots(
        self,
        mock_airbnb_client,
        sample_listing,
        mock_session_scope,
        db_session,
    ):
        """캘린더 일별 데이터가 CalendarSnapshot으로 DB에 저장된다."""
        crawler = CalendarCrawler(mock_airbnb_client)
        days = [
            {
                "date": "2026-02-18",
                "available": True,
                "bookable": True,
                "price": 100000.0,
                "min_nights": 2,
            },
            {
                "date": "2026-02-19",
                "available": False,
                "bookable": None,
                "price": None,
                "min_nights": 1,
            },
        ]

        with patch("crawler.calendar_crawler.session_scope", mock_session_scope):
            crawler._save_calendar(sample_listing, days)

        snapshots = db_session.query(CalendarSnapshot).all()
        assert len(snapshots) == 2

        snap_18 = next(s for s in snapshots if s.date == date(2026, 2, 18))
        assert snap_18.available is True
        assert snap_18.price == 100000.0
        assert snap_18.min_nights == 2
        assert snap_18.listing_id == sample_listing.id

        snap_19 = next(s for s in snapshots if s.date == date(2026, 2, 19))
        assert snap_19.available is False
        assert snap_19.price is None

    def test_invalid_date_skipped(
        self,
        mock_airbnb_client,
        sample_listing,
        mock_session_scope,
        db_session,
    ):
        """유효하지 않은 날짜 형식은 건너뛴다."""
        crawler = CalendarCrawler(mock_airbnb_client)
        days = [
            {"date": "invalid-date", "available": True, "price": 50000.0, "min_nights": 1},
            {"date": "2026-02-20", "available": True, "price": 60000.0, "min_nights": 1},
        ]

        with patch("crawler.calendar_crawler.session_scope", mock_session_scope):
            crawler._save_calendar(sample_listing, days)

        snapshots = db_session.query(CalendarSnapshot).all()
        assert len(snapshots) == 1
        assert snapshots[0].date == date(2026, 2, 20)

    def test_empty_days_list(
        self,
        mock_airbnb_client,
        sample_listing,
        mock_session_scope,
        db_session,
    ):
        """빈 리스트에 대해서는 아무 것도 저장되지 않는다."""
        crawler = CalendarCrawler(mock_airbnb_client)

        with patch("crawler.calendar_crawler.session_scope", mock_session_scope):
            crawler._save_calendar(sample_listing, [])

        snapshots = db_session.query(CalendarSnapshot).all()
        assert len(snapshots) == 0


# ─── crawl_listing_calendar ──────────────────────────────────────────


class TestCrawlListingCalendar:
    """crawl_listing_calendar 비동기 메서드 테스트."""

    async def test_successful_crawl(
        self,
        mock_airbnb_client,
        sample_calendar_response,
        sample_listing,
        mock_session_scope,
    ):
        """캘린더 API 호출 후 결과가 저장되고 날짜 리스트가 반환된다."""
        mock_airbnb_client.get_calendar = AsyncMock(
            return_value=sample_calendar_response
        )
        crawler = CalendarCrawler(mock_airbnb_client)

        with patch("crawler.calendar_crawler.session_scope", mock_session_scope):
            result = await crawler.crawl_listing_calendar(sample_listing)

        assert result is not None
        assert len(result) == 3
        mock_airbnb_client.get_calendar.assert_awaited_once()

    async def test_returns_none_when_no_data(
        self, mock_airbnb_client, sample_listing
    ):
        """API가 None을 반환하면 None을 반환한다."""
        mock_airbnb_client.get_calendar = AsyncMock(return_value=None)
        crawler = CalendarCrawler(mock_airbnb_client)

        result = await crawler.crawl_listing_calendar(sample_listing)
        assert result is None

    async def test_returns_empty_days_without_saving(
        self, mock_airbnb_client, sample_listing, mock_session_scope, db_session
    ):
        """추출된 날짜가 없으면 저장하지 않고 빈 리스트를 반환한다."""
        empty_response = {
            "data": {
                "merlin": {
                    "pdpAvailabilityCalendar": {"calendarMonths": []}
                }
            }
        }
        mock_airbnb_client.get_calendar = AsyncMock(return_value=empty_response)
        crawler = CalendarCrawler(mock_airbnb_client)

        with patch("crawler.calendar_crawler.session_scope", mock_session_scope):
            result = await crawler.crawl_listing_calendar(sample_listing)

        # 빈 리스트는 falsy이므로 _save_calendar가 호출되지 않는다
        assert result == []
        snapshots = db_session.query(CalendarSnapshot).all()
        assert len(snapshots) == 0

    async def test_passes_correct_parameters(
        self,
        mock_airbnb_client,
        sample_calendar_response,
        sample_listing,
        mock_session_scope,
    ):
        """API에 올바른 파라미터가 전달된다."""
        mock_airbnb_client.get_calendar = AsyncMock(
            return_value=sample_calendar_response
        )
        crawler = CalendarCrawler(mock_airbnb_client)

        with patch("crawler.calendar_crawler.session_scope", mock_session_scope):
            await crawler.crawl_listing_calendar(sample_listing)

        call_kwargs = mock_airbnb_client.get_calendar.call_args.kwargs
        assert call_kwargs["listing_id"] == sample_listing.airbnb_id
        assert call_kwargs["count"] == 3


# ─── crawl_all_listings ──────────────────────────────────────────────


class TestCrawlAllListings:
    """crawl_all_listings 비동기 메서드 테스트."""

    async def test_all_success(
        self,
        mock_airbnb_client,
        sample_calendar_response,
        sample_listing,
        mock_session_scope,
    ):
        """모든 리스팅이 성공적으로 크롤링되면 success 요약을 반환한다."""
        mock_airbnb_client.get_calendar = AsyncMock(
            return_value=sample_calendar_response
        )
        crawler = CalendarCrawler(mock_airbnb_client)

        with patch("crawler.calendar_crawler.session_scope", mock_session_scope):
            summary = await crawler.crawl_all_listings([sample_listing])

        assert summary["total"] == 1
        assert summary["success"] == 1
        assert summary["failed"] == 0

    async def test_partial_failure(
        self,
        mock_airbnb_client,
        sample_calendar_response,
        sample_listing,
        mock_session_scope,
        db_session,
    ):
        """일부 리스팅이 실패하면 partial 상태로 기록된다."""
        listing_b = Listing(
            airbnb_id="999888777",
            name="실패 숙소",
            room_type="private_room",
            latitude=37.5,
            longitude=127.0,
            nearest_station_id=sample_listing.nearest_station_id,
            first_seen=datetime.utcnow(),
            last_seen=datetime.utcnow(),
        )
        db_session.add(listing_b)
        db_session.commit()
        db_session.refresh(listing_b)

        # 첫 번째 성공, 두 번째 None 반환(실패)
        mock_airbnb_client.get_calendar = AsyncMock(
            side_effect=[sample_calendar_response, None]
        )
        crawler = CalendarCrawler(mock_airbnb_client)

        with patch("crawler.calendar_crawler.session_scope", mock_session_scope):
            summary = await crawler.crawl_all_listings(
                [sample_listing, listing_b]
            )

        assert summary["total"] == 2
        assert summary["success"] == 1
        assert summary["failed"] == 1

    async def test_exception_handling(
        self,
        mock_airbnb_client,
        sample_listing,
        mock_session_scope,
    ):
        """크롤링 중 예외가 발생해도 나머지를 계속 처리한다."""
        mock_airbnb_client.get_calendar = AsyncMock(
            side_effect=Exception("Timeout error")
        )
        crawler = CalendarCrawler(mock_airbnb_client)

        with patch("crawler.calendar_crawler.session_scope", mock_session_scope):
            summary = await crawler.crawl_all_listings([sample_listing])

        assert summary["total"] == 1
        assert summary["success"] == 0
        assert summary["failed"] == 1

    async def test_empty_listings_list(
        self, mock_airbnb_client, mock_session_scope
    ):
        """빈 리스팅 리스트에 대해서도 정상적으로 반환한다."""
        crawler = CalendarCrawler(mock_airbnb_client)

        with patch("crawler.calendar_crawler.session_scope", mock_session_scope):
            summary = await crawler.crawl_all_listings([])

        assert summary["total"] == 0
        assert summary["success"] == 0
        assert summary["failed"] == 0


# ─── 추가 커버리지: exception fallback path ──────────────────────────

class TestCalendarCrawlerEdgeCases:
    """calendar_crawler 추가 에러 핸들링 테스트."""

    def test_extract_days_type_error_uses_fallback(self, mock_airbnb_client):
        """파싱 중 TypeError 발생 시 fallback을 사용한다 (lines 93-95)."""
        crawler = CalendarCrawler(mock_airbnb_client)
        # calendarMonths가 리스트가 아니라 None → TypeError
        data = {"data": {"merlin": {"pdpAvailabilityCalendar": {"calendarMonths": None}}}}
        days = crawler._extract_calendar_days(data)
        assert isinstance(days, list)
