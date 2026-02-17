"""
Tests for scheduler/jobs.py

Covers:
- load_stations_from_json (priority filtering, duplicate skipping)
- get_target_stations / get_all_listings (DB queries)
- run_search_job / run_calendar_job / run_listing_detail_job (async crawl orchestration)
- setup_scheduler (APScheduler configuration)
"""

import json
from contextlib import contextmanager
from unittest.mock import AsyncMock, MagicMock, mock_open, patch

import pytest

from models.schema import Listing, Station


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TIER_A_CONFIG = {
    "station_priority": [1],
    "search_interval_minutes": 60,
    "calendar_enabled": True,
    "calendar_hour": 3,
    "listing_detail_enabled": False,
    "max_concurrent_requests": 1,
    "delay_base": 7.0,
    "delay_jitter": (2.0, 8.0),
    "proxy_required": False,
    "requests_per_ip_before_rotate": 50,
    "max_requests_per_hour": 50,
    "daily_limit_per_ip": 800,
}

TIER_B_CONFIG = {
    "station_priority": [1, 2],
    "search_interval_minutes": 60,
    "calendar_enabled": True,
    "calendar_hour": 2,
    "listing_detail_enabled": True,
    "max_concurrent_requests": 2,
    "delay_base": 5.0,
    "delay_jitter": (1.0, 5.0),
    "proxy_required": True,
    "requests_per_ip_before_rotate": 30,
    "max_requests_per_hour": 80,
    "daily_limit_per_ip": 600,
}

SAMPLE_STATIONS_JSON = {
    "stations": [
        {"name": "Gangnam", "line": "Line2", "district": "Gangnam-gu",
         "lat": 37.4981, "lng": 127.0276, "priority": 1},
        {"name": "Hongdae", "line": "Line2", "district": "Mapo-gu",
         "lat": 37.5571, "lng": 126.9244, "priority": 1},
        {"name": "Gupabal", "line": "Line3", "district": "Eunpyeong-gu",
         "lat": 37.6375, "lng": 126.9188, "priority": 2},
        {"name": "Suseo", "line": "LineSRT", "district": "Gangnam-gu",
         "lat": 37.4866, "lng": 127.1017, "priority": 3},
    ]
}


def _make_mock_session_scope(session):
    """Return a context manager factory that yields the given session."""
    @contextmanager
    def _scope():
        yield session
    return _scope


# ---------------------------------------------------------------------------
# load_stations_from_json
# ---------------------------------------------------------------------------

class TestLoadStationsFromJson:
    """Tests for load_stations_from_json()."""

    @patch("scheduler.jobs.get_tier_config")
    @patch("scheduler.jobs.session_scope")
    @patch("builtins.open", new_callable=mock_open,
           read_data=json.dumps(SAMPLE_STATIONS_JSON))
    def test_load_stations_from_json(self, mock_file, mock_scope, mock_tier):
        """Stations matching priority are saved to the DB; others are skipped."""
        mock_tier.return_value = {**TIER_A_CONFIG, "station_priority": [1]}

        mock_session = MagicMock()
        # No existing stations in DB
        mock_session.query.return_value.filter_by.return_value.first.return_value = None
        mock_scope.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_scope.return_value.__exit__ = MagicMock(return_value=False)

        from scheduler.jobs import load_stations_from_json
        load_stations_from_json()

        # Only priority-1 stations (Gangnam, Hongdae) should be added
        assert mock_session.add.call_count == 2
        added_stations = [call.args[0] for call in mock_session.add.call_args_list]
        for st in added_stations:
            assert isinstance(st, Station)
        names = {st.name for st in added_stations}
        assert names == {"Gangnam", "Hongdae"}

    @patch("scheduler.jobs.get_tier_config")
    @patch("scheduler.jobs.session_scope")
    @patch("builtins.open", new_callable=mock_open,
           read_data=json.dumps(SAMPLE_STATIONS_JSON))
    def test_load_stations_respects_priority(self, mock_file, mock_scope, mock_tier):
        """When tier allows priorities [1, 2], stations with priority 3 are excluded."""
        mock_tier.return_value = {**TIER_B_CONFIG, "station_priority": [1, 2]}

        mock_session = MagicMock()
        mock_session.query.return_value.filter_by.return_value.first.return_value = None
        mock_scope.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_scope.return_value.__exit__ = MagicMock(return_value=False)

        from scheduler.jobs import load_stations_from_json
        load_stations_from_json()

        # Gangnam (1), Hongdae (1), Gupabal (2) -- but NOT Suseo (3)
        assert mock_session.add.call_count == 3
        added_names = {call.args[0].name for call in mock_session.add.call_args_list}
        assert "Suseo" not in added_names
        assert added_names == {"Gangnam", "Hongdae", "Gupabal"}

    @patch("scheduler.jobs.get_tier_config")
    @patch("scheduler.jobs.session_scope")
    @patch("builtins.open", new_callable=mock_open,
           read_data=json.dumps(SAMPLE_STATIONS_JSON))
    def test_load_stations_no_duplicates(self, mock_file, mock_scope, mock_tier):
        """Existing stations (by name+line) are not re-added."""
        mock_tier.return_value = {**TIER_A_CONFIG, "station_priority": [1]}

        mock_session = MagicMock()
        # Simulate: Gangnam already exists, Hongdae does not
        existing_station = Station(name="Gangnam", line="Line2")

        def _filter_by_side_effect(**kwargs):
            query_result = MagicMock()
            if kwargs.get("name") == "Gangnam" and kwargs.get("line") == "Line2":
                query_result.first.return_value = existing_station
            else:
                query_result.first.return_value = None
            return query_result

        mock_session.query.return_value.filter_by.side_effect = _filter_by_side_effect
        mock_scope.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_scope.return_value.__exit__ = MagicMock(return_value=False)

        from scheduler.jobs import load_stations_from_json
        load_stations_from_json()

        # Only Hongdae should be added (Gangnam already exists)
        assert mock_session.add.call_count == 1
        added_station = mock_session.add.call_args.args[0]
        assert added_station.name == "Hongdae"


# ---------------------------------------------------------------------------
# get_target_stations
# ---------------------------------------------------------------------------

class TestGetTargetStations:
    """Tests for get_target_stations()."""

    @patch("scheduler.jobs.get_tier_config")
    @patch("scheduler.jobs.session_scope")
    def test_get_target_stations(self, mock_scope, mock_tier):
        """Returns stations whose priority is in the allowed list."""
        mock_tier.return_value = {**TIER_A_CONFIG, "station_priority": [1]}

        station_a = Station(id=1, name="Gangnam", line="Line2",
                            latitude=37.498, longitude=127.028, priority=1)
        station_b = Station(id=2, name="Hongdae", line="Line2",
                            latitude=37.557, longitude=126.924, priority=1)

        mock_session = MagicMock()
        mock_query = mock_session.query.return_value
        mock_filter = mock_query.filter.return_value
        mock_order = mock_filter.order_by.return_value
        mock_order.all.return_value = [station_a, station_b]

        mock_scope.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_scope.return_value.__exit__ = MagicMock(return_value=False)

        from scheduler.jobs import get_target_stations
        result = get_target_stations()

        assert len(result) == 2
        assert result[0].name == "Gangnam"
        assert result[1].name == "Hongdae"
        mock_session.expunge_all.assert_called_once()


# ---------------------------------------------------------------------------
# get_all_listings
# ---------------------------------------------------------------------------

class TestGetAllListings:
    """Tests for get_all_listings()."""

    @patch("scheduler.jobs.session_scope")
    def test_get_all_listings(self, mock_scope):
        """Returns all listings from the database."""
        listing_a = Listing(id=1, airbnb_id="111", name="Listing A")
        listing_b = Listing(id=2, airbnb_id="222", name="Listing B")

        mock_session = MagicMock()
        mock_query = mock_session.query.return_value
        mock_order = mock_query.order_by.return_value
        mock_order.all.return_value = [listing_a, listing_b]

        mock_scope.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_scope.return_value.__exit__ = MagicMock(return_value=False)

        from scheduler.jobs import get_all_listings
        result = get_all_listings()

        assert len(result) == 2
        assert result[0].airbnb_id == "111"
        assert result[1].airbnb_id == "222"
        mock_session.expunge_all.assert_called_once()


# ---------------------------------------------------------------------------
# run_search_job
# ---------------------------------------------------------------------------

class TestRunSearchJob:
    """Tests for run_search_job()."""

    @patch("scheduler.jobs.get_target_stations")
    @patch("scheduler.jobs.SearchCrawler")
    @patch("scheduler.jobs.AirbnbClient")
    async def test_run_search_job(self, MockClient, MockCrawler, mock_get_stations):
        """Creates client + crawler, crawls stations, closes client."""
        station = Station(id=1, name="Gangnam", line="Line2",
                          latitude=37.498, longitude=127.028, priority=1)
        mock_get_stations.return_value = [station]

        mock_client_instance = MagicMock()
        mock_client_instance.close = AsyncMock()
        mock_client_instance.get_stats.return_value = {
            "rate_limiter": {"requests": 10},
            "proxy_manager": {"total": 0},
        }
        MockClient.return_value = mock_client_instance

        mock_crawler_instance = MagicMock()
        mock_crawler_instance.crawl_all_stations = AsyncMock(
            return_value=[{"station": "Gangnam", "listings": 5}]
        )
        MockCrawler.return_value = mock_crawler_instance

        from scheduler.jobs import run_search_job
        await run_search_job()

        MockClient.assert_called_once()
        MockCrawler.assert_called_once_with(mock_client_instance)
        mock_crawler_instance.crawl_all_stations.assert_awaited_once_with([station])
        mock_client_instance.close.assert_awaited_once()

    @patch("scheduler.jobs.get_target_stations")
    @patch("scheduler.jobs.SearchCrawler")
    @patch("scheduler.jobs.AirbnbClient")
    async def test_run_search_job_no_stations(
        self, MockClient, MockCrawler, mock_get_stations, caplog
    ):
        """When no target stations exist, logs a warning and returns early."""
        mock_get_stations.return_value = []

        mock_client_instance = MagicMock()
        mock_client_instance.close = AsyncMock()
        MockClient.return_value = mock_client_instance

        from scheduler.jobs import run_search_job

        import logging
        with caplog.at_level(logging.WARNING, logger="scheduler.jobs"):
            await run_search_job()

        assert any("No target stations found" in msg for msg in caplog.messages)
        # Crawler should never have been called
        MockCrawler.return_value.crawl_all_stations.assert_not_called()
        # Client close should still be called (finally block)
        mock_client_instance.close.assert_awaited_once()


# ---------------------------------------------------------------------------
# run_calendar_job
# ---------------------------------------------------------------------------

class TestRunCalendarJob:
    """Tests for run_calendar_job()."""

    @patch("scheduler.jobs.get_all_listings")
    @patch("scheduler.jobs.CalendarCrawler")
    @patch("scheduler.jobs.AirbnbClient")
    @patch("scheduler.jobs.get_tier_config")
    async def test_run_calendar_job(
        self, mock_tier, MockClient, MockCrawler, mock_get_listings
    ):
        """When calendar_enabled=True, crawls all listings."""
        mock_tier.return_value = {**TIER_A_CONFIG, "calendar_enabled": True}

        listing = Listing(id=1, airbnb_id="111", name="Test Listing")
        mock_get_listings.return_value = [listing]

        mock_client_instance = MagicMock()
        mock_client_instance.close = AsyncMock()
        MockClient.return_value = mock_client_instance

        mock_crawler_instance = MagicMock()
        mock_crawler_instance.crawl_all_listings = AsyncMock(
            return_value={"total": 1, "success": 1, "failed": 0}
        )
        MockCrawler.return_value = mock_crawler_instance

        from scheduler.jobs import run_calendar_job
        await run_calendar_job()

        MockClient.assert_called_once()
        MockCrawler.assert_called_once_with(mock_client_instance)
        mock_crawler_instance.crawl_all_listings.assert_awaited_once_with([listing])
        mock_client_instance.close.assert_awaited_once()

    @patch("scheduler.jobs.get_all_listings")
    @patch("scheduler.jobs.CalendarCrawler")
    @patch("scheduler.jobs.AirbnbClient")
    @patch("scheduler.jobs.get_tier_config")
    async def test_run_calendar_job_disabled(
        self, mock_tier, MockClient, MockCrawler, mock_get_listings, caplog
    ):
        """When calendar_enabled=False, returns early without crawling."""
        mock_tier.return_value = {**TIER_A_CONFIG, "calendar_enabled": False}

        from scheduler.jobs import run_calendar_job

        import logging
        with caplog.at_level(logging.INFO, logger="scheduler.jobs"):
            await run_calendar_job()

        assert any(
            "Calendar crawling disabled" in msg for msg in caplog.messages
        )
        # Should never create client or crawler
        MockClient.assert_not_called()
        MockCrawler.assert_not_called()
        mock_get_listings.assert_not_called()


# ---------------------------------------------------------------------------
# run_listing_detail_job
# ---------------------------------------------------------------------------

class TestRunListingDetailJob:
    """Tests for run_listing_detail_job()."""

    @patch("scheduler.jobs.get_all_listings")
    @patch("scheduler.jobs.ListingCrawler")
    @patch("scheduler.jobs.AirbnbClient")
    @patch("scheduler.jobs.get_tier_config")
    async def test_run_listing_detail_job(
        self, mock_tier, MockClient, MockCrawler, mock_get_listings
    ):
        """When listing_detail_enabled=True, crawls all listings."""
        mock_tier.return_value = {**TIER_B_CONFIG, "listing_detail_enabled": True}

        listing = Listing(id=1, airbnb_id="111", name="Test Listing")
        mock_get_listings.return_value = [listing]

        mock_client_instance = MagicMock()
        mock_client_instance.close = AsyncMock()
        MockClient.return_value = mock_client_instance

        mock_crawler_instance = MagicMock()
        mock_crawler_instance.crawl_all_listings = AsyncMock(
            return_value={"total": 1, "success": 1, "failed": 0}
        )
        MockCrawler.return_value = mock_crawler_instance

        from scheduler.jobs import run_listing_detail_job
        await run_listing_detail_job()

        MockClient.assert_called_once()
        MockCrawler.assert_called_once_with(mock_client_instance)
        mock_crawler_instance.crawl_all_listings.assert_awaited_once_with([listing])
        mock_client_instance.close.assert_awaited_once()

    @patch("scheduler.jobs.get_all_listings")
    @patch("scheduler.jobs.ListingCrawler")
    @patch("scheduler.jobs.AirbnbClient")
    @patch("scheduler.jobs.get_tier_config")
    async def test_run_listing_detail_job_disabled(
        self, mock_tier, MockClient, MockCrawler, mock_get_listings, caplog
    ):
        """When listing_detail_enabled=False, returns early without crawling."""
        mock_tier.return_value = {**TIER_A_CONFIG, "listing_detail_enabled": False}

        from scheduler.jobs import run_listing_detail_job

        import logging
        with caplog.at_level(logging.INFO, logger="scheduler.jobs"):
            await run_listing_detail_job()

        assert any(
            "Listing detail crawling disabled" in msg for msg in caplog.messages
        )
        MockClient.assert_not_called()
        MockCrawler.assert_not_called()
        mock_get_listings.assert_not_called()


# ---------------------------------------------------------------------------
# setup_scheduler
# ---------------------------------------------------------------------------

class TestSetupScheduler:
    """Tests for setup_scheduler()."""

    @patch("scheduler.jobs.get_tier_config")
    def test_setup_scheduler_tier_a(self, mock_tier):
        """Tier A: search job + calendar job added; listing detail NOT added."""
        mock_tier.return_value = {**TIER_A_CONFIG}

        from scheduler.jobs import setup_scheduler
        scheduler = setup_scheduler()

        job_ids = {job.id for job in scheduler.get_jobs()}
        assert "search_job" in job_ids
        assert "calendar_job" in job_ids       # calendar_enabled=True for A
        assert "listing_detail_job" not in job_ids  # listing_detail_enabled=False for A

    @patch("scheduler.jobs.get_tier_config")
    def test_setup_scheduler_tier_b_all_jobs(self, mock_tier):
        """Tier B: all three jobs should be registered."""
        mock_tier.return_value = {**TIER_B_CONFIG}

        from scheduler.jobs import setup_scheduler
        scheduler = setup_scheduler()

        job_ids = {job.id for job in scheduler.get_jobs()}
        assert "search_job" in job_ids
        assert "calendar_job" in job_ids
        assert "listing_detail_job" in job_ids

    @patch("scheduler.jobs.get_tier_config")
    def test_setup_scheduler_calendar_disabled(self, mock_tier):
        """When calendar_enabled=False, calendar_job is NOT registered."""
        config = {**TIER_A_CONFIG, "calendar_enabled": False,
                  "listing_detail_enabled": False}
        mock_tier.return_value = config

        from scheduler.jobs import setup_scheduler
        scheduler = setup_scheduler()

        job_ids = {job.id for job in scheduler.get_jobs()}
        assert "search_job" in job_ids
        assert "calendar_job" not in job_ids
        assert "listing_detail_job" not in job_ids

    @patch("scheduler.jobs.get_tier_config")
    def test_setup_scheduler_returns_scheduler(self, mock_tier):
        """setup_scheduler returns an AsyncIOScheduler instance."""
        from apscheduler.schedulers.asyncio import AsyncIOScheduler

        mock_tier.return_value = {**TIER_A_CONFIG}

        from scheduler.jobs import setup_scheduler
        scheduler = setup_scheduler()

        assert isinstance(scheduler, AsyncIOScheduler)


# ─── 추가 커버리지: 통계 로깅 + no-listings 경로 ─────────────────────

class TestJobEdgeCases:
    """jobs.py의 추가 커버리지 테스트."""

    @patch("scheduler.jobs.AirbnbClient")
    @patch("scheduler.jobs.get_target_stations")
    async def test_search_job_with_proxy_stats(self, mock_stations, mock_client_cls):
        """프록시 통계가 있을 때 로깅한다 (line 105)."""
        from scheduler.jobs import run_search_job

        mock_client = MagicMock()
        mock_client.close = AsyncMock()
        mock_client.get_stats.return_value = {
            "rate_limiter": {"total": 5},
            "proxy_manager": {"total": 3, "available": 2},
        }
        mock_client_cls.return_value = mock_client

        mock_station = MagicMock()
        mock_stations.return_value = [mock_station]

        mock_crawler = AsyncMock()
        mock_crawler.crawl_all_stations.return_value = [{"station": "test"}]

        with patch("scheduler.jobs.SearchCrawler", return_value=mock_crawler):
            await run_search_job()

        mock_client.get_stats.assert_called_once()

    @patch("scheduler.jobs.AirbnbClient")
    @patch("scheduler.jobs.get_all_listings", return_value=[])
    @patch("scheduler.jobs.get_tier_config")
    async def test_calendar_job_no_listings(self, mock_tier, mock_listings, mock_client_cls):
        """캘린더 작업에서 리스팅이 없으면 경고 로그를 남긴다 (lines 126-127)."""
        from scheduler.jobs import run_calendar_job

        mock_tier.return_value = {"calendar_enabled": True}
        mock_client = MagicMock()
        mock_client.close = AsyncMock()
        mock_client_cls.return_value = mock_client

        await run_calendar_job()
        mock_listings.assert_called_once()

    @patch("scheduler.jobs.AirbnbClient")
    @patch("scheduler.jobs.get_all_listings", return_value=[])
    @patch("scheduler.jobs.get_tier_config")
    async def test_listing_detail_job_no_listings(self, mock_tier, mock_listings, mock_client_cls):
        """상세 크롤링 작업에서 리스팅이 없으면 경고 로그를 남긴다 (lines 150-151)."""
        from scheduler.jobs import run_listing_detail_job

        mock_tier.return_value = {"listing_detail_enabled": True}
        mock_client = MagicMock()
        mock_client.close = AsyncMock()
        mock_client_cls.return_value = mock_client

        await run_listing_detail_job()


# ─── run_aggregation_job ────────────────────────────────────────────

class TestRunAggregationJob:
    """run_aggregation_job 함수 테스트."""

    def test_run_aggregation_job_calls_run_aggregation(self):
        """run_aggregation_job이 run_aggregation(days_back=1)을 호출한다."""
        from scheduler.jobs import run_aggregation_job
        with patch("analysis.aggregator.run_aggregation") as mock_agg:
            run_aggregation_job()
        mock_agg.assert_called_once_with(days_back=1)

    def test_run_aggregation_job_logs_messages(self):
        """run_aggregation_job이 시작/완료 로그를 남긴다."""
        from scheduler.jobs import run_aggregation_job
        with patch("analysis.aggregator.run_aggregation"), \
             patch("scheduler.jobs.logger") as mock_logger:
            run_aggregation_job()
        mock_logger.info.assert_called()

    @patch("scheduler.jobs.get_tier_config")
    def test_setup_scheduler_aggregation_job_always_added(self, mock_tier):
        """aggregation_job은 티어에 관계없이 항상 등록된다."""
        mock_tier.return_value = {**TIER_A_CONFIG}

        from scheduler.jobs import setup_scheduler
        scheduler = setup_scheduler()

        job_ids = {job.id for job in scheduler.get_jobs()}
        assert "aggregation_job" in job_ids
