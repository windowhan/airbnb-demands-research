"""Tests for dashboard/pages/*.py - 비즈니스 로직(데이터 fetch 함수) 테스트."""

from datetime import date, datetime, timedelta
from unittest.mock import patch

import pytest

from models.schema import DailyStat, Listing, Station, CrawlLog


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_station(session, name="강남", line="2호선", district="강남구",
                 lat=37.498, lng=127.028, priority=1):
    stn = Station(
        name=name, line=line, district=district,
        latitude=lat, longitude=lng, priority=priority,
    )
    session.add(stn)
    session.flush()
    return stn


def make_listing(session, station_id, airbnb_id="ABC123",
                 room_type="entire_home", name="Test Listing",
                 lat=37.498, lng=127.028, base_price=100000.0, bedrooms=1):
    lst = Listing(
        airbnb_id=airbnb_id,
        name=name,
        room_type=room_type,
        nearest_station_id=station_id,
        latitude=lat,
        longitude=lng,
        base_price=base_price,
        bedrooms=bedrooms,
        first_seen=datetime.utcnow(),
        last_seen=datetime.utcnow(),
    )
    session.add(lst)
    session.flush()
    return lst


def make_daily_stat(session, station_id, target_date, room_type=None,
                    booking_rate=0.5, avg_daily_price=100000.0,
                    estimated_revenue=500000.0, total_listings=10,
                    booked_count=5):
    stat = DailyStat(
        station_id=station_id,
        date=target_date,
        room_type=room_type,
        booking_rate=booking_rate,
        avg_daily_price=avg_daily_price,
        estimated_revenue=estimated_revenue,
        total_listings=total_listings,
        booked_count=booked_count,
    )
    session.add(stat)
    session.flush()
    return stat


# ---------------------------------------------------------------------------
# overview.py
# ---------------------------------------------------------------------------


class TestGetSummaryMetrics:
    def test_no_data_returns_zeros(self, db_session):
        from dashboard.pages.overview import get_summary_metrics
        result = get_summary_metrics(db_session, date(2026, 2, 10))
        assert result["total_listings"] == 0
        assert result["total_stations"] == 0
        assert result["avg_booking_rate"] == 0.0
        assert result["avg_daily_price"] == 0.0
        assert result["total_estimated_revenue"] == 0.0

    def test_with_listings_and_stations(self, db_session):
        from dashboard.pages.overview import get_summary_metrics
        stn = make_station(db_session)
        make_listing(db_session, stn.id)
        db_session.commit()

        result = get_summary_metrics(db_session, date(2026, 2, 10))
        assert result["total_listings"] == 1
        assert result["total_stations"] == 1

    def test_with_daily_stats(self, db_session):
        from dashboard.pages.overview import get_summary_metrics
        stn = make_station(db_session)
        target = date(2026, 2, 10)
        make_daily_stat(
            db_session, stn.id, target,
            room_type=None, booking_rate=0.8,
            avg_daily_price=120000.0, estimated_revenue=600000.0,
        )
        db_session.commit()

        result = get_summary_metrics(db_session, target)
        assert result["avg_booking_rate"] == pytest.approx(0.8)
        assert result["avg_daily_price"] == pytest.approx(120000.0)
        assert result["total_estimated_revenue"] == pytest.approx(600000.0)

    def test_avg_across_multiple_stations(self, db_session):
        from dashboard.pages.overview import get_summary_metrics
        stn1 = make_station(db_session, name="강남")
        stn2 = make_station(db_session, name="홍대")
        target = date(2026, 2, 10)
        make_daily_stat(db_session, stn1.id, target, room_type=None, booking_rate=0.8,
                        avg_daily_price=100000.0, estimated_revenue=100.0)
        make_daily_stat(db_session, stn2.id, target, room_type=None, booking_rate=0.4,
                        avg_daily_price=100000.0, estimated_revenue=100.0)
        db_session.commit()

        result = get_summary_metrics(db_session, target)
        assert result["avg_booking_rate"] == pytest.approx(0.6)

    def test_avg_price_excludes_zero_prices(self, db_session):
        from dashboard.pages.overview import get_summary_metrics
        stn = make_station(db_session)
        target = date(2026, 2, 10)
        make_daily_stat(db_session, stn.id, target, room_type=None,
                        booking_rate=0.5, avg_daily_price=0.0, estimated_revenue=0.0)
        db_session.commit()

        result = get_summary_metrics(db_session, target)
        assert result["avg_daily_price"] == 0.0


class TestGetStationMapStats:
    def test_no_stations_returns_empty(self, db_session):
        from dashboard.pages.overview import get_station_map_stats
        result = get_station_map_stats(db_session, date(2026, 2, 10))
        assert result == []

    def test_station_without_stat_has_zeros(self, db_session):
        from dashboard.pages.overview import get_station_map_stats
        make_station(db_session, name="강남")
        db_session.commit()

        result = get_station_map_stats(db_session, date(2026, 2, 10))
        assert len(result) == 1
        assert result[0]["booking_rate"] == 0.0
        assert result[0]["estimated_revenue"] == 0.0

    def test_station_with_stat_returns_values(self, db_session):
        from dashboard.pages.overview import get_station_map_stats
        stn = make_station(db_session, name="강남", lat=37.498, lng=127.028)
        target = date(2026, 2, 10)
        make_daily_stat(db_session, stn.id, target, room_type=None,
                        booking_rate=0.75, estimated_revenue=750000.0)
        db_session.commit()

        result = get_station_map_stats(db_session, target)
        assert len(result) == 1
        row = result[0]
        assert row["name"] == "강남"
        assert row["booking_rate"] == pytest.approx(0.75)
        assert row["latitude"] == pytest.approx(37.498)

    def test_result_has_required_keys(self, db_session):
        from dashboard.pages.overview import get_station_map_stats
        make_station(db_session)
        db_session.commit()

        result = get_station_map_stats(db_session, date(2026, 2, 10))
        required = {"station_id", "name", "latitude", "longitude",
                    "booking_rate", "estimated_revenue", "total_listings"}
        assert set(result[0].keys()) == required


class TestGetRecentCrawlLog:
    def test_no_log_returns_none(self, db_session):
        from dashboard.pages.overview import get_recent_crawl_log
        result = get_recent_crawl_log(db_session)
        assert result is None

    def test_returns_most_recent_log(self, db_session):
        from dashboard.pages.overview import get_recent_crawl_log

        log1 = CrawlLog(
            job_type="search",
            started_at=datetime(2026, 2, 9, 3, 0),
            status="success",
            total_requests=10,
            successful_requests=10,
            blocked_requests=0,
        )
        log2 = CrawlLog(
            job_type="calendar",
            started_at=datetime(2026, 2, 10, 3, 0),
            status="success",
            total_requests=20,
            successful_requests=18,
            blocked_requests=2,
        )
        db_session.add_all([log1, log2])
        db_session.commit()

        result = get_recent_crawl_log(db_session)
        assert result is not None
        assert result["job_type"] == "calendar"

    def test_result_has_required_keys(self, db_session):
        from dashboard.pages.overview import get_recent_crawl_log

        log = CrawlLog(
            job_type="search",
            started_at=datetime(2026, 2, 10, 3, 0),
            status="success",
            total_requests=5,
            successful_requests=5,
            blocked_requests=0,
        )
        db_session.add(log)
        db_session.commit()

        result = get_recent_crawl_log(db_session)
        assert set(result.keys()) == {
            "job_type", "started_at", "status",
            "total_requests", "successful_requests", "blocked_requests",
        }


class TestGetBookingRateTrend:
    def test_returns_n_days_of_data(self, db_session):
        from dashboard.pages.overview import get_booking_rate_trend
        result = get_booking_rate_trend(db_session, days=7)
        assert len(result) == 7

    def test_date_range_correct(self, db_session):
        from dashboard.pages.overview import get_booking_rate_trend
        result = get_booking_rate_trend(db_session, days=3)
        today = datetime.utcnow().date()
        expected_dates = [today - timedelta(days=i) for i in range(2, -1, -1)]
        actual_dates = [r["date"] for r in result]
        assert actual_dates == expected_dates

    def test_no_data_returns_zeros(self, db_session):
        from dashboard.pages.overview import get_booking_rate_trend
        result = get_booking_rate_trend(db_session, days=5)
        assert all(r["booking_rate"] == 0.0 for r in result)

    def test_with_stats_calculates_average(self, db_session):
        from dashboard.pages.overview import get_booking_rate_trend
        stn1 = make_station(db_session, name="강남")
        stn2 = make_station(db_session, name="홍대")
        today = datetime.utcnow().date()

        make_daily_stat(db_session, stn1.id, today, room_type=None, booking_rate=0.8)
        make_daily_stat(db_session, stn2.id, today, room_type=None, booking_rate=0.4)
        db_session.commit()

        result = get_booking_rate_trend(db_session, days=3)
        today_entry = next(r for r in result if r["date"] == today)
        assert today_entry["booking_rate"] == pytest.approx(0.6)

    def test_with_room_type_filter(self, db_session):
        from dashboard.pages.overview import get_booking_rate_trend
        stn = make_station(db_session)
        today = datetime.utcnow().date()
        make_daily_stat(db_session, stn.id, today, room_type="entire_home", booking_rate=0.9)
        make_daily_stat(db_session, stn.id, today, room_type=None, booking_rate=0.5)
        db_session.commit()

        result = get_booking_rate_trend(db_session, days=2, room_type="entire_home")
        today_entry = next(r for r in result if r["date"] == today)
        assert today_entry["booking_rate"] == pytest.approx(0.9)


# ---------------------------------------------------------------------------
# station_detail.py
# ---------------------------------------------------------------------------


class TestGetStationOptions:
    def test_no_stations_returns_empty(self, db_session):
        from dashboard.pages.station_detail import get_station_options
        result = get_station_options(db_session)
        assert result == []

    def test_returns_id_name_tuples(self, db_session):
        from dashboard.pages.station_detail import get_station_options
        make_station(db_session, name="강남", line="2호선")
        db_session.commit()

        result = get_station_options(db_session)
        assert len(result) == 1
        assert result[0][1] == "강남 (2호선)"

    def test_sorted_by_name(self, db_session):
        from dashboard.pages.station_detail import get_station_options
        make_station(db_session, name="홍대")
        make_station(db_session, name="강남")
        db_session.commit()

        result = get_station_options(db_session)
        assert result[0][1].startswith("강남")
        assert result[1][1].startswith("홍대")


class TestGetStationTimeseries:
    def test_no_stats_returns_empty(self, db_session):
        from dashboard.pages.station_detail import get_station_timeseries
        stn = make_station(db_session)
        db_session.commit()

        result = get_station_timeseries(db_session, stn.id, days=7)
        assert result == []

    def test_returns_stats_within_window(self, db_session):
        from dashboard.pages.station_detail import get_station_timeseries
        stn = make_station(db_session)
        today = datetime.utcnow().date()
        make_daily_stat(db_session, stn.id, today, room_type=None, booking_rate=0.7)
        db_session.commit()

        result = get_station_timeseries(db_session, stn.id, days=7)
        assert len(result) == 1
        assert result[0]["booking_rate"] == pytest.approx(0.7)

    def test_result_has_required_keys(self, db_session):
        from dashboard.pages.station_detail import get_station_timeseries
        stn = make_station(db_session)
        today = datetime.utcnow().date()
        make_daily_stat(db_session, stn.id, today, room_type=None)
        db_session.commit()

        result = get_station_timeseries(db_session, stn.id, days=7)
        required = {"date", "booking_rate", "avg_daily_price",
                    "estimated_revenue", "booked_count", "total_listings"}
        assert set(result[0].keys()) == required

    def test_filters_by_room_type(self, db_session):
        from dashboard.pages.station_detail import get_station_timeseries
        stn = make_station(db_session)
        today = datetime.utcnow().date()
        make_daily_stat(db_session, stn.id, today, room_type="entire_home", booking_rate=0.9)
        make_daily_stat(db_session, stn.id, today, room_type=None, booking_rate=0.5)
        db_session.commit()

        result = get_station_timeseries(db_session, stn.id, days=7, room_type="entire_home")
        assert len(result) == 1
        assert result[0]["booking_rate"] == pytest.approx(0.9)

    def test_excludes_stats_outside_window(self, db_session):
        from dashboard.pages.station_detail import get_station_timeseries
        stn = make_station(db_session)
        old_date = datetime.utcnow().date() - timedelta(days=100)
        make_daily_stat(db_session, stn.id, old_date, room_type=None, booking_rate=0.9)
        db_session.commit()

        result = get_station_timeseries(db_session, stn.id, days=7)
        assert result == []


class TestGetStationListings:
    def test_no_listings_returns_empty(self, db_session):
        from dashboard.pages.station_detail import get_station_listings
        stn = make_station(db_session)
        db_session.commit()

        result = get_station_listings(db_session, stn.id)
        assert result == []

    def test_returns_listing_info(self, db_session):
        from dashboard.pages.station_detail import get_station_listings
        stn = make_station(db_session)
        make_listing(db_session, stn.id, name="Nice Place", room_type="entire_home",
                     base_price=150000.0, bedrooms=2)
        db_session.commit()

        result = get_station_listings(db_session, stn.id)
        assert len(result) == 1
        assert result[0]["name"] == "Nice Place"
        assert result[0]["room_type"] == "entire_home"
        assert result[0]["base_price"] == pytest.approx(150000.0)

    def test_result_has_required_keys(self, db_session):
        from dashboard.pages.station_detail import get_station_listings
        stn = make_station(db_session)
        make_listing(db_session, stn.id)
        db_session.commit()

        result = get_station_listings(db_session, stn.id)
        required = {"id", "name", "room_type", "latitude", "longitude",
                    "base_price", "bedrooms"}
        assert set(result[0].keys()) == required


class TestGetStationRoomTypeStats:
    def test_no_stats_returns_empty(self, db_session):
        from dashboard.pages.station_detail import get_station_room_type_stats
        stn = make_station(db_session)
        db_session.commit()

        result = get_station_room_type_stats(db_session, stn.id, date(2026, 2, 10))
        assert result == []

    def test_excludes_none_room_type(self, db_session):
        from dashboard.pages.station_detail import get_station_room_type_stats
        stn = make_station(db_session)
        target = date(2026, 2, 10)
        make_daily_stat(db_session, stn.id, target, room_type=None, booking_rate=0.7)
        make_daily_stat(db_session, stn.id, target, room_type="entire_home", booking_rate=0.8)
        db_session.commit()

        result = get_station_room_type_stats(db_session, stn.id, target)
        assert len(result) == 1
        assert result[0]["room_type"] == "entire_home"

    def test_result_has_required_keys(self, db_session):
        from dashboard.pages.station_detail import get_station_room_type_stats
        stn = make_station(db_session)
        target = date(2026, 2, 10)
        make_daily_stat(db_session, stn.id, target, room_type="entire_home")
        db_session.commit()

        result = get_station_room_type_stats(db_session, stn.id, target)
        required = {"room_type", "total_listings", "booked_count",
                    "booking_rate", "avg_daily_price", "estimated_revenue"}
        assert set(result[0].keys()) == required


# ---------------------------------------------------------------------------
# listing_type.py
# ---------------------------------------------------------------------------


class TestGetRoomTypeDailyStats:
    def test_no_data_returns_empty(self, db_session):
        from dashboard.pages.listing_type import get_room_type_daily_stats
        result = get_room_type_daily_stats(db_session, date(2026, 2, 10))
        assert result == []

    def test_groups_by_room_type(self, db_session):
        from dashboard.pages.listing_type import get_room_type_daily_stats
        stn1 = make_station(db_session, name="강남")
        stn2 = make_station(db_session, name="홍대")
        target = date(2026, 2, 10)
        make_daily_stat(db_session, stn1.id, target, room_type="entire_home",
                        total_listings=5, booked_count=3, booking_rate=0.6,
                        avg_daily_price=100000.0, estimated_revenue=300000.0)
        make_daily_stat(db_session, stn2.id, target, room_type="entire_home",
                        total_listings=3, booked_count=2, booking_rate=0.7,
                        avg_daily_price=80000.0, estimated_revenue=160000.0)
        db_session.commit()

        result = get_room_type_daily_stats(db_session, target)
        assert len(result) == 1
        assert result[0]["room_type"] == "entire_home"
        assert result[0]["total_listings"] == 8
        assert result[0]["total_revenue"] == pytest.approx(460000.0)

    def test_excludes_none_room_type(self, db_session):
        from dashboard.pages.listing_type import get_room_type_daily_stats
        stn = make_station(db_session)
        target = date(2026, 2, 10)
        make_daily_stat(db_session, stn.id, target, room_type=None, booking_rate=0.7)
        make_daily_stat(db_session, stn.id, target, room_type="hotel", booking_rate=0.3)
        db_session.commit()

        result = get_room_type_daily_stats(db_session, target)
        room_types = [r["room_type"] for r in result]
        assert None not in room_types

    def test_result_has_required_keys(self, db_session):
        from dashboard.pages.listing_type import get_room_type_daily_stats
        stn = make_station(db_session)
        target = date(2026, 2, 10)
        make_daily_stat(db_session, stn.id, target, room_type="private_room")
        db_session.commit()

        result = get_room_type_daily_stats(db_session, target)
        required = {"room_type", "total_listings", "booked_count",
                    "booking_rate", "avg_daily_price", "total_revenue"}
        assert set(result[0].keys()) == required


class TestGetRoomTypeTrend:
    def test_returns_n_days(self, db_session):
        from dashboard.pages.listing_type import get_room_type_trend
        result = get_room_type_trend(db_session, "entire_home", days=7)
        assert len(result) == 7

    def test_no_data_returns_zeros(self, db_session):
        from dashboard.pages.listing_type import get_room_type_trend
        result = get_room_type_trend(db_session, "entire_home", days=3)
        assert all(r["booking_rate"] == 0.0 for r in result)

    def test_with_stats_calculates_average(self, db_session):
        from dashboard.pages.listing_type import get_room_type_trend
        stn1 = make_station(db_session, name="강남")
        stn2 = make_station(db_session, name="홍대")
        today = datetime.utcnow().date()
        make_daily_stat(db_session, stn1.id, today, room_type="entire_home", booking_rate=0.8,
                        avg_daily_price=100000.0)
        make_daily_stat(db_session, stn2.id, today, room_type="entire_home", booking_rate=0.4,
                        avg_daily_price=80000.0)
        db_session.commit()

        result = get_room_type_trend(db_session, "entire_home", days=3)
        today_entry = next(r for r in result if r["date"] == today)
        assert today_entry["booking_rate"] == pytest.approx(0.6)

    def test_result_has_required_keys(self, db_session):
        from dashboard.pages.listing_type import get_room_type_trend
        result = get_room_type_trend(db_session, "entire_home", days=3)
        required = {"date", "booking_rate", "avg_daily_price"}
        assert set(result[0].keys()) == required

    def test_average_price_excludes_zero(self, db_session):
        from dashboard.pages.listing_type import get_room_type_trend
        stn = make_station(db_session)
        today = datetime.utcnow().date()
        make_daily_stat(db_session, stn.id, today, room_type="entire_home",
                        booking_rate=0.5, avg_daily_price=0.0)
        db_session.commit()

        result = get_room_type_trend(db_session, "entire_home", days=2)
        today_entry = next(r for r in result if r["date"] == today)
        assert today_entry["avg_daily_price"] == 0.0


class TestGetListingCountByRoomType:
    def test_no_listings_returns_empty(self, db_session):
        from dashboard.pages.listing_type import get_listing_count_by_room_type
        result = get_listing_count_by_room_type(db_session)
        assert result == {}

    def test_counts_by_room_type(self, db_session):
        from dashboard.pages.listing_type import get_listing_count_by_room_type
        stn = make_station(db_session)
        make_listing(db_session, stn.id, airbnb_id="A1", room_type="entire_home")
        make_listing(db_session, stn.id, airbnb_id="A2", room_type="entire_home")
        make_listing(db_session, stn.id, airbnb_id="A3", room_type="private_room")
        db_session.commit()

        result = get_listing_count_by_room_type(db_session)
        assert result["entire_home"] == 2
        assert result["private_room"] == 1

    def test_excludes_none_room_type(self, db_session):
        from dashboard.pages.listing_type import get_listing_count_by_room_type
        stn = make_station(db_session)
        lst = Listing(
            airbnb_id="N1", name="No Type",
            nearest_station_id=stn.id,
            first_seen=datetime.utcnow(),
            last_seen=datetime.utcnow(),
        )
        db_session.add(lst)
        db_session.commit()

        result = get_listing_count_by_room_type(db_session)
        assert None not in result


# ---------------------------------------------------------------------------
# revenue_map.py
# ---------------------------------------------------------------------------


class TestGetRevenueRanking:
    def test_no_data_returns_empty(self, db_session):
        from dashboard.pages.revenue_map import get_revenue_ranking
        result = get_revenue_ranking(db_session, date(2026, 2, 10))
        assert result == []

    def test_returns_top_n_by_revenue(self, db_session):
        from dashboard.pages.revenue_map import get_revenue_ranking
        stn1 = make_station(db_session, name="강남", lat=37.498, lng=127.028)
        stn2 = make_station(db_session, name="홍대", lat=37.557, lng=126.924)
        target = date(2026, 2, 10)
        make_daily_stat(db_session, stn1.id, target, room_type=None,
                        estimated_revenue=1000000.0, booking_rate=0.8)
        make_daily_stat(db_session, stn2.id, target, room_type=None,
                        estimated_revenue=500000.0, booking_rate=0.5)
        db_session.commit()

        result = get_revenue_ranking(db_session, target, n=2)
        assert len(result) == 2
        assert result[0]["name"] == "강남"
        assert result[0]["rank"] == 1

    def test_result_has_required_keys(self, db_session):
        from dashboard.pages.revenue_map import get_revenue_ranking
        stn = make_station(db_session)
        target = date(2026, 2, 10)
        make_daily_stat(db_session, stn.id, target, room_type=None,
                        estimated_revenue=100000.0)
        db_session.commit()

        result = get_revenue_ranking(db_session, target)
        required = {"rank", "station_id", "name", "latitude", "longitude",
                    "estimated_revenue", "booking_rate", "total_listings"}
        assert set(result[0].keys()) == required

    def test_room_type_filter(self, db_session):
        from dashboard.pages.revenue_map import get_revenue_ranking
        stn = make_station(db_session)
        target = date(2026, 2, 10)
        make_daily_stat(db_session, stn.id, target, room_type="entire_home",
                        estimated_revenue=100000.0)
        make_daily_stat(db_session, stn.id, target, room_type=None,
                        estimated_revenue=200000.0)
        db_session.commit()

        result = get_revenue_ranking(db_session, target, room_type="entire_home")
        assert len(result) == 1
        assert result[0]["estimated_revenue"] == pytest.approx(100000.0)

    def test_n_limit_respected(self, db_session):
        from dashboard.pages.revenue_map import get_revenue_ranking
        for i in range(5):
            stn = make_station(db_session, name=f"역{i}")
            make_daily_stat(db_session, stn.id, date(2026, 2, 10), room_type=None,
                            estimated_revenue=float(i * 1000))
        db_session.commit()

        result = get_revenue_ranking(db_session, date(2026, 2, 10), n=3)
        assert len(result) == 3


class TestGetMonthlyRevenueSummary:
    def test_no_data_returns_empty(self, db_session):
        from dashboard.pages.revenue_map import get_monthly_revenue_summary
        result = get_monthly_revenue_summary(db_session, 2026, 2)
        assert result == []

    def test_sums_revenue_for_month(self, db_session):
        from dashboard.pages.revenue_map import get_monthly_revenue_summary
        stn = make_station(db_session, name="강남")
        make_daily_stat(db_session, stn.id, date(2026, 2, 1), room_type=None,
                        estimated_revenue=100000.0, booking_rate=0.5)
        make_daily_stat(db_session, stn.id, date(2026, 2, 2), room_type=None,
                        estimated_revenue=200000.0, booking_rate=0.7)
        db_session.commit()

        result = get_monthly_revenue_summary(db_session, 2026, 2)
        assert len(result) == 1
        assert result[0]["total_revenue"] == pytest.approx(300000.0)
        assert result[0]["name"] == "강남"

    def test_sorted_by_revenue_desc(self, db_session):
        from dashboard.pages.revenue_map import get_monthly_revenue_summary
        stn1 = make_station(db_session, name="강남")
        stn2 = make_station(db_session, name="홍대")
        make_daily_stat(db_session, stn1.id, date(2026, 2, 1), room_type=None,
                        estimated_revenue=500000.0)
        make_daily_stat(db_session, stn2.id, date(2026, 2, 1), room_type=None,
                        estimated_revenue=300000.0)
        db_session.commit()

        result = get_monthly_revenue_summary(db_session, 2026, 2)
        assert result[0]["name"] == "강남"

    def test_excludes_dates_outside_month(self, db_session):
        from dashboard.pages.revenue_map import get_monthly_revenue_summary
        stn = make_station(db_session)
        # Jan data - should not appear in Feb summary
        make_daily_stat(db_session, stn.id, date(2026, 1, 31), room_type=None,
                        estimated_revenue=999999.0)
        db_session.commit()

        result = get_monthly_revenue_summary(db_session, 2026, 2)
        assert result == []

    def test_result_has_required_keys(self, db_session):
        from dashboard.pages.revenue_map import get_monthly_revenue_summary
        stn = make_station(db_session)
        make_daily_stat(db_session, stn.id, date(2026, 2, 1), room_type=None,
                        estimated_revenue=100000.0)
        db_session.commit()

        result = get_monthly_revenue_summary(db_session, 2026, 2)
        required = {"station_id", "name", "latitude", "longitude",
                    "total_revenue", "avg_booking_rate"}
        assert set(result[0].keys()) == required

    def test_with_room_type_filter(self, db_session):
        """room_type 필터링 else 분기 (line 103)를 커버합니다."""
        from dashboard.pages.revenue_map import get_monthly_revenue_summary
        stn = make_station(db_session)
        make_daily_stat(db_session, stn.id, date(2026, 2, 1), room_type="entire_home",
                        estimated_revenue=200000.0)
        make_daily_stat(db_session, stn.id, date(2026, 2, 1), room_type=None,
                        estimated_revenue=100000.0)
        db_session.commit()

        result = get_monthly_revenue_summary(db_session, 2026, 2, room_type="entire_home")
        assert len(result) == 1
        assert result[0]["total_revenue"] == pytest.approx(200000.0)

    def test_skips_stat_with_missing_station(self, db_session):
        """station_map에 없는 station_id는 건너뜁니다 (line 112)."""
        from dashboard.pages.revenue_map import get_monthly_revenue_summary

        # DailyStat with non-existent station_id (SQLite doesn't enforce FK strictly)
        orphan = DailyStat(
            station_id=99999,
            date=date(2026, 2, 1),
            room_type=None,
            booking_rate=0.5,
            avg_daily_price=100000.0,
            estimated_revenue=500000.0,
            total_listings=5,
            booked_count=2,
        )
        db_session.add(orphan)
        db_session.commit()

        # Should not raise, just skip the orphan entry
        result = get_monthly_revenue_summary(db_session, 2026, 2)
        assert result == []


class TestGetRevenueHeatmapData:
    def test_no_data_returns_empty(self, db_session):
        from dashboard.pages.revenue_map import get_revenue_heatmap_data
        result = get_revenue_heatmap_data(db_session, date(2026, 2, 10))
        assert result == []

    def test_returns_tuples_with_normalized_revenue(self, db_session):
        from dashboard.pages.revenue_map import get_revenue_heatmap_data
        stn = make_station(db_session, lat=37.498, lng=127.028)
        make_daily_stat(db_session, stn.id, date(2026, 2, 10), room_type=None,
                        estimated_revenue=100000.0)
        db_session.commit()

        result = get_revenue_heatmap_data(db_session, date(2026, 2, 10))
        assert len(result) == 1
        lat, lng, weight = result[0]
        assert lat == pytest.approx(37.498)
        assert lng == pytest.approx(127.028)
        assert weight == pytest.approx(1.0)  # max/max = 1.0

    def test_zero_max_revenue_returns_zero_weights(self, db_session):
        from dashboard.pages.revenue_map import get_revenue_heatmap_data
        stn = make_station(db_session, lat=37.498, lng=127.028)
        make_daily_stat(db_session, stn.id, date(2026, 2, 10), room_type=None,
                        estimated_revenue=0.0)
        db_session.commit()

        result = get_revenue_heatmap_data(db_session, date(2026, 2, 10))
        # revenue=0 → filtered as 0.0 weight from zero-check
        assert all(w == 0.0 for _, _, w in result)

    def test_normalization_relative_to_max(self, db_session):
        from dashboard.pages.revenue_map import get_revenue_heatmap_data
        stn1 = make_station(db_session, name="A", lat=37.5, lng=127.0)
        stn2 = make_station(db_session, name="B", lat=37.6, lng=127.1)
        target = date(2026, 2, 10)
        make_daily_stat(db_session, stn1.id, target, room_type=None, estimated_revenue=200000.0)
        make_daily_stat(db_session, stn2.id, target, room_type=None, estimated_revenue=100000.0)
        db_session.commit()

        result = get_revenue_heatmap_data(db_session, target)
        assert len(result) == 2
        weights = [w for _, _, w in result]
        assert max(weights) == pytest.approx(1.0)
        assert min(weights) == pytest.approx(0.5)
