"""Tests for analysis/revenue.py"""

import calendar
from datetime import date, datetime
from unittest.mock import patch, MagicMock

import pytest

from models.schema import CalendarSnapshot, Listing
from analysis.revenue import (
    _get_latest_snapshot,
    estimate_listing_daily_revenue,
    estimate_listing_monthly_revenue,
    estimate_station_revenue,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_snapshot(session, listing_id, snap_date, available, crawled_at, price=None):
    snap = CalendarSnapshot(
        listing_id=listing_id,
        date=snap_date,
        available=available,
        price=price,
        crawled_at=crawled_at,
    )
    session.add(snap)
    session.flush()
    return snap


# ---------------------------------------------------------------------------
# _get_latest_snapshot
# ---------------------------------------------------------------------------

class TestGetLatestSnapshot:
    def test_no_snapshot_returns_none(self, db_session, sample_listing):
        result = _get_latest_snapshot(db_session, sample_listing.id, date(2026, 2, 10))
        assert result is None

    def test_has_snapshot_returns_latest(self, db_session, sample_listing):
        target = date(2026, 2, 10)
        make_snapshot(db_session, sample_listing.id, target, False,
                      datetime(2026, 2, 10, 6, 0, 0), price=80000.0)
        latest = make_snapshot(db_session, sample_listing.id, target, False,
                               datetime(2026, 2, 10, 12, 0, 0), price=90000.0)
        db_session.commit()

        result = _get_latest_snapshot(db_session, sample_listing.id, target)
        assert result is not None
        assert result.price == 90000.0
        assert result.crawled_at == datetime(2026, 2, 10, 12, 0, 0)

    def test_returns_snapshot_for_correct_date(self, db_session, sample_listing):
        make_snapshot(db_session, sample_listing.id, date(2026, 2, 10), True,
                      datetime(2026, 2, 10, 8, 0, 0))
        make_snapshot(db_session, sample_listing.id, date(2026, 2, 11), False,
                      datetime(2026, 2, 11, 8, 0, 0), price=100000.0)
        db_session.commit()

        result = _get_latest_snapshot(db_session, sample_listing.id, date(2026, 2, 10))
        assert result.available is True


# ---------------------------------------------------------------------------
# estimate_listing_daily_revenue
# ---------------------------------------------------------------------------

class TestEstimateListingDailyRevenue:
    def test_no_snapshot_returns_zero(self, db_session, sample_listing):
        result = estimate_listing_daily_revenue(
            db_session, sample_listing.id, date(2026, 2, 10)
        )
        assert result == 0.0

    def test_available_true_returns_zero(self, db_session, sample_listing):
        target = date(2026, 2, 10)
        make_snapshot(db_session, sample_listing.id, target, True,
                      datetime(2026, 2, 10, 8, 0, 0), price=100000.0)
        db_session.commit()

        result = estimate_listing_daily_revenue(db_session, sample_listing.id, target)
        assert result == 0.0

    def test_available_false_no_price_returns_zero(self, db_session, sample_listing):
        target = date(2026, 2, 10)
        make_snapshot(db_session, sample_listing.id, target, False,
                      datetime(2026, 2, 10, 8, 0, 0), price=None)
        db_session.commit()

        result = estimate_listing_daily_revenue(db_session, sample_listing.id, target)
        assert result == 0.0

    def test_available_false_with_price_returns_price(self, db_session, sample_listing):
        target = date(2026, 2, 10)
        make_snapshot(db_session, sample_listing.id, target, False,
                      datetime(2026, 2, 10, 8, 0, 0), price=150000.0)
        db_session.commit()

        result = estimate_listing_daily_revenue(db_session, sample_listing.id, target)
        assert result == 150000.0

    def test_available_none_returns_zero(self, db_session, sample_listing):
        target = date(2026, 2, 10)
        make_snapshot(db_session, sample_listing.id, target, None,
                      datetime(2026, 2, 10, 8, 0, 0), price=100000.0)
        db_session.commit()

        result = estimate_listing_daily_revenue(db_session, sample_listing.id, target)
        assert result == 0.0


# ---------------------------------------------------------------------------
# estimate_listing_monthly_revenue
# ---------------------------------------------------------------------------

class TestEstimateListingMonthlyRevenue:
    def test_uses_session_scope_and_sums_days(
        self, db_session, mock_session_scope, sample_listing
    ):
        year, month = 2026, 2
        _, days_in_month = calendar.monthrange(year, month)

        # Make Feb 1 and Feb 15 booked with price, rest available
        make_snapshot(db_session, sample_listing.id, date(2026, 2, 1), False,
                      datetime(2026, 2, 1, 8, 0, 0), price=100000.0)
        make_snapshot(db_session, sample_listing.id, date(2026, 2, 15), False,
                      datetime(2026, 2, 15, 8, 0, 0), price=200000.0)
        # Rest are available (or missing) → 0.0 revenue
        db_session.commit()

        with patch("analysis.revenue.session_scope", mock_session_scope):
            result = estimate_listing_monthly_revenue(sample_listing.id, year, month)

        assert result == 300000.0

    def test_no_bookings_returns_zero(
        self, db_session, mock_session_scope, sample_listing
    ):
        with patch("analysis.revenue.session_scope", mock_session_scope):
            result = estimate_listing_monthly_revenue(sample_listing.id, 2026, 2)

        assert result == 0.0

    def test_all_days_booked_with_price(
        self, db_session, mock_session_scope, sample_listing
    ):
        year, month = 2026, 2
        _, days_in_month = calendar.monthrange(year, month)
        price_per_day = 50000.0

        for day in range(1, days_in_month + 1):
            make_snapshot(
                db_session, sample_listing.id, date(year, month, day), False,
                datetime(year, month, day, 8, 0, 0), price=price_per_day,
            )
        db_session.commit()

        with patch("analysis.revenue.session_scope", mock_session_scope):
            result = estimate_listing_monthly_revenue(sample_listing.id, year, month)

        assert result == price_per_day * days_in_month


# ---------------------------------------------------------------------------
# estimate_station_revenue
# ---------------------------------------------------------------------------

class TestEstimateStationRevenue:
    def test_no_listings_returns_zero_dict(self, mock_session_scope):
        with patch("analysis.revenue.session_scope", mock_session_scope):
            result = estimate_station_revenue(9999, date(2026, 2, 10))

        assert result["station_id"] == 9999
        assert result["total_listings"] == 0
        assert result["booked_count"] == 0
        assert result["total_revenue"] == 0.0
        assert result["avg_revenue"] == 0.0

    def test_no_listings_logs_info(self, mock_session_scope):
        with patch("analysis.revenue.session_scope", mock_session_scope), \
             patch("analysis.revenue.logger") as mock_logger:
            estimate_station_revenue(9999, date(2026, 2, 10))
        mock_logger.info.assert_called()

    def test_some_listings_with_revenue(
        self, db_session, mock_session_scope, sample_listing, sample_station
    ):
        target = date(2026, 2, 10)
        make_snapshot(db_session, sample_listing.id, target, False,
                      datetime(2026, 2, 10, 8, 0, 0), price=120000.0)
        db_session.commit()

        with patch("analysis.revenue.session_scope", mock_session_scope):
            result = estimate_station_revenue(sample_station.id, target)

        assert result["station_id"] == sample_station.id
        assert result["total_listings"] == 1
        assert result["booked_count"] == 1
        assert result["total_revenue"] == 120000.0
        assert result["avg_revenue"] == 120000.0

    def test_room_type_filter_no_match(
        self, db_session, mock_session_scope, sample_listing, sample_station
    ):
        # sample_listing is entire_home; filter for private_room → empty
        with patch("analysis.revenue.session_scope", mock_session_scope):
            result = estimate_station_revenue(
                sample_station.id, date(2026, 2, 10), room_type="private_room"
            )
        assert result["total_listings"] == 0
        assert result["room_type"] == "private_room"

    def test_room_type_filter_match(
        self, db_session, mock_session_scope, sample_listing, sample_station
    ):
        target = date(2026, 2, 10)
        make_snapshot(db_session, sample_listing.id, target, False,
                      datetime(2026, 2, 10, 8, 0, 0), price=100000.0)
        db_session.commit()

        with patch("analysis.revenue.session_scope", mock_session_scope):
            result = estimate_station_revenue(
                sample_station.id, target, room_type="entire_home"
            )
        assert result["total_listings"] == 1
        assert result["total_revenue"] == 100000.0

    def test_booked_count_zero_avg_revenue_is_zero(
        self, db_session, mock_session_scope, sample_listing, sample_station
    ):
        target = date(2026, 2, 10)
        # Listing exists but available (not booked), so revenue = 0
        make_snapshot(db_session, sample_listing.id, target, True,
                      datetime(2026, 2, 10, 8, 0, 0), price=100000.0)
        db_session.commit()

        with patch("analysis.revenue.session_scope", mock_session_scope):
            result = estimate_station_revenue(sample_station.id, target)

        assert result["booked_count"] == 0
        assert result["total_revenue"] == 0.0
        assert result["avg_revenue"] == 0.0

    def test_logger_info_called_with_listings(
        self, db_session, mock_session_scope, sample_listing, sample_station
    ):
        target = date(2026, 2, 10)
        make_snapshot(db_session, sample_listing.id, target, False,
                      datetime(2026, 2, 10, 8, 0, 0), price=100000.0)
        db_session.commit()

        with patch("analysis.revenue.session_scope", mock_session_scope), \
             patch("analysis.revenue.logger") as mock_logger:
            estimate_station_revenue(sample_station.id, target)

        mock_logger.info.assert_called()

    def test_result_structure(self, mock_session_scope):
        with patch("analysis.revenue.session_scope", mock_session_scope):
            result = estimate_station_revenue(1, date(2026, 2, 10))

        assert set(result.keys()) == {
            "station_id", "date", "room_type", "total_listings",
            "booked_count", "total_revenue", "avg_revenue",
        }

    def test_multiple_listings_avg_revenue(
        self, db_session, mock_session_scope, sample_station
    ):
        target = date(2026, 2, 10)
        # Create two listings
        for i, (airbnb_id, price) in enumerate([("AAA111", 100000.0), ("BBB222", 200000.0)]):
            listing = Listing(
                airbnb_id=airbnb_id,
                name=f"Listing {i}",
                room_type="entire_home",
                nearest_station_id=sample_station.id,
                first_seen=datetime.utcnow(),
                last_seen=datetime.utcnow(),
            )
            db_session.add(listing)
            db_session.flush()
            make_snapshot(db_session, listing.id, target, False,
                          datetime(2026, 2, 10, 8, 0, 0), price=price)
        db_session.commit()

        with patch("analysis.revenue.session_scope", mock_session_scope):
            result = estimate_station_revenue(sample_station.id, target)

        assert result["total_listings"] == 2
        assert result["booked_count"] == 2
        assert result["total_revenue"] == 300000.0
        assert result["avg_revenue"] == 150000.0
