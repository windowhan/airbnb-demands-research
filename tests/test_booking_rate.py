"""Tests for analysis/booking_rate.py"""

from datetime import date, datetime, timedelta
from unittest.mock import patch, MagicMock

import pytest

from models.schema import CalendarSnapshot, Listing
from analysis.booking_rate import (
    get_latest_snapshots,
    calculate_booking_rate,
    is_actually_booked,
    get_station_booking_rate,
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
# get_latest_snapshots
# ---------------------------------------------------------------------------

class TestGetLatestSnapshots:
    def test_empty_result_when_no_snapshots(self, db_session, sample_listing):
        result = get_latest_snapshots(
            db_session,
            sample_listing.id,
            date(2026, 1, 1),
            date(2026, 1, 31),
        )
        assert result == []

    def test_single_date_single_snapshot(self, db_session, sample_listing):
        target = date(2026, 2, 10)
        make_snapshot(db_session, sample_listing.id, target, False,
                      datetime(2026, 2, 10, 8, 0, 0))
        db_session.commit()

        result = get_latest_snapshots(
            db_session,
            sample_listing.id,
            date(2026, 2, 1),
            date(2026, 2, 28),
        )
        assert len(result) == 1
        assert result[0].date == target

    def test_multiple_snapshots_per_date_returns_latest(self, db_session, sample_listing):
        target = date(2026, 2, 10)
        # older snapshot
        make_snapshot(db_session, sample_listing.id, target, True,
                      datetime(2026, 2, 10, 6, 0, 0))
        # newer snapshot
        newer = make_snapshot(db_session, sample_listing.id, target, False,
                              datetime(2026, 2, 10, 12, 0, 0))
        db_session.commit()

        result = get_latest_snapshots(
            db_session,
            sample_listing.id,
            date(2026, 2, 1),
            date(2026, 2, 28),
        )
        assert len(result) == 1
        assert result[0].crawled_at == datetime(2026, 2, 10, 12, 0, 0)
        assert result[0].available is False

    def test_multiple_dates_returns_one_per_date(self, db_session, sample_listing):
        for day in [10, 11, 12]:
            make_snapshot(db_session, sample_listing.id, date(2026, 2, day), True,
                          datetime(2026, 2, day, 8, 0, 0))
        db_session.commit()

        result = get_latest_snapshots(
            db_session,
            sample_listing.id,
            date(2026, 2, 1),
            date(2026, 2, 28),
        )
        assert len(result) == 3

    def test_filters_by_listing_id(self, db_session, sample_listing, sample_station):
        # Create a second listing
        listing2 = Listing(
            airbnb_id="9999999999",
            name="Other listing",
            room_type="private_room",
            nearest_station_id=sample_station.id,
            first_seen=datetime.utcnow(),
            last_seen=datetime.utcnow(),
        )
        db_session.add(listing2)
        db_session.flush()

        target = date(2026, 2, 10)
        make_snapshot(db_session, sample_listing.id, target, False,
                      datetime(2026, 2, 10, 8, 0, 0))
        make_snapshot(db_session, listing2.id, target, True,
                      datetime(2026, 2, 10, 8, 0, 0))
        db_session.commit()

        result = get_latest_snapshots(
            db_session,
            sample_listing.id,
            date(2026, 2, 1),
            date(2026, 2, 28),
        )
        assert len(result) == 1
        assert result[0].listing_id == sample_listing.id


# ---------------------------------------------------------------------------
# calculate_booking_rate
# ---------------------------------------------------------------------------

class TestCalculateBookingRate:
    def test_no_data_returns_zero(self, mock_session_scope):
        with patch("analysis.booking_rate.session_scope", mock_session_scope):
            rate = calculate_booking_rate(9999, date(2026, 1, 1), date(2026, 1, 31))
        assert rate == 0.0

    def test_all_booked_returns_one(self, db_session, mock_session_scope, sample_listing):
        for day in range(1, 6):
            make_snapshot(db_session, sample_listing.id, date(2026, 2, day), False,
                          datetime(2026, 2, day, 8, 0, 0))
        db_session.commit()

        with patch("analysis.booking_rate.session_scope", mock_session_scope):
            rate = calculate_booking_rate(
                sample_listing.id, date(2026, 2, 1), date(2026, 2, 5)
            )
        assert rate == 1.0

    def test_all_available_returns_zero(self, db_session, mock_session_scope, sample_listing):
        for day in range(1, 6):
            make_snapshot(db_session, sample_listing.id, date(2026, 2, day), True,
                          datetime(2026, 2, day, 8, 0, 0))
        db_session.commit()

        with patch("analysis.booking_rate.session_scope", mock_session_scope):
            rate = calculate_booking_rate(
                sample_listing.id, date(2026, 2, 1), date(2026, 2, 5)
            )
        assert rate == 0.0

    def test_mixed_returns_correct_ratio(self, db_session, mock_session_scope, sample_listing):
        # 2 booked, 2 available → 0.5
        make_snapshot(db_session, sample_listing.id, date(2026, 2, 1), False,
                      datetime(2026, 2, 1, 8, 0, 0))
        make_snapshot(db_session, sample_listing.id, date(2026, 2, 2), False,
                      datetime(2026, 2, 2, 8, 0, 0))
        make_snapshot(db_session, sample_listing.id, date(2026, 2, 3), True,
                      datetime(2026, 2, 3, 8, 0, 0))
        make_snapshot(db_session, sample_listing.id, date(2026, 2, 4), True,
                      datetime(2026, 2, 4, 8, 0, 0))
        db_session.commit()

        with patch("analysis.booking_rate.session_scope", mock_session_scope):
            rate = calculate_booking_rate(
                sample_listing.id, date(2026, 2, 1), date(2026, 2, 4)
            )
        assert rate == 0.5


# ---------------------------------------------------------------------------
# is_actually_booked
# ---------------------------------------------------------------------------

class TestIsActuallyBooked:
    def test_less_than_two_snapshots_returns_false(self, db_session, sample_listing):
        target = date(2026, 2, 10)
        make_snapshot(db_session, sample_listing.id, target, False,
                      datetime(2026, 2, 10, 8, 0, 0))
        db_session.commit()

        result = is_actually_booked(db_session, sample_listing.id, target)
        assert result is False

    def test_zero_snapshots_returns_false(self, db_session, sample_listing):
        target = date(2026, 2, 10)
        result = is_actually_booked(db_session, sample_listing.id, target)
        assert result is False

    def test_latest_available_returns_false(self, db_session, sample_listing):
        target = date(2026, 2, 10)
        # Earlier: unavailable, latest: available
        make_snapshot(db_session, sample_listing.id, target, False,
                      datetime(2026, 2, 10, 6, 0, 0))
        make_snapshot(db_session, sample_listing.id, target, True,
                      datetime(2026, 2, 10, 12, 0, 0))
        db_session.commit()

        result = is_actually_booked(db_session, sample_listing.id, target)
        assert result is False

    def test_was_never_available_returns_false(self, db_session, sample_listing):
        target = date(2026, 2, 10)
        # Both snapshots show unavailable — never available (host block)
        make_snapshot(db_session, sample_listing.id, target, False,
                      datetime(2026, 2, 10, 6, 0, 0))
        make_snapshot(db_session, sample_listing.id, target, False,
                      datetime(2026, 2, 10, 12, 0, 0))
        db_session.commit()

        result = is_actually_booked(db_session, sample_listing.id, target)
        assert result is False

    def test_actually_booked_true_then_false(self, db_session, sample_listing):
        target = date(2026, 2, 10)
        # Earlier: available, latest: unavailable → actual booking
        make_snapshot(db_session, sample_listing.id, target, True,
                      datetime(2026, 2, 10, 6, 0, 0))
        make_snapshot(db_session, sample_listing.id, target, False,
                      datetime(2026, 2, 10, 12, 0, 0))
        db_session.commit()

        result = is_actually_booked(db_session, sample_listing.id, target)
        assert result is True

    def test_multiple_history_with_true_then_false(self, db_session, sample_listing):
        target = date(2026, 2, 10)
        # Three snapshots: True, True, False → actually booked
        make_snapshot(db_session, sample_listing.id, target, True,
                      datetime(2026, 2, 10, 4, 0, 0))
        make_snapshot(db_session, sample_listing.id, target, True,
                      datetime(2026, 2, 10, 8, 0, 0))
        make_snapshot(db_session, sample_listing.id, target, False,
                      datetime(2026, 2, 10, 12, 0, 0))
        db_session.commit()

        result = is_actually_booked(db_session, sample_listing.id, target)
        assert result is True


# ---------------------------------------------------------------------------
# get_station_booking_rate
# ---------------------------------------------------------------------------

class TestGetStationBookingRate:
    def test_no_listings_returns_zero_dict(self, mock_session_scope):
        with patch("analysis.booking_rate.session_scope", mock_session_scope):
            result = get_station_booking_rate(9999, date(2026, 2, 10))

        assert result["station_id"] == 9999
        assert result["total_listings"] == 0
        assert result["booking_rate"] == 0.0
        assert result["booked_count"] == 0

    def test_no_listings_logs_info(self, mock_session_scope):
        with patch("analysis.booking_rate.session_scope", mock_session_scope), \
             patch("analysis.booking_rate.logger") as mock_logger:
            get_station_booking_rate(9999, date(2026, 2, 10))
        mock_logger.info.assert_called()

    def test_with_listings_calculates_rate(
        self, db_session, mock_session_scope, sample_listing, sample_station
    ):
        target = date(2026, 2, 10)
        start = target - timedelta(days=29)
        # Add one booked snapshot within window
        make_snapshot(db_session, sample_listing.id, target, False,
                      datetime(2026, 2, 10, 8, 0, 0))
        db_session.commit()

        with patch("analysis.booking_rate.session_scope", mock_session_scope), \
             patch("analysis.booking_rate.calculate_booking_rate") as mock_calc:
            mock_calc.return_value = 0.8
            result = get_station_booking_rate(sample_station.id, target)

        assert result["total_listings"] == 1
        assert result["booking_rate"] == 0.8
        assert result["booked_count"] == 1

    def test_with_room_type_filter(
        self, db_session, mock_session_scope, sample_listing, sample_station
    ):
        # sample_listing is entire_home; filtering for private_room → no listings
        with patch("analysis.booking_rate.session_scope", mock_session_scope):
            result = get_station_booking_rate(
                sample_station.id, date(2026, 2, 10), room_type="private_room"
            )

        assert result["total_listings"] == 0
        assert result["room_type"] == "private_room"

    def test_with_room_type_match(
        self, db_session, mock_session_scope, sample_listing, sample_station
    ):
        # sample_listing is entire_home
        with patch("analysis.booking_rate.session_scope", mock_session_scope), \
             patch("analysis.booking_rate.calculate_booking_rate") as mock_calc:
            mock_calc.return_value = 0.5
            result = get_station_booking_rate(
                sample_station.id, date(2026, 2, 10), room_type="entire_home"
            )

        assert result["total_listings"] == 1
        assert result["room_type"] == "entire_home"

    def test_logger_info_called_with_listings(
        self, db_session, mock_session_scope, sample_listing, sample_station
    ):
        with patch("analysis.booking_rate.session_scope", mock_session_scope), \
             patch("analysis.booking_rate.calculate_booking_rate", return_value=0.0), \
             patch("analysis.booking_rate.logger") as mock_logger:
            get_station_booking_rate(sample_station.id, date(2026, 2, 10))

        mock_logger.info.assert_called()

    def test_result_structure(self, mock_session_scope):
        with patch("analysis.booking_rate.session_scope", mock_session_scope):
            result = get_station_booking_rate(1, date(2026, 2, 10))

        assert set(result.keys()) == {
            "station_id", "date", "room_type", "total_listings",
            "booking_rate", "booked_count",
        }

    def test_window_days_affects_date_range(
        self, db_session, mock_session_scope, sample_listing, sample_station
    ):
        target = date(2026, 2, 10)
        with patch("analysis.booking_rate.session_scope", mock_session_scope), \
             patch("analysis.booking_rate.calculate_booking_rate") as mock_calc:
            mock_calc.return_value = 0.3
            result = get_station_booking_rate(
                sample_station.id, target, window_days=7
            )
        # calculate_booking_rate called with start_date = target - 6 days
        expected_start = target - timedelta(days=6)
        mock_calc.assert_called_once_with(sample_listing.id, expected_start, target)
