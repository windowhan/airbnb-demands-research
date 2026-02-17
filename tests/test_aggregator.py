"""Tests for analysis/aggregator.py"""

from datetime import date, datetime, timedelta
from unittest.mock import patch, MagicMock, call

import pytest

from models.schema import CalendarSnapshot, DailyStat, Listing, Station
from analysis.aggregator import (
    ROOM_TYPES,
    _get_listing_ids,
    _get_date_stats,
    _upsert_daily_stat,
    aggregate_station_date,
    aggregate_daily_stats,
    run_aggregation,
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


def make_listing(session, airbnb_id, room_type, station_id):
    listing = Listing(
        airbnb_id=airbnb_id,
        name=f"Listing {airbnb_id}",
        room_type=room_type,
        nearest_station_id=station_id,
        first_seen=datetime.utcnow(),
        last_seen=datetime.utcnow(),
    )
    session.add(listing)
    session.flush()
    return listing


# ---------------------------------------------------------------------------
# ROOM_TYPES constant
# ---------------------------------------------------------------------------

class TestRoomTypes:
    def test_room_types_contains_expected_values(self):
        assert "entire_home" in ROOM_TYPES
        assert "private_room" in ROOM_TYPES
        assert "shared_room" in ROOM_TYPES
        assert "hotel" in ROOM_TYPES
        assert None in ROOM_TYPES


# ---------------------------------------------------------------------------
# _get_listing_ids
# ---------------------------------------------------------------------------

class TestGetListingIds:
    def test_no_listings_returns_empty_list(self, db_session, sample_station):
        result = _get_listing_ids(db_session, sample_station.id, None)
        assert result == []

    def test_with_listings_returns_ids(
        self, db_session, sample_station, sample_listing
    ):
        result = _get_listing_ids(db_session, sample_station.id, None)
        assert sample_listing.id in result

    def test_with_room_type_filter_no_match(
        self, db_session, sample_station, sample_listing
    ):
        # sample_listing is entire_home; filter private_room → empty
        result = _get_listing_ids(db_session, sample_station.id, "private_room")
        assert result == []

    def test_with_room_type_filter_match(
        self, db_session, sample_station, sample_listing
    ):
        # sample_listing is entire_home
        result = _get_listing_ids(db_session, sample_station.id, "entire_home")
        assert sample_listing.id in result

    def test_filters_by_station(self, db_session, sample_station):
        # Create a second station
        station2 = Station(
            name="홍대",
            line="2호선",
            district="마포구",
            latitude=37.556,
            longitude=126.923,
            priority=1,
        )
        db_session.add(station2)
        db_session.flush()

        listing2 = make_listing(db_session, "ZZZZZZ", "entire_home", station2.id)
        db_session.commit()

        result = _get_listing_ids(db_session, sample_station.id, None)
        # listing2 belongs to station2, not sample_station
        assert listing2.id not in result


# ---------------------------------------------------------------------------
# _get_date_stats
# ---------------------------------------------------------------------------

class TestGetDateStats:
    def test_empty_listing_ids_returns_zeros(self, db_session):
        result = _get_date_stats(db_session, [], date(2026, 2, 10))
        assert result == (0, 0.0, 0.0)

    def test_no_snapshots_for_date_returns_zeros(
        self, db_session, sample_listing
    ):
        result = _get_date_stats(
            db_session, [sample_listing.id], date(2026, 2, 10)
        )
        assert result == (0, 0.0, 0.0)

    def test_all_available_returns_zero_booked(
        self, db_session, sample_listing
    ):
        target = date(2026, 2, 10)
        make_snapshot(db_session, sample_listing.id, target, True,
                      datetime(2026, 2, 10, 8, 0, 0), price=100000.0)
        db_session.commit()

        booked_count, avg_price, total_revenue = _get_date_stats(
            db_session, [sample_listing.id], target
        )
        assert booked_count == 0
        assert total_revenue == 0.0

    def test_booked_and_available_mix(self, db_session, sample_station):
        target = date(2026, 2, 10)
        listing1 = make_listing(db_session, "L001", "entire_home", sample_station.id)
        listing2 = make_listing(db_session, "L002", "entire_home", sample_station.id)
        make_snapshot(db_session, listing1.id, target, False,
                      datetime(2026, 2, 10, 8, 0, 0), price=100000.0)
        make_snapshot(db_session, listing2.id, target, True,
                      datetime(2026, 2, 10, 8, 0, 0), price=80000.0)
        db_session.commit()

        booked_count, avg_price, total_revenue = _get_date_stats(
            db_session, [listing1.id, listing2.id], target
        )
        assert booked_count == 1
        assert total_revenue == 100000.0
        assert avg_price == 100000.0

    def test_prices_calculation_with_multiple_bookings(
        self, db_session, sample_station
    ):
        target = date(2026, 2, 10)
        listing1 = make_listing(db_session, "P001", "entire_home", sample_station.id)
        listing2 = make_listing(db_session, "P002", "entire_home", sample_station.id)
        make_snapshot(db_session, listing1.id, target, False,
                      datetime(2026, 2, 10, 8, 0, 0), price=100000.0)
        make_snapshot(db_session, listing2.id, target, False,
                      datetime(2026, 2, 10, 8, 0, 0), price=200000.0)
        db_session.commit()

        booked_count, avg_price, total_revenue = _get_date_stats(
            db_session, [listing1.id, listing2.id], target
        )
        assert booked_count == 2
        assert total_revenue == 300000.0
        assert avg_price == 150000.0

    def test_booked_without_price_not_counted_in_avg(
        self, db_session, sample_station
    ):
        target = date(2026, 2, 10)
        listing1 = make_listing(db_session, "NP001", "entire_home", sample_station.id)
        # Booked but no price
        make_snapshot(db_session, listing1.id, target, False,
                      datetime(2026, 2, 10, 8, 0, 0), price=None)
        db_session.commit()

        booked_count, avg_price, total_revenue = _get_date_stats(
            db_session, [listing1.id], target
        )
        assert booked_count == 1
        assert total_revenue == 0.0
        assert avg_price == 0.0

    def test_latest_snapshot_used_per_listing(self, db_session, sample_station):
        target = date(2026, 2, 10)
        listing = make_listing(db_session, "LATEST01", "entire_home", sample_station.id)
        # Older: available
        make_snapshot(db_session, listing.id, target, True,
                      datetime(2026, 2, 10, 6, 0, 0), price=50000.0)
        # Newer: booked
        make_snapshot(db_session, listing.id, target, False,
                      datetime(2026, 2, 10, 12, 0, 0), price=100000.0)
        db_session.commit()

        booked_count, avg_price, total_revenue = _get_date_stats(
            db_session, [listing.id], target
        )
        assert booked_count == 1
        assert total_revenue == 100000.0


# ---------------------------------------------------------------------------
# _upsert_daily_stat
# ---------------------------------------------------------------------------

class TestUpsertDailyStat:
    def test_insert_new_record(self, db_session, sample_station):
        target = date(2026, 2, 10)
        stat = _upsert_daily_stat(
            session=db_session,
            station_id=sample_station.id,
            target_date=target,
            room_type="entire_home",
            total_listings=5,
            booked_count=3,
            booking_rate=0.6,
            avg_daily_price=100000.0,
            estimated_revenue=300000.0,
        )
        db_session.commit()

        assert stat is not None
        assert stat.station_id == sample_station.id
        assert stat.date == target
        assert stat.room_type == "entire_home"
        assert stat.total_listings == 5
        assert stat.booked_count == 3
        assert stat.booking_rate == 0.6
        assert stat.avg_daily_price == 100000.0
        assert stat.estimated_revenue == 300000.0

        # Verify it's actually in DB
        db_stat = db_session.query(DailyStat).filter_by(
            station_id=sample_station.id, date=target, room_type="entire_home"
        ).first()
        assert db_stat is not None

    def test_update_existing_record(self, db_session, sample_station):
        target = date(2026, 2, 10)
        # Insert first
        stat1 = _upsert_daily_stat(
            session=db_session,
            station_id=sample_station.id,
            target_date=target,
            room_type="entire_home",
            total_listings=5,
            booked_count=3,
            booking_rate=0.6,
            avg_daily_price=100000.0,
            estimated_revenue=300000.0,
        )
        db_session.commit()

        # Update
        stat2 = _upsert_daily_stat(
            session=db_session,
            station_id=sample_station.id,
            target_date=target,
            room_type="entire_home",
            total_listings=10,
            booked_count=7,
            booking_rate=0.7,
            avg_daily_price=120000.0,
            estimated_revenue=840000.0,
        )
        db_session.commit()

        # Should be the same record updated
        count = db_session.query(DailyStat).filter_by(
            station_id=sample_station.id, date=target, room_type="entire_home"
        ).count()
        assert count == 1

        assert stat2.total_listings == 10
        assert stat2.booked_count == 7
        assert stat2.booking_rate == 0.7
        assert stat2.avg_daily_price == 120000.0
        assert stat2.estimated_revenue == 840000.0

    def test_insert_with_none_room_type(self, db_session, sample_station):
        target = date(2026, 2, 10)
        stat = _upsert_daily_stat(
            session=db_session,
            station_id=sample_station.id,
            target_date=target,
            room_type=None,
            total_listings=10,
            booked_count=5,
            booking_rate=0.5,
            avg_daily_price=90000.0,
            estimated_revenue=450000.0,
        )
        db_session.commit()
        assert stat.room_type is None


# ---------------------------------------------------------------------------
# aggregate_station_date
# ---------------------------------------------------------------------------

class TestAggregateStationDate:
    def test_no_listings_for_any_room_type_rows_written_zero(
        self, db_session, sample_station
    ):
        result = aggregate_station_date(db_session, sample_station.id, date(2026, 2, 10))
        assert result["rows_written"] == 0
        assert result["station_id"] == sample_station.id
        assert result["date"] == date(2026, 2, 10)

    def test_some_listings_writes_rows(
        self, db_session, sample_station, sample_listing
    ):
        # sample_listing is entire_home → triggers entire_home + None (all) room types
        target = date(2026, 2, 10)
        make_snapshot(db_session, sample_listing.id, target, False,
                      datetime(2026, 2, 10, 8, 0, 0), price=100000.0)
        db_session.commit()

        result = aggregate_station_date(db_session, sample_station.id, target)

        # Should write rows for "entire_home" and None (total)
        assert result["rows_written"] >= 2

    def test_result_structure(self, db_session, sample_station):
        result = aggregate_station_date(db_session, sample_station.id, date(2026, 2, 10))
        assert set(result.keys()) == {"station_id", "date", "rows_written"}

    def test_daily_stats_persisted_in_db(
        self, db_session, sample_station, sample_listing
    ):
        target = date(2026, 2, 10)
        make_snapshot(db_session, sample_listing.id, target, False,
                      datetime(2026, 2, 10, 8, 0, 0), price=100000.0)
        db_session.commit()

        aggregate_station_date(db_session, sample_station.id, target)
        db_session.commit()

        stats = db_session.query(DailyStat).filter_by(
            station_id=sample_station.id, date=target
        ).all()
        assert len(stats) >= 1


# ---------------------------------------------------------------------------
# aggregate_daily_stats
# ---------------------------------------------------------------------------

class TestAggregateDailyStats:
    def test_target_date_none_uses_yesterday(self, mock_session_scope, db_session):
        today = datetime.utcnow().date()
        yesterday = today - timedelta(days=1)

        with patch("analysis.aggregator.session_scope", mock_session_scope), \
             patch("analysis.aggregator.aggregate_station_date") as mock_agg:
            mock_agg.return_value = {"station_id": 1, "date": yesterday, "rows_written": 0}
            result = aggregate_daily_stats(target_date=None)

        assert result["date"] == yesterday

    def test_explicit_date_used(self, mock_session_scope, db_session):
        target = date(2026, 1, 15)

        with patch("analysis.aggregator.session_scope", mock_session_scope), \
             patch("analysis.aggregator.aggregate_station_date") as mock_agg:
            mock_agg.return_value = {"station_id": 1, "date": target, "rows_written": 0}
            result = aggregate_daily_stats(target_date=target)

        assert result["date"] == target

    def test_no_stations_returns_zero(self, mock_session_scope, db_session):
        with patch("analysis.aggregator.session_scope", mock_session_scope):
            result = aggregate_daily_stats(target_date=date(2026, 2, 10))

        assert result["stations_processed"] == 0
        assert result["total_rows"] == 0

    def test_processes_all_stations(
        self, db_session, mock_session_scope, sample_station
    ):
        # Add a second station
        station2 = Station(
            name="홍대",
            line="2호선",
            district="마포구",
            latitude=37.556,
            longitude=126.923,
            priority=1,
        )
        db_session.add(station2)
        db_session.commit()

        with patch("analysis.aggregator.session_scope", mock_session_scope):
            result = aggregate_daily_stats(target_date=date(2026, 2, 10))

        assert result["stations_processed"] == 2

    def test_result_structure(self, mock_session_scope):
        with patch("analysis.aggregator.session_scope", mock_session_scope):
            result = aggregate_daily_stats(target_date=date(2026, 2, 10))

        assert set(result.keys()) == {"date", "stations_processed", "total_rows"}

    def test_total_rows_summed(
        self, db_session, mock_session_scope, sample_station, sample_listing
    ):
        target = date(2026, 2, 10)
        make_snapshot(db_session, sample_listing.id, target, False,
                      datetime(2026, 2, 10, 8, 0, 0), price=100000.0)
        db_session.commit()

        with patch("analysis.aggregator.session_scope", mock_session_scope):
            result = aggregate_daily_stats(target_date=target)

        # At least 2 rows (entire_home + None/all)
        assert result["total_rows"] >= 2


# ---------------------------------------------------------------------------
# run_aggregation
# ---------------------------------------------------------------------------

class TestRunAggregation:
    def test_days_back_1_runs_for_yesterday(self, mock_session_scope):
        today = datetime.utcnow().date()
        yesterday = today - timedelta(days=1)

        with patch("analysis.aggregator.aggregate_daily_stats") as mock_agg:
            mock_agg.return_value = {
                "date": yesterday,
                "stations_processed": 0,
                "total_rows": 0,
            }
            run_aggregation(days_back=1)

        mock_agg.assert_called_once_with(yesterday)

    def test_days_back_3_runs_for_three_days(self, mock_session_scope):
        today = datetime.utcnow().date()
        expected_dates = [today - timedelta(days=i) for i in range(1, 4)]

        with patch("analysis.aggregator.aggregate_daily_stats") as mock_agg:
            mock_agg.return_value = {
                "date": None,
                "stations_processed": 0,
                "total_rows": 0,
            }
            run_aggregation(days_back=3)

        assert mock_agg.call_count == 3
        called_dates = [c.args[0] for c in mock_agg.call_args_list]
        for expected_date in expected_dates:
            assert expected_date in called_dates

    def test_run_aggregation_default_days_back(self):
        today = datetime.utcnow().date()
        yesterday = today - timedelta(days=1)

        with patch("analysis.aggregator.aggregate_daily_stats") as mock_agg:
            mock_agg.return_value = {
                "date": yesterday,
                "stations_processed": 0,
                "total_rows": 0,
            }
            run_aggregation()  # default days_back=1

        mock_agg.assert_called_once_with(yesterday)

    def test_run_aggregation_logs_completion(self):
        with patch("analysis.aggregator.aggregate_daily_stats") as mock_agg, \
             patch("analysis.aggregator.logger") as mock_logger:
            mock_agg.return_value = {
                "date": date(2026, 2, 10),
                "stations_processed": 0,
                "total_rows": 0,
            }
            run_aggregation(days_back=2)

        mock_logger.info.assert_called()
