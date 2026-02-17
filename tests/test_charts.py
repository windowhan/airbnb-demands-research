"""Tests for dashboard/components/charts.py"""

from datetime import date, datetime

import pandas as pd
import pytest

from dashboard.components.charts import (
    build_booking_rate_timeseries,
    build_room_type_bar_data,
    build_station_summary,
    build_top_stations,
    build_price_distribution,
    format_korean_number,
)


# ---------------------------------------------------------------------------
# build_booking_rate_timeseries
# ---------------------------------------------------------------------------


class TestBuildBookingRateTimeseries:
    def test_empty_stats_returns_empty_df(self):
        df = build_booking_rate_timeseries([])
        assert isinstance(df, pd.DataFrame)
        assert df.empty
        assert list(df.columns) == ["date", "booking_rate"]

    def test_no_matching_room_type_returns_empty(self):
        stats = [
            {"date": date(2026, 2, 10), "booking_rate": 0.8, "room_type": "entire_home"}
        ]
        df = build_booking_rate_timeseries(stats, room_type="private_room")
        assert df.empty

    def test_returns_filtered_by_room_type(self):
        stats = [
            {"date": date(2026, 2, 10), "booking_rate": 0.8, "room_type": "entire_home"},
            {"date": date(2026, 2, 11), "booking_rate": 0.5, "room_type": "private_room"},
        ]
        df = build_booking_rate_timeseries(stats, room_type="entire_home")
        assert len(df) == 1
        assert df.iloc[0]["booking_rate"] == 0.8

    def test_returns_none_room_type(self):
        stats = [
            {"date": date(2026, 2, 10), "booking_rate": 0.7, "room_type": None},
            {"date": date(2026, 2, 11), "booking_rate": 0.6, "room_type": None},
        ]
        df = build_booking_rate_timeseries(stats, room_type=None)
        assert len(df) == 2

    def test_sorted_by_date(self):
        stats = [
            {"date": date(2026, 2, 15), "booking_rate": 0.9, "room_type": None},
            {"date": date(2026, 2, 10), "booking_rate": 0.7, "room_type": None},
        ]
        df = build_booking_rate_timeseries(stats, room_type=None)
        assert df.iloc[0]["booking_rate"] == 0.7
        assert df.iloc[1]["booking_rate"] == 0.9

    def test_date_column_is_datetime(self):
        stats = [{"date": date(2026, 2, 10), "booking_rate": 0.5, "room_type": None}]
        df = build_booking_rate_timeseries(stats, room_type=None)
        assert pd.api.types.is_datetime64_any_dtype(df["date"])


# ---------------------------------------------------------------------------
# build_room_type_bar_data
# ---------------------------------------------------------------------------


class TestBuildRoomTypeBarData:
    def test_empty_stats_returns_empty_df(self):
        df = build_room_type_bar_data([])
        assert df.empty
        assert "room_type" in df.columns

    def test_groups_by_room_type(self):
        stats = [
            {"room_type": "entire_home", "booking_rate": 0.8,
             "avg_daily_price": 100000.0, "estimated_revenue": 500000.0},
            {"room_type": "entire_home", "booking_rate": 0.6,
             "avg_daily_price": 80000.0, "estimated_revenue": 300000.0},
            {"room_type": "private_room", "booking_rate": 0.5,
             "avg_daily_price": 50000.0, "estimated_revenue": 200000.0},
        ]
        df = build_room_type_bar_data(stats)
        assert len(df) == 2
        eh = df[df["room_type"] == "entire_home"].iloc[0]
        assert eh["booking_rate"] == pytest.approx(0.7)  # avg of 0.8, 0.6
        assert eh["estimated_revenue"] == pytest.approx(800000.0)  # sum

    def test_filters_none_room_type(self):
        stats = [
            {"room_type": None, "booking_rate": 0.8,
             "avg_daily_price": 100000.0, "estimated_revenue": 500000.0},
            {"room_type": "entire_home", "booking_rate": 0.6,
             "avg_daily_price": 80000.0, "estimated_revenue": 300000.0},
        ]
        df = build_room_type_bar_data(stats)
        # None room_type filtered out
        assert None not in df["room_type"].values

    def test_sorted_by_booking_rate_desc(self):
        stats = [
            {"room_type": "hotel", "booking_rate": 0.3,
             "avg_daily_price": 150000.0, "estimated_revenue": 100000.0},
            {"room_type": "entire_home", "booking_rate": 0.9,
             "avg_daily_price": 100000.0, "estimated_revenue": 500000.0},
        ]
        df = build_room_type_bar_data(stats)
        assert df.iloc[0]["room_type"] == "entire_home"

    def test_missing_columns_handled(self):
        stats = [{"room_type": "entire_home"}]
        df = build_room_type_bar_data(stats)
        assert "booking_rate" in df.columns
        assert "avg_daily_price" in df.columns
        assert "estimated_revenue" in df.columns


# ---------------------------------------------------------------------------
# build_station_summary
# ---------------------------------------------------------------------------


class TestBuildStationSummary:
    def test_no_stations_returns_empty(self):
        df = build_station_summary([], [])
        assert df.empty

    def test_merges_station_and_stats(self):
        stations = [
            {"id": 1, "name": "강남", "latitude": 37.498, "longitude": 127.028},
        ]
        stats = [
            {"station_id": 1, "booking_rate": 0.8,
             "estimated_revenue": 500000.0, "total_listings": 10, "room_type": None},
        ]
        df = build_station_summary(stats, stations)
        assert len(df) == 1
        assert df.iloc[0]["booking_rate"] == 0.8
        assert df.iloc[0]["name"] == "강남"

    def test_no_stats_fills_zeros(self):
        stations = [
            {"id": 1, "name": "홍대", "latitude": 37.557, "longitude": 126.924},
        ]
        df = build_station_summary([], stations)
        assert df.iloc[0]["booking_rate"] == 0.0
        assert df.iloc[0]["estimated_revenue"] == 0.0
        assert df.iloc[0]["total_listings"] == 0

    def test_only_none_room_type_used(self):
        stations = [{"id": 1, "name": "강남", "latitude": 37.498, "longitude": 127.028}]
        stats = [
            {"station_id": 1, "booking_rate": 0.8,
             "estimated_revenue": 500000.0, "total_listings": 10, "room_type": "entire_home"},
            {"station_id": 1, "booking_rate": 0.6,
             "estimated_revenue": 300000.0, "total_listings": 5, "room_type": None},
        ]
        df = build_station_summary(stats, stations)
        # Only room_type=None row should be used
        assert df.iloc[0]["booking_rate"] == 0.6

    def test_station_without_stats_has_zero_metrics(self):
        stations = [
            {"id": 1, "name": "강남", "latitude": 37.498, "longitude": 127.028},
            {"id": 2, "name": "홍대", "latitude": 37.557, "longitude": 126.924},
        ]
        stats = [
            {"station_id": 1, "booking_rate": 0.8,
             "estimated_revenue": 500000.0, "total_listings": 10, "room_type": None},
        ]
        df = build_station_summary(stats, stations)
        hongdae = df[df["name"] == "홍대"].iloc[0]
        assert hongdae["booking_rate"] == 0.0


# ---------------------------------------------------------------------------
# build_top_stations
# ---------------------------------------------------------------------------


class TestBuildTopStations:
    def test_empty_df_returns_empty(self):
        empty = pd.DataFrame(columns=["station_id", "name", "booking_rate"])
        result = build_top_stations(empty)
        assert result.empty

    def test_returns_top_n_by_booking_rate(self):
        df = pd.DataFrame([
            {"station_id": 1, "name": "A", "booking_rate": 0.9, "estimated_revenue": 100.0},
            {"station_id": 2, "name": "B", "booking_rate": 0.5, "estimated_revenue": 200.0},
            {"station_id": 3, "name": "C", "booking_rate": 0.7, "estimated_revenue": 150.0},
        ])
        result = build_top_stations(df, metric="booking_rate", n=2)
        assert len(result) == 2
        assert result.iloc[0]["name"] == "A"
        assert result.iloc[1]["name"] == "C"

    def test_returns_top_n_by_revenue(self):
        df = pd.DataFrame([
            {"station_id": 1, "name": "A", "booking_rate": 0.9, "estimated_revenue": 100.0},
            {"station_id": 2, "name": "B", "booking_rate": 0.5, "estimated_revenue": 200.0},
        ])
        result = build_top_stations(df, metric="estimated_revenue", n=1)
        assert result.iloc[0]["name"] == "B"

    def test_invalid_metric_falls_back_to_booking_rate(self):
        df = pd.DataFrame([
            {"station_id": 1, "name": "A", "booking_rate": 0.9, "estimated_revenue": 100.0},
        ])
        result = build_top_stations(df, metric="nonexistent")
        assert len(result) == 1

    def test_n_larger_than_df_returns_all(self):
        df = pd.DataFrame([
            {"station_id": i, "name": str(i), "booking_rate": 0.5, "estimated_revenue": 100.0}
            for i in range(3)
        ])
        result = build_top_stations(df, n=100)
        assert len(result) == 3


# ---------------------------------------------------------------------------
# build_price_distribution
# ---------------------------------------------------------------------------


class TestBuildPriceDistribution:
    def test_empty_stats_returns_empty(self):
        df = build_price_distribution([])
        assert df.empty

    def test_returns_room_type_and_price_columns(self):
        stats = [
            {"room_type": "entire_home", "avg_daily_price": 100000.0},
            {"room_type": "private_room", "avg_daily_price": 50000.0},
        ]
        df = build_price_distribution(stats)
        assert list(df.columns) == ["room_type", "avg_daily_price"]
        assert len(df) == 2

    def test_drops_rows_without_room_type(self):
        stats = [
            {"room_type": None, "avg_daily_price": 100000.0},
            {"room_type": "entire_home", "avg_daily_price": 80000.0},
        ]
        df = build_price_distribution(stats)
        assert len(df) == 1

    def test_missing_columns_returns_empty(self):
        stats = [{"some_other_key": 123}]
        df = build_price_distribution(stats)
        assert df.empty


# ---------------------------------------------------------------------------
# format_korean_number
# ---------------------------------------------------------------------------


class TestFormatKoreanNumber:
    def test_small_number(self):
        assert format_korean_number(5000) == "5,000원"

    def test_man_won(self):
        result = format_korean_number(150000)
        assert "만원" in result
        assert "15.0" in result

    def test_eok_won(self):
        result = format_korean_number(200_000_000)
        assert "억원" in result
        assert "2.0" in result

    def test_exactly_10000(self):
        result = format_korean_number(10000)
        assert "1.0만원" == result

    def test_exactly_100_million(self):
        result = format_korean_number(100_000_000)
        assert "1.0억원" == result

    def test_zero(self):
        result = format_korean_number(0)
        assert result == "0원"
