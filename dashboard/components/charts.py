"""재사용 차트 컴포넌트 - 데이터 변환 로직.

Streamlit UI 렌더링은 각 페이지에서 담당하며,
이 모듈은 DataFrame/dict 변환 순수 함수만 포함합니다.
"""

from __future__ import annotations

from datetime import date
from typing import Optional

import pandas as pd


def build_booking_rate_timeseries(
    stats: list[dict],
    room_type: Optional[str] = None,
) -> pd.DataFrame:
    """daily_stats 목록을 날짜별 예약률 시계열 DataFrame으로 변환합니다.

    Args:
        stats: DailyStat dict 목록 (keys: date, booking_rate, room_type)
        room_type: 필터링할 숙소 유형 (None = 전체)

    Returns:
        columns: [date, booking_rate]
    """
    filtered = [
        s for s in stats
        if s.get("room_type") == room_type
    ]
    if not filtered:
        return pd.DataFrame(columns=["date", "booking_rate"])

    df = pd.DataFrame(filtered)[["date", "booking_rate"]]
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    return df


def build_room_type_bar_data(stats: list[dict]) -> pd.DataFrame:
    """room_type별 평균 예약률/수익 DataFrame을 반환합니다.

    Args:
        stats: DailyStat dict 목록

    Returns:
        columns: [room_type, booking_rate, avg_daily_price, estimated_revenue]
    """
    if not stats:
        return pd.DataFrame(
            columns=["room_type", "booking_rate", "avg_daily_price", "estimated_revenue"]
        )

    df = pd.DataFrame(stats)
    required = {"room_type", "booking_rate", "avg_daily_price", "estimated_revenue"}
    for col in required:
        if col not in df.columns:
            df[col] = 0.0

    df = df[df["room_type"].notna()]
    result = (
        df.groupby("room_type", as_index=False)
        .agg(
            booking_rate=("booking_rate", "mean"),
            avg_daily_price=("avg_daily_price", "mean"),
            estimated_revenue=("estimated_revenue", "sum"),
        )
        .sort_values("booking_rate", ascending=False)
        .reset_index(drop=True)
    )
    return result


def build_station_summary(
    stats: list[dict],
    stations: list[dict],
) -> pd.DataFrame:
    """역 정보와 daily_stats를 조인하여 요약 DataFrame을 반환합니다.

    Args:
        stats: DailyStat dict 목록 (room_type=None인 전체 집계)
        stations: Station dict 목록 (keys: id, name, latitude, longitude)

    Returns:
        columns: [station_id, name, latitude, longitude,
                  booking_rate, estimated_revenue, total_listings]
    """
    if not stations:
        return pd.DataFrame(
            columns=[
                "station_id", "name", "latitude", "longitude",
                "booking_rate", "estimated_revenue", "total_listings",
            ]
        )

    stn_df = pd.DataFrame(stations).rename(columns={"id": "station_id"})

    # room_type=None인 전체 집계만 사용
    total_stats = [s for s in stats if s.get("room_type") is None]
    if not total_stats:
        stn_df["booking_rate"] = 0.0
        stn_df["estimated_revenue"] = 0.0
        stn_df["total_listings"] = 0
        return stn_df[
            ["station_id", "name", "latitude", "longitude",
             "booking_rate", "estimated_revenue", "total_listings"]
        ]

    stat_df = pd.DataFrame(total_stats)[
        ["station_id", "booking_rate", "estimated_revenue", "total_listings"]
    ]
    merged = stn_df.merge(stat_df, on="station_id", how="left").fillna(0)
    return merged[
        ["station_id", "name", "latitude", "longitude",
         "booking_rate", "estimated_revenue", "total_listings"]
    ]


def build_top_stations(
    summary_df: pd.DataFrame,
    metric: str = "booking_rate",
    n: int = 10,
) -> pd.DataFrame:
    """상위 N개 역 DataFrame을 반환합니다.

    Args:
        summary_df: build_station_summary() 결과
        metric: 정렬 기준 컬럼 ('booking_rate' or 'estimated_revenue')
        n: 상위 몇 개

    Returns:
        상위 n개 역 DataFrame
    """
    if summary_df.empty:
        return summary_df

    if metric not in summary_df.columns:
        metric = "booking_rate"

    return (
        summary_df.sort_values(metric, ascending=False)
        .head(n)
        .reset_index(drop=True)
    )


def build_price_distribution(stats: list[dict]) -> pd.DataFrame:
    """가격 분포 DataFrame을 반환합니다.

    Args:
        stats: DailyStat dict 목록

    Returns:
        columns: [room_type, avg_daily_price]
    """
    if not stats:
        return pd.DataFrame(columns=["room_type", "avg_daily_price"])

    df = pd.DataFrame(stats)
    if "room_type" not in df.columns or "avg_daily_price" not in df.columns:
        return pd.DataFrame(columns=["room_type", "avg_daily_price"])

    return df[["room_type", "avg_daily_price"]].dropna().reset_index(drop=True)


def format_korean_number(value: float) -> str:
    """숫자를 한국식 단위(만원)로 포맷합니다.

    Args:
        value: KRW 금액

    Returns:
        '123.4만원' 형식 문자열
    """
    if value >= 100_000_000:
        return f"{value / 100_000_000:.1f}억원"
    if value >= 10_000:
        return f"{value / 10_000:.1f}만원"
    return f"{int(value):,}원"
