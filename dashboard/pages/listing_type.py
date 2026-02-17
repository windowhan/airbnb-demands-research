"""숙소 유형별 분석 페이지.

데이터 fetch 함수 (testable) + Streamlit 렌더링 (# pragma: no cover).
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from models.database import session_scope
from models.schema import DailyStat, Listing

logger = logging.getLogger(__name__)

# 분석 대상 room_type
ROOM_TYPES = ["entire_home", "private_room", "shared_room", "hotel"]


# ---------------------------------------------------------------------------
# 데이터 fetch 함수 (비즈니스 로직 - 테스트 대상)
# ---------------------------------------------------------------------------


def get_room_type_daily_stats(
    session: Session,
    target_date: date,
) -> list[dict]:
    """특정 날짜의 room_type별 전체 집계를 반환합니다 (전체 역 합산).

    Returns:
        [{"room_type", "total_listings", "booked_count",
          "booking_rate", "avg_daily_price", "total_revenue"}, ...]
    """
    rows = (
        session.query(
            DailyStat.room_type,
            func.sum(DailyStat.total_listings).label("total_listings"),
            func.sum(DailyStat.booked_count).label("booked_count"),
            func.avg(DailyStat.booking_rate).label("booking_rate"),
            func.avg(DailyStat.avg_daily_price).label("avg_daily_price"),
            func.sum(DailyStat.estimated_revenue).label("total_revenue"),
        )
        .filter(
            DailyStat.date == target_date,
            DailyStat.room_type.isnot(None),
        )
        .group_by(DailyStat.room_type)
        .all()
    )

    return [
        {
            "room_type": row.room_type,
            "total_listings": row.total_listings or 0,
            "booked_count": row.booked_count or 0,
            "booking_rate": float(row.booking_rate or 0.0),
            "avg_daily_price": float(row.avg_daily_price or 0.0),
            "total_revenue": float(row.total_revenue or 0.0),
        }
        for row in rows
    ]


def get_room_type_trend(
    session: Session,
    room_type: str,
    days: int = 30,
) -> list[dict]:
    """특정 room_type의 최근 N일간 예약률 추이를 반환합니다 (전체 역 평균).

    Returns:
        [{"date", "booking_rate", "avg_daily_price"}, ...]
    """
    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=days - 1)

    stats = (
        session.query(DailyStat)
        .filter(
            DailyStat.room_type == room_type,
            DailyStat.date >= start_date,
            DailyStat.date <= end_date,
        )
        .order_by(DailyStat.date)
        .all()
    )

    # 날짜별 집계
    by_date: dict[date, list] = {}
    for s in stats:
        by_date.setdefault(s.date, []).append(
            (s.booking_rate, s.avg_daily_price)
        )

    result = []
    d = start_date
    while d <= end_date:
        entries = by_date.get(d, [])
        if entries:
            avg_rate = sum(e[0] for e in entries) / len(entries)
            prices = [e[1] for e in entries if e[1]]
            avg_price = sum(prices) / len(prices) if prices else 0.0
        else:
            avg_rate = 0.0
            avg_price = 0.0
        result.append(
            {"date": d, "booking_rate": avg_rate, "avg_daily_price": avg_price}
        )
        d += timedelta(days=1)
    return result


def get_listing_count_by_room_type(session: Session) -> dict[str, int]:
    """room_type별 총 숙소 수를 반환합니다.

    Returns:
        {"entire_home": 123, "private_room": 45, ...}
    """
    rows = (
        session.query(Listing.room_type, func.count(Listing.id).label("cnt"))
        .filter(Listing.room_type.isnot(None))
        .group_by(Listing.room_type)
        .all()
    )
    return {row.room_type: row.cnt for row in rows}


# ---------------------------------------------------------------------------
# Streamlit UI 렌더링 (# pragma: no cover)
# ---------------------------------------------------------------------------


def render_listing_type():  # pragma: no cover
    """숙소 유형별 분석 페이지를 렌더링합니다."""
    import streamlit as st
    import plotly.express as px

    from dashboard.components.charts import (
        build_room_type_bar_data,
        build_booking_rate_timeseries,
        format_korean_number,
    )

    st.title("숙소 유형별 분석")

    target_date = st.date_input("기준 날짜", value=datetime.utcnow().date())

    with session_scope() as session:
        daily_stats = get_room_type_daily_stats(session, target_date)
        listing_counts = get_listing_count_by_room_type(session)

    if not daily_stats:
        st.warning(f"{target_date} 데이터가 없습니다.")
        return

    col1, col2, col3, col4 = st.columns(4)
    for i, rt in enumerate(ROOM_TYPES):
        stat = next((s for s in daily_stats if s["room_type"] == rt), None)
        col = [col1, col2, col3, col4][i]
        col.metric(
            rt,
            f"{stat['booking_rate']:.1%}" if stat else "N/A",
            f"{listing_counts.get(rt, 0):,}개",
        )

    st.subheader("유형별 예약률 비교")
    bar_df = build_room_type_bar_data(daily_stats)
    if not bar_df.empty:
        fig = px.bar(
            bar_df, x="room_type", y="booking_rate",
            color="room_type", labels={"booking_rate": "예약률"},
        )
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("유형별 평균 가격")
    if not bar_df.empty:
        fig2 = px.bar(
            bar_df, x="room_type", y="avg_daily_price",
            color="room_type", labels={"avg_daily_price": "평균 일일 가격 (KRW)"},
        )
        st.plotly_chart(fig2, use_container_width=True)

    st.subheader("유형별 예약률 추이")
    selected_type = st.selectbox("유형 선택", ROOM_TYPES)
    days = st.slider("조회 기간 (일)", 7, 90, 30)

    with session_scope() as session:
        trend = get_room_type_trend(session, selected_type, days=days)

    trend_df = build_booking_rate_timeseries(trend, room_type=None)
    if not trend_df.empty:
        fig3 = px.line(trend_df, x="date", y="booking_rate")
        st.plotly_chart(fig3, use_container_width=True)
