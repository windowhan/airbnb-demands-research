"""역별 상세 분석 페이지.

데이터 fetch 함수 (testable) + Streamlit 렌더링 (# pragma: no cover).
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Optional

from sqlalchemy.orm import Session

from models.database import session_scope
from models.schema import DailyStat, Listing, Station

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 데이터 fetch 함수 (비즈니스 로직 - 테스트 대상)
# ---------------------------------------------------------------------------


def get_station_options(session: Session) -> list[tuple[int, str]]:
    """역 선택 드롭다운용 (id, name) 목록을 반환합니다.

    Returns:
        [(station_id, "역명 (노선)"), ...]
    """
    stations = session.query(Station).order_by(Station.name).all()
    return [(stn.id, f"{stn.name} ({stn.line})") for stn in stations]


def get_station_timeseries(
    session: Session,
    station_id: int,
    days: int = 30,
    room_type: Optional[str] = None,
) -> list[dict]:
    """특정 역의 최근 N일간 일별 통계를 반환합니다.

    Args:
        session: DB 세션
        station_id: 역 ID
        days: 조회 일수
        room_type: 숙소 유형 필터 (None = 전체 집계)

    Returns:
        [{"date", "booking_rate", "avg_daily_price", "estimated_revenue",
          "booked_count", "total_listings"}, ...]
    """
    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=days - 1)

    query = session.query(DailyStat).filter(
        DailyStat.station_id == station_id,
        DailyStat.date >= start_date,
        DailyStat.date <= end_date,
    )
    if room_type is None:
        query = query.filter(DailyStat.room_type.is_(None))
    else:
        query = query.filter(DailyStat.room_type == room_type)

    stats = query.order_by(DailyStat.date).all()

    return [
        {
            "date": s.date,
            "booking_rate": s.booking_rate,
            "avg_daily_price": s.avg_daily_price,
            "estimated_revenue": s.estimated_revenue,
            "booked_count": s.booked_count,
            "total_listings": s.total_listings,
        }
        for s in stats
    ]


def get_station_listings(
    session: Session,
    station_id: int,
) -> list[dict]:
    """역 주변 숙소 목록을 반환합니다.

    Returns:
        [{"id", "name", "room_type", "latitude", "longitude",
          "base_price", "bedrooms"}, ...]
    """
    listings = (
        session.query(Listing)
        .filter(Listing.nearest_station_id == station_id)
        .order_by(Listing.name)
        .all()
    )
    return [
        {
            "id": lst.id,
            "name": lst.name,
            "room_type": lst.room_type,
            "latitude": lst.latitude,
            "longitude": lst.longitude,
            "base_price": lst.base_price,
            "bedrooms": lst.bedrooms,
        }
        for lst in listings
    ]


def get_station_room_type_stats(
    session: Session,
    station_id: int,
    target_date: date,
) -> list[dict]:
    """특정 역의 특정 날짜 room_type별 통계를 반환합니다.

    Returns:
        [{"room_type", "total_listings", "booked_count",
          "booking_rate", "avg_daily_price", "estimated_revenue"}, ...]
    """
    stats = (
        session.query(DailyStat)
        .filter(
            DailyStat.station_id == station_id,
            DailyStat.date == target_date,
            DailyStat.room_type.isnot(None),
        )
        .all()
    )
    return [
        {
            "room_type": s.room_type,
            "total_listings": s.total_listings,
            "booked_count": s.booked_count,
            "booking_rate": s.booking_rate,
            "avg_daily_price": s.avg_daily_price,
            "estimated_revenue": s.estimated_revenue,
        }
        for s in stats
    ]


# ---------------------------------------------------------------------------
# Streamlit UI 렌더링 (# pragma: no cover)
# ---------------------------------------------------------------------------


def render_station_detail():  # pragma: no cover
    """역별 상세 분석 페이지를 렌더링합니다."""
    import streamlit as st
    import plotly.express as px
    import folium
    from streamlit_folium import st_folium

    from dashboard.components.charts import (
        build_booking_rate_timeseries,
        build_room_type_bar_data,
        format_korean_number,
    )

    st.title("역별 상세 분석")

    with session_scope() as session:
        options = get_station_options(session)

    if not options:
        st.warning("등록된 역이 없습니다.")
        return

    station_id, station_label = st.selectbox(
        "역 선택",
        options=options,
        format_func=lambda x: x[1],
    )
    days = st.slider("조회 기간 (일)", 7, 90, 30)

    with session_scope() as session:
        timeseries = get_station_timeseries(session, station_id, days=days)
        listings = get_station_listings(session, station_id)
        room_stats = get_station_room_type_stats(
            session, station_id, datetime.utcnow().date()
        )

    st.subheader(f"{station_label} - 예약률 추이")
    ts_df = build_booking_rate_timeseries(timeseries)
    if not ts_df.empty:
        fig = px.line(ts_df, x="date", y="booking_rate")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("데이터 없음")

    st.subheader("숙소 유형별 비교")
    room_df = build_room_type_bar_data(room_stats)
    if not room_df.empty:
        fig2 = px.bar(room_df, x="room_type", y="booking_rate", color="room_type")
        st.plotly_chart(fig2, use_container_width=True)

    st.subheader(f"주변 숙소 ({len(listings)}개)")
    if listings:
        m = folium.Map(
            location=[listings[0]["latitude"] or 37.5665,
                      listings[0]["longitude"] or 126.9780],
            zoom_start=15,
        )
        for lst in listings:
            if lst["latitude"] and lst["longitude"]:
                folium.Marker(
                    [lst["latitude"], lst["longitude"]],
                    popup=f"{lst['name']} ({lst['room_type']})",
                ).add_to(m)
        st_folium(m, width=900, height=400)
