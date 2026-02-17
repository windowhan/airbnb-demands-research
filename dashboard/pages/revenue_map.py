"""수익률 지도 페이지.

데이터 fetch 함수 (testable) + Streamlit 렌더링 (# pragma: no cover).
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from models.database import session_scope
from models.schema import DailyStat, Station

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 데이터 fetch 함수 (비즈니스 로직 - 테스트 대상)
# ---------------------------------------------------------------------------


def get_revenue_ranking(
    session: Session,
    target_date: date,
    room_type: Optional[str] = None,
    n: int = 20,
) -> list[dict]:
    """특정 날짜 추정 수익 상위 N개 역을 반환합니다.

    Args:
        session: DB 세션
        target_date: 기준 날짜
        room_type: 숙소 유형 필터 (None = 전체)
        n: 상위 몇 개

    Returns:
        [{"rank", "station_id", "name", "latitude", "longitude",
          "estimated_revenue", "booking_rate", "total_listings"}, ...]
    """
    query = (
        session.query(DailyStat, Station)
        .join(Station, DailyStat.station_id == Station.id)
        .filter(DailyStat.date == target_date)
    )
    if room_type is None:
        query = query.filter(DailyStat.room_type.is_(None))
    else:
        query = query.filter(DailyStat.room_type == room_type)

    rows = query.order_by(DailyStat.estimated_revenue.desc()).limit(n).all()

    return [
        {
            "rank": i + 1,
            "station_id": stat.station_id,
            "name": stn.name,
            "latitude": stn.latitude,
            "longitude": stn.longitude,
            "estimated_revenue": stat.estimated_revenue,
            "booking_rate": stat.booking_rate,
            "total_listings": stat.total_listings,
        }
        for i, (stat, stn) in enumerate(rows)
    ]


def get_monthly_revenue_summary(
    session: Session,
    year: int,
    month: int,
    room_type: Optional[str] = None,
) -> list[dict]:
    """특정 월의 역별 총 추정 수익을 반환합니다.

    Returns:
        [{"station_id", "name", "latitude", "longitude",
          "total_revenue", "avg_booking_rate"}, ...]
    """
    import calendar

    _, days_in_month = calendar.monthrange(year, month)
    start_date = date(year, month, 1)
    end_date = date(year, month, days_in_month)

    query = (
        session.query(
            DailyStat.station_id,
            func.sum(DailyStat.estimated_revenue).label("total_revenue"),
            func.avg(DailyStat.booking_rate).label("avg_booking_rate"),
        )
        .filter(
            DailyStat.date >= start_date,
            DailyStat.date <= end_date,
        )
    )
    if room_type is None:
        query = query.filter(DailyStat.room_type.is_(None))
    else:
        query = query.filter(DailyStat.room_type == room_type)

    rows = query.group_by(DailyStat.station_id).all()

    station_map = {stn.id: stn for stn in session.query(Station).all()}
    result = []
    for row in rows:
        stn = station_map.get(row.station_id)
        if stn is None:
            continue
        result.append(
            {
                "station_id": row.station_id,
                "name": stn.name,
                "latitude": stn.latitude,
                "longitude": stn.longitude,
                "total_revenue": float(row.total_revenue or 0.0),
                "avg_booking_rate": float(row.avg_booking_rate or 0.0),
            }
        )
    result.sort(key=lambda x: x["total_revenue"], reverse=True)
    return result


def get_revenue_heatmap_data(
    session: Session,
    target_date: date,
    room_type: Optional[str] = None,
) -> list[tuple[float, float, float]]:
    """folium 히트맵용 (위도, 경도, 강도) 튜플 목록을 반환합니다.

    Returns:
        [(latitude, longitude, normalized_revenue), ...]
    """
    ranking = get_revenue_ranking(session, target_date, room_type=room_type, n=100)

    if not ranking:
        return []

    max_rev = max(r["estimated_revenue"] for r in ranking)
    if max_rev == 0:
        return [(r["latitude"], r["longitude"], 0.0) for r in ranking
                if r["latitude"] and r["longitude"]]

    return [
        (r["latitude"], r["longitude"], r["estimated_revenue"] / max_rev)
        for r in ranking
        if r["latitude"] and r["longitude"]
    ]


# ---------------------------------------------------------------------------
# Streamlit UI 렌더링 (# pragma: no cover)
# ---------------------------------------------------------------------------


def render_revenue_map():  # pragma: no cover
    """수익률 지도 페이지를 렌더링합니다."""
    import streamlit as st
    import plotly.express as px
    import folium
    from folium.plugins import HeatMap
    from streamlit_folium import st_folium

    from dashboard.components.charts import format_korean_number

    st.title("수익률 지도")

    col1, col2 = st.columns(2)
    with col1:
        target_date = st.date_input("기준 날짜", value=datetime.utcnow().date())
    with col2:
        room_type_opt = st.selectbox(
            "숙소 유형",
            ["전체", "entire_home", "private_room", "shared_room", "hotel"],
        )
        room_type = None if room_type_opt == "전체" else room_type_opt

    with session_scope() as session:
        ranking = get_revenue_ranking(session, target_date, room_type=room_type)
        heatmap_data = get_revenue_heatmap_data(session, target_date, room_type=room_type)

    st.subheader("역별 추정 수익 히트맵")
    m = folium.Map(location=[37.5665, 126.9780], zoom_start=12)
    if heatmap_data:
        HeatMap(heatmap_data).add_to(m)
    st_folium(m, width=900, height=500)

    st.subheader(f"수익 상위 {len(ranking)}개 역")
    if ranking:
        import pandas as pd

        df = pd.DataFrame(ranking)
        df["estimated_revenue"] = df["estimated_revenue"].apply(format_korean_number)
        df["booking_rate"] = df["booking_rate"].apply(lambda x: f"{x:.1%}")
        df = df.rename(columns={
            "rank": "순위", "name": "역명",
            "estimated_revenue": "추정 수익",
            "booking_rate": "예약률",
            "total_listings": "숙소 수",
        })
        st.dataframe(df[["순위", "역명", "추정 수익", "예약률", "숙소 수"]], hide_index=True)
    else:
        st.info("데이터 없음")
