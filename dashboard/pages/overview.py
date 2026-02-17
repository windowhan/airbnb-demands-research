"""전체 현황 대시보드 페이지.

데이터 fetch 함수 (testable) + Streamlit 렌더링 (# pragma: no cover).
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Optional

from sqlalchemy.orm import Session

from models.database import session_scope
from models.schema import CrawlLog, DailyStat, Listing, Station

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 데이터 fetch 함수 (비즈니스 로직 - 테스트 대상)
# ---------------------------------------------------------------------------


def get_summary_metrics(session: Session, target_date: date) -> dict:
    """오늘의 주요 지표를 반환합니다.

    Returns:
        {
            "total_listings": int,
            "total_stations": int,
            "avg_booking_rate": float,
            "avg_daily_price": float,
            "total_estimated_revenue": float,
        }
    """
    total_listings = session.query(Listing).count()
    total_stations = session.query(Station).count()

    stats = (
        session.query(DailyStat)
        .filter(
            DailyStat.date == target_date,
            DailyStat.room_type.is_(None),
        )
        .all()
    )

    if not stats:
        return {
            "total_listings": total_listings,
            "total_stations": total_stations,
            "avg_booking_rate": 0.0,
            "avg_daily_price": 0.0,
            "total_estimated_revenue": 0.0,
        }

    avg_booking_rate = sum(s.booking_rate for s in stats) / len(stats)
    prices = [s.avg_daily_price for s in stats if s.avg_daily_price]
    avg_daily_price = sum(prices) / len(prices) if prices else 0.0
    total_revenue = sum(s.estimated_revenue for s in stats)

    return {
        "total_listings": total_listings,
        "total_stations": total_stations,
        "avg_booking_rate": avg_booking_rate,
        "avg_daily_price": avg_daily_price,
        "total_estimated_revenue": total_revenue,
    }


def get_station_map_stats(session: Session, target_date: date) -> list[dict]:
    """지도 표시용 역별 집계 데이터를 반환합니다.

    Returns:
        [{"station_id", "name", "latitude", "longitude",
          "booking_rate", "estimated_revenue", "total_listings"}, ...]
    """
    stations = session.query(Station).all()
    stats = (
        session.query(DailyStat)
        .filter(
            DailyStat.date == target_date,
            DailyStat.room_type.is_(None),
        )
        .all()
    )

    stat_map = {s.station_id: s for s in stats}
    result = []
    for stn in stations:
        stat = stat_map.get(stn.id)
        result.append(
            {
                "station_id": stn.id,
                "name": stn.name,
                "latitude": stn.latitude,
                "longitude": stn.longitude,
                "booking_rate": stat.booking_rate if stat else 0.0,
                "estimated_revenue": stat.estimated_revenue if stat else 0.0,
                "total_listings": stat.total_listings if stat else 0,
            }
        )
    return result


def get_recent_crawl_log(session: Session) -> Optional[dict]:
    """가장 최근 크롤 로그를 반환합니다.

    Returns:
        {"job_type", "started_at", "status", "total_requests",
         "successful_requests", "blocked_requests"} or None
    """
    log = (
        session.query(CrawlLog)
        .order_by(CrawlLog.started_at.desc())
        .first()
    )
    if log is None:
        return None
    return {
        "job_type": log.job_type,
        "started_at": log.started_at,
        "status": log.status,
        "total_requests": log.total_requests,
        "successful_requests": log.successful_requests,
        "blocked_requests": log.blocked_requests,
    }


def get_booking_rate_trend(
    session: Session,
    days: int = 14,
    room_type: Optional[str] = None,
) -> list[dict]:
    """최근 N일간 전체 역 평균 예약률 추이를 반환합니다.

    Returns:
        [{"date": date, "booking_rate": float}, ...]
    """
    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=days - 1)

    query = session.query(DailyStat).filter(
        DailyStat.date >= start_date,
        DailyStat.date <= end_date,
    )
    if room_type is None:
        query = query.filter(DailyStat.room_type.is_(None))
    else:
        query = query.filter(DailyStat.room_type == room_type)

    stats = query.all()

    # 날짜별 그룹핑
    by_date: dict[date, list[float]] = {}
    for s in stats:
        by_date.setdefault(s.date, []).append(s.booking_rate)

    result = []
    d = start_date
    while d <= end_date:
        rates = by_date.get(d, [])
        result.append(
            {
                "date": d,
                "booking_rate": sum(rates) / len(rates) if rates else 0.0,
            }
        )
        d += timedelta(days=1)
    return result


# ---------------------------------------------------------------------------
# Streamlit UI 렌더링 (# pragma: no cover)
# ---------------------------------------------------------------------------


def render_overview():  # pragma: no cover
    """전체 현황 페이지를 렌더링합니다."""
    import streamlit as st
    import plotly.express as px
    import folium
    from streamlit_folium import st_folium

    from dashboard.components.charts import (
        build_booking_rate_timeseries,
        build_station_summary,
        build_top_stations,
        format_korean_number,
    )

    st.title("서울 Airbnb 수요 현황")

    target_date = st.date_input("기준 날짜", value=datetime.utcnow().date())

    with session_scope() as session:
        metrics = get_summary_metrics(session, target_date)
        map_stats = get_station_map_stats(session, target_date)
        trend = get_booking_rate_trend(session, days=14)
        crawl_log = get_recent_crawl_log(session)

    col1, col2, col3 = st.columns(3)
    col1.metric("총 숙소 수", f"{metrics['total_listings']:,}")
    col2.metric("평균 예약률", f"{metrics['avg_booking_rate']:.1%}")
    col3.metric("총 추정 수익", format_korean_number(metrics["total_estimated_revenue"]))

    st.subheader("역별 예약률 지도")
    m = folium.Map(location=[37.5665, 126.9780], zoom_start=12)
    for row in map_stats:
        folium.CircleMarker(
            location=[row["latitude"], row["longitude"]],
            radius=max(5, row["booking_rate"] * 20),
            color="red",
            fill=True,
            popup=f"{row['name']}: {row['booking_rate']:.1%}",
        ).add_to(m)
    st_folium(m, width=900, height=500)

    st.subheader("최근 14일 평균 예약률 추이")
    trend_df = build_booking_rate_timeseries(trend)
    if not trend_df.empty:
        fig = px.line(trend_df, x="date", y="booking_rate", labels={"booking_rate": "예약률"})
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("크롤링 상태")
    if crawl_log:
        st.write(
            f"최근 작업: **{crawl_log['job_type']}** "
            f"({crawl_log['status']}) @ {crawl_log['started_at']}"
        )
    else:
        st.info("크롤링 기록 없음")
