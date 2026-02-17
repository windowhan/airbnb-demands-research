"""일별 집계 파이프라인

daily_stats 테이블에 역별 × 숙소유형별 예약률, 수익률을 집계하여 저장합니다.

스케줄러에서 매일 새벽 실행:
  run_aggregation(days_back=1)  → 어제 데이터 집계
"""

import logging
from datetime import date, datetime, timedelta
from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from models.database import session_scope
from models.schema import CalendarSnapshot, DailyStat, Listing, Station

logger = logging.getLogger(__name__)

# 집계할 숙소 유형 (None = 전체)
ROOM_TYPES: list[Optional[str]] = [
    "entire_home",
    "private_room",
    "shared_room",
    "hotel",
    None,
]


def _get_listing_ids(
    session: Session,
    station_id: int,
    room_type: Optional[str],
) -> list[int]:
    """역 주변 숙소 ID 목록을 반환합니다."""
    query = session.query(Listing.id).filter(
        Listing.nearest_station_id == station_id
    )
    if room_type:
        query = query.filter(Listing.room_type == room_type)
    return [row[0] for row in query.all()]


def _get_date_stats(
    session: Session,
    listing_ids: list[int],
    target_date: date,
) -> tuple[int, float, float]:
    """날짜별 (예약 수, 평균 가격, 총 수익)을 반환합니다.

    각 숙소의 해당 날짜 최신 스냅샷을 기준으로 집계합니다.

    Returns:
        (booked_count, avg_price, total_revenue)
    """
    if not listing_ids:
        return 0, 0.0, 0.0

    # 각 listing의 해당 날짜 최신 crawled_at 서브쿼리
    subq = (
        session.query(
            CalendarSnapshot.listing_id,
            func.max(CalendarSnapshot.crawled_at).label("latest_at"),
        )
        .filter(
            CalendarSnapshot.listing_id.in_(listing_ids),
            CalendarSnapshot.date == target_date,
        )
        .group_by(CalendarSnapshot.listing_id)
        .subquery()
    )

    snaps = (
        session.query(CalendarSnapshot)
        .join(
            subq,
            (CalendarSnapshot.listing_id == subq.c.listing_id)
            & (CalendarSnapshot.crawled_at == subq.c.latest_at),
        )
        .filter(CalendarSnapshot.date == target_date)
        .all()
    )

    booked = [s for s in snaps if s.available is False]
    booked_count = len(booked)
    prices = [s.price for s in booked if s.price]
    total_revenue = sum(prices)
    avg_price = total_revenue / len(prices) if prices else 0.0

    return booked_count, avg_price, total_revenue


def _upsert_daily_stat(
    session: Session,
    station_id: int,
    target_date: date,
    room_type: Optional[str],
    total_listings: int,
    booked_count: int,
    booking_rate: float,
    avg_daily_price: float,
    estimated_revenue: float,
) -> DailyStat:
    """DailyStat을 upsert (있으면 갱신, 없으면 삽입)합니다."""
    existing = (
        session.query(DailyStat)
        .filter_by(
            station_id=station_id,
            date=target_date,
            room_type=room_type,
        )
        .first()
    )

    if existing:
        existing.total_listings = total_listings
        existing.booked_count = booked_count
        existing.booking_rate = booking_rate
        existing.avg_daily_price = avg_daily_price
        existing.estimated_revenue = estimated_revenue
        return existing

    stat = DailyStat(
        station_id=station_id,
        date=target_date,
        room_type=room_type,
        total_listings=total_listings,
        booked_count=booked_count,
        booking_rate=booking_rate,
        avg_daily_price=avg_daily_price,
        estimated_revenue=estimated_revenue,
    )
    session.add(stat)
    return stat


def aggregate_station_date(
    session: Session,
    station_id: int,
    target_date: date,
) -> dict:
    """한 역의 특정 날짜 통계를 집계하여 DailyStat에 저장합니다.

    room_type별(entire_home, private_room, shared_room, hotel) + 전체(None)로 집계.

    Returns:
        {"station_id": int, "date": date, "rows_written": int}
    """
    rows_written = 0

    for rt in ROOM_TYPES:
        listing_ids = _get_listing_ids(session, station_id, rt)
        total = len(listing_ids)
        if total == 0:
            continue

        booked_count, avg_price, total_revenue = _get_date_stats(
            session, listing_ids, target_date
        )
        booking_rate = booked_count / total

        _upsert_daily_stat(
            session=session,
            station_id=station_id,
            target_date=target_date,
            room_type=rt,
            total_listings=total,
            booked_count=booked_count,
            booking_rate=booking_rate,
            avg_daily_price=avg_price,
            estimated_revenue=total_revenue,
        )
        rows_written += 1

    logger.info(
        "Station %d on %s: %d rows aggregated", station_id, target_date, rows_written
    )
    return {
        "station_id": station_id,
        "date": target_date,
        "rows_written": rows_written,
    }


def aggregate_daily_stats(target_date: Optional[date] = None) -> dict:
    """모든 역의 daily_stats를 집계합니다.

    Args:
        target_date: 집계할 날짜. None이면 어제.

    Returns:
        {
            "date": date,
            "stations_processed": int,
            "total_rows": int,
        }
    """
    if target_date is None:
        target_date = datetime.utcnow().date() - timedelta(days=1)

    logger.info("Starting daily aggregation for %s", target_date)
    total_rows = 0
    stations_processed = 0

    with session_scope() as session:
        station_ids = [row[0] for row in session.query(Station.id).all()]
        for station_id in station_ids:
            result = aggregate_station_date(session, station_id, target_date)
            total_rows += result["rows_written"]
            stations_processed += 1

    logger.info(
        "Aggregation complete for %s: %d stations, %d rows",
        target_date,
        stations_processed,
        total_rows,
    )
    return {
        "date": target_date,
        "stations_processed": stations_processed,
        "total_rows": total_rows,
    }


def run_aggregation(days_back: int = 1) -> None:
    """최근 days_back일치의 집계를 실행합니다.

    Args:
        days_back: 어제부터 며칠 전까지 집계할지 (기본 1 = 어제만)
    """
    today = datetime.utcnow().date()
    for i in range(1, days_back + 1):
        target = today - timedelta(days=i)
        logger.info("Running aggregation for %s (%d/%d)", target, i, days_back)
        aggregate_daily_stats(target)
    logger.info("run_aggregation complete: processed %d days", days_back)
