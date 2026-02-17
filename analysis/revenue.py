"""수익률 추정 로직

일일 추정 수익 = 예약된 날짜(available=False)의 가격
월간 추정 수익 = 해당 월의 일일 추정 수익 합계
역별 추정 수익 = 역 주변 숙소들의 일일 수익 합계
"""

import calendar
import logging
from datetime import date
from typing import Optional

from sqlalchemy.orm import Session

from models.database import session_scope
from models.schema import CalendarSnapshot, Listing

logger = logging.getLogger(__name__)


def _get_latest_snapshot(
    session: Session,
    listing_id: int,
    target_date: date,
) -> Optional[CalendarSnapshot]:
    """특정 날짜의 최신 CalendarSnapshot을 반환합니다."""
    return (
        session.query(CalendarSnapshot)
        .filter(
            CalendarSnapshot.listing_id == listing_id,
            CalendarSnapshot.date == target_date,
        )
        .order_by(CalendarSnapshot.crawled_at.desc())
        .first()
    )


def estimate_listing_daily_revenue(
    session: Session,
    listing_id: int,
    target_date: date,
) -> float:
    """특정 날짜의 숙소 추정 수익을 반환합니다.

    예약(available=False)이고 가격 정보가 있으면 해당 가격,
    그렇지 않으면 0.0.

    Args:
        session: DB 세션
        listing_id: 숙소 ID
        target_date: 날짜

    Returns:
        추정 수익 (KRW). 예약 아님 / 데이터 없음이면 0.0.
    """
    snap = _get_latest_snapshot(session, listing_id, target_date)
    if snap is None:
        return 0.0
    if snap.available is False and snap.price:
        return snap.price
    return 0.0


def estimate_listing_monthly_revenue(
    listing_id: int,
    year: int,
    month: int,
) -> float:
    """숙소의 특정 월 추정 수익을 반환합니다.

    Args:
        listing_id: 숙소 ID
        year: 연도
        month: 월 (1~12)

    Returns:
        해당 월 추정 총 수익 (KRW).
    """
    _, days_in_month = calendar.monthrange(year, month)
    total = 0.0
    with session_scope() as session:
        for day in range(1, days_in_month + 1):
            d = date(year, month, day)
            total += estimate_listing_daily_revenue(session, listing_id, d)
    return total


def estimate_station_revenue(
    station_id: int,
    target_date: date,
    room_type: Optional[str] = None,
) -> dict:
    """역 주변 숙소들의 특정 날짜 추정 수익을 집계합니다.

    Args:
        station_id: 역 ID
        target_date: 날짜
        room_type: 숙소 유형 필터 (None이면 전체)

    Returns:
        {
            "station_id": int,
            "date": date,
            "room_type": str | None,
            "total_listings": int,
            "booked_count": int,
            "total_revenue": float,
            "avg_revenue": float,   # 예약된 숙소당 평균 수익
        }
    """
    with session_scope() as session:
        query = session.query(Listing).filter(
            Listing.nearest_station_id == station_id
        )
        if room_type:
            query = query.filter(Listing.room_type == room_type)
        listing_ids = [lst.id for lst in query.all()]

        if not listing_ids:
            logger.info(
                "No listings for station_id=%d room_type=%s", station_id, room_type
            )
            return {
                "station_id": station_id,
                "date": target_date,
                "room_type": room_type,
                "total_listings": 0,
                "booked_count": 0,
                "total_revenue": 0.0,
                "avg_revenue": 0.0,
            }

        revenues = [
            estimate_listing_daily_revenue(session, lid, target_date)
            for lid in listing_ids
        ]

    total_revenue = sum(revenues)
    booked_count = sum(1 for r in revenues if r > 0)
    avg_revenue = total_revenue / booked_count if booked_count > 0 else 0.0

    logger.info(
        "Station %d [%s] on %s: booked=%d/%d, revenue=%.0f",
        station_id,
        room_type,
        target_date,
        booked_count,
        len(listing_ids),
        total_revenue,
    )

    return {
        "station_id": station_id,
        "date": target_date,
        "room_type": room_type,
        "total_listings": len(listing_ids),
        "booked_count": booked_count,
        "total_revenue": total_revenue,
        "avg_revenue": avg_revenue,
    }
