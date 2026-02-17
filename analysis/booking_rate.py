"""예약률 계산 로직

예약률 = 예약불가(available=False) 날짜 수 / 전체 날짜 수

실제 예약 감지:
  이전 스냅샷에서 available=True → 최신 스냅샷에서 False = 실제 예약
  처음부터 False = 호스트 차단 가능성
"""

import logging
from datetime import date, timedelta
from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from models.database import session_scope
from models.schema import CalendarSnapshot, Listing

logger = logging.getLogger(__name__)


def get_latest_snapshots(
    session: Session,
    listing_id: int,
    start_date: date,
    end_date: date,
) -> list[CalendarSnapshot]:
    """기간 내 날짜별 최신 CalendarSnapshot 목록을 반환합니다.

    같은 날짜에 여러 스냅샷이 있으면 가장 최근 것만 반환합니다.
    """
    subq = (
        session.query(
            CalendarSnapshot.date,
            func.max(CalendarSnapshot.crawled_at).label("latest_at"),
        )
        .filter(
            CalendarSnapshot.listing_id == listing_id,
            CalendarSnapshot.date >= start_date,
            CalendarSnapshot.date <= end_date,
        )
        .group_by(CalendarSnapshot.date)
        .subquery()
    )

    return (
        session.query(CalendarSnapshot)
        .join(
            subq,
            (CalendarSnapshot.date == subq.c.date)
            & (CalendarSnapshot.crawled_at == subq.c.latest_at)
            & (CalendarSnapshot.listing_id == listing_id),
        )
        .all()
    )


def calculate_booking_rate(
    listing_id: int,
    start_date: date,
    end_date: date,
) -> float:
    """숙소의 지정 기간 예약률을 계산합니다.

    예약률 = available=False인 날짜 수 / 전체 스냅샷 날짜 수

    Returns:
        0.0 ~ 1.0 사이의 예약률. 데이터 없으면 0.0.
    """
    with session_scope() as session:
        snapshots = get_latest_snapshots(session, listing_id, start_date, end_date)
        if not snapshots:
            return 0.0
        booked_days = sum(1 for s in snapshots if s.available is False)
        return booked_days / len(snapshots)


def is_actually_booked(
    session: Session,
    listing_id: int,
    target_date: date,
) -> bool:
    """시계열 추적으로 실제 예약 여부를 판단합니다.

    이전 스냅샷에서 available=True였다가 최신에서 False로 바뀐 경우 = 실제 예약.
    처음부터 False = 호스트 차단 가능성.

    Args:
        session: DB 세션
        listing_id: 숙소 ID
        target_date: 판단할 날짜

    Returns:
        True이면 실제 예약, False이면 미예약 또는 확인 불가.
    """
    snapshots = (
        session.query(CalendarSnapshot)
        .filter(
            CalendarSnapshot.listing_id == listing_id,
            CalendarSnapshot.date == target_date,
        )
        .order_by(CalendarSnapshot.crawled_at)
        .all()
    )

    if len(snapshots) < 2:
        # 스냅샷이 1개 이하면 변화 추적 불가
        return False

    latest = snapshots[-1]
    if latest.available is not False:
        # 현재 예약 가능 → 예약 아님
        return False

    # 최신이 불가인데, 이전에 가능한 적이 있었는지
    return any(s.available is True for s in snapshots[:-1])


def get_station_booking_rate(
    station_id: int,
    target_date: date,
    room_type: Optional[str] = None,
    window_days: int = 30,
) -> dict:
    """역 주변 숙소들의 예약률을 계산합니다.

    Args:
        station_id: 역 ID
        target_date: 기준 날짜
        room_type: 숙소 유형 필터 (None이면 전체)
        window_days: 예약률 계산 기간 (일)

    Returns:
        {
            "station_id": int,
            "date": date,
            "room_type": str | None,
            "total_listings": int,
            "booking_rate": float,   # 0~1
            "booked_count": int,
        }
    """
    end_date = target_date
    start_date = target_date - timedelta(days=window_days - 1)

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
            "booking_rate": 0.0,
            "booked_count": 0,
        }

    rates = [
        calculate_booking_rate(lid, start_date, end_date) for lid in listing_ids
    ]
    avg_rate = sum(rates) / len(rates)
    booked_count = sum(1 for r in rates if r > 0)

    logger.info(
        "Station %d [%s]: %d listings, booking_rate=%.2f",
        station_id,
        room_type,
        len(listing_ids),
        avg_rate,
    )

    return {
        "station_id": station_id,
        "date": target_date,
        "room_type": room_type,
        "total_listings": len(listing_ids),
        "booking_rate": avg_rate,
        "booked_count": booked_count,
    }
