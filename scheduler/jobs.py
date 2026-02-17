"""
스케줄러 작업 정의

APScheduler를 사용하여 크롤링 작업을 자동 실행합니다.
- 검색 스냅샷: 매 시간 (대상 역만)
- 캘린더 크롤링: 매일 새벽 (전체 숙소)
- 숙소 상세 갱신: 매주 1회 (옵션 B/C만)
"""

import asyncio
import json
import logging
from datetime import datetime

from config.settings import get_tier_config, BASE_DIR
from crawler.airbnb_client import AirbnbClient
from crawler.calendar_crawler import CalendarCrawler
from crawler.listing_crawler import ListingCrawler
from crawler.search_crawler import SearchCrawler
from models.database import init_db, session_scope
from models.schema import Listing, Station

logger = logging.getLogger(__name__)


def load_stations_from_json():
    """stations.json에서 역 데이터를 로드하여 DB에 저장합니다."""
    stations_file = BASE_DIR / "config" / "stations.json"
    with open(stations_file) as f:
        data = json.load(f)

    tier = get_tier_config()
    allowed_priorities = tier["station_priority"]

    with session_scope() as session:
        count = 0
        for s in data["stations"]:
            if s["priority"] not in allowed_priorities:
                continue

            existing = session.query(Station).filter_by(
                name=s["name"], line=s["line"]
            ).first()

            if not existing:
                station = Station(
                    name=s["name"],
                    line=s["line"],
                    district=s.get("district"),
                    latitude=s["lat"],
                    longitude=s["lng"],
                    priority=s["priority"],
                )
                session.add(station)
                count += 1

        logger.info("Loaded %d new stations (priority filter: %s)", count, allowed_priorities)


def get_target_stations() -> list[Station]:
    """현재 티어에 해당하는 역 목록을 반환합니다."""
    tier = get_tier_config()
    allowed = tier["station_priority"]

    with session_scope() as session:
        stations = (
            session.query(Station)
            .filter(Station.priority.in_(allowed))
            .order_by(Station.priority, Station.id)
            .all()
        )
        # detach from session
        session.expunge_all()
    return stations


def get_all_listings() -> list[Listing]:
    """DB에 저장된 모든 숙소를 반환합니다."""
    with session_scope() as session:
        listings = session.query(Listing).order_by(Listing.id).all()
        session.expunge_all()
    return listings


async def run_search_job():
    """검색 크롤링 작업 (매 시간 실행)."""
    logger.info("=== Search job started at %s ===", datetime.now().isoformat())

    client = AirbnbClient()
    crawler = SearchCrawler(client)

    try:
        stations = get_target_stations()
        if not stations:
            logger.warning("No target stations found. Run load_stations_from_json() first.")
            return

        results = await crawler.crawl_all_stations(stations)
        logger.info("Search job completed: %d stations crawled", len(results))

        # 통계 로깅
        stats = client.get_stats()
        logger.info("Rate limiter stats: %s", stats["rate_limiter"])
        if stats["proxy_manager"]["total"] > 0:
            logger.info("Proxy stats: %s", stats["proxy_manager"])

    finally:
        await client.close()


async def run_calendar_job():
    """캘린더 크롤링 작업 (매일 새벽 실행)."""
    tier = get_tier_config()
    if not tier["calendar_enabled"]:
        logger.info("Calendar crawling disabled for current tier")
        return

    logger.info("=== Calendar job started at %s ===", datetime.now().isoformat())

    client = AirbnbClient()
    crawler = CalendarCrawler(client)

    try:
        listings = get_all_listings()
        if not listings:
            logger.warning("No listings found in DB. Run search job first.")
            return

        summary = await crawler.crawl_all_listings(listings)
        logger.info("Calendar job completed: %s", summary)
    finally:
        await client.close()


async def run_listing_detail_job():
    """숙소 상세 크롤링 작업 (매주 1회, 옵션 B/C만)."""
    tier = get_tier_config()
    if not tier["listing_detail_enabled"]:
        logger.info("Listing detail crawling disabled for current tier")
        return

    logger.info("=== Listing detail job started at %s ===", datetime.now().isoformat())

    client = AirbnbClient()
    crawler = ListingCrawler(client)

    try:
        listings = get_all_listings()
        if not listings:
            logger.warning("No listings found in DB.")
            return

        summary = await crawler.crawl_all_listings(listings)
        logger.info("Listing detail job completed: %s", summary)
    finally:
        await client.close()


def setup_scheduler():
    """APScheduler를 설정합니다."""
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.triggers.cron import CronTrigger
    from apscheduler.triggers.interval import IntervalTrigger

    tier = get_tier_config()
    scheduler = AsyncIOScheduler()

    # 검색 스냅샷: 매 시간
    scheduler.add_job(
        run_search_job,
        IntervalTrigger(minutes=tier["search_interval_minutes"]),
        id="search_job",
        name="Search Snapshot Crawler",
        max_instances=1,
    )

    # 캘린더: 매일 새벽
    if tier["calendar_enabled"]:
        scheduler.add_job(
            run_calendar_job,
            CronTrigger(hour=tier["calendar_hour"], minute=0),
            id="calendar_job",
            name="Calendar Crawler",
            max_instances=1,
        )

    # 숙소 상세: 매주 월요일 새벽 5시
    if tier["listing_detail_enabled"]:
        scheduler.add_job(
            run_listing_detail_job,
            CronTrigger(day_of_week="mon", hour=5, minute=0),
            id="listing_detail_job",
            name="Listing Detail Crawler",
            max_instances=1,
        )

    logger.info(
        "Scheduler configured (tier=%s): search=%dmin, calendar=%s, detail=%s",
        tier,
        tier["search_interval_minutes"],
        f"daily@{tier['calendar_hour']}:00" if tier["calendar_enabled"] else "disabled",
        "weekly" if tier["listing_detail_enabled"] else "disabled",
    )

    return scheduler
