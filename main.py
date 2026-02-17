"""
Airbnb 서울 지역별 수요 조사 크롤러 - 진입점

사용법:
  # 스케줄러 모드 (기본): 설정된 주기로 자동 크롤링
  python main.py

  # 단일 실행 모드: 한 번만 크롤링 후 종료
  python main.py --once search      # 검색만
  python main.py --once calendar    # 캘린더만
  python main.py --once all         # 전부

  # 역 데이터 초기 로드
  python main.py --init

  # 티어 변경 (환경변수)
  CRAWL_TIER=B python main.py

  # 상태 조회
  python main.py --status
"""

import argparse
import asyncio
import logging
import signal
import sys

from config.settings import CRAWL_TIER, LOG_DIR, LOG_FORMAT, LOG_LEVEL, get_tier_config
from models.database import init_db, session_scope
from models.schema import CrawlLog, Listing, SearchSnapshot, Station
from scheduler.jobs import (
    load_stations_from_json,
    run_calendar_job,
    run_listing_detail_job,
    run_search_job,
    setup_scheduler,
)


def setup_logging():
    """로깅을 설정합니다."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL),
        format=LOG_FORMAT,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(LOG_DIR / "crawler.log", encoding="utf-8"),
        ],
    )

    # 외부 라이브러리 로그 억제
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("apscheduler").setLevel(logging.INFO)


def show_status():
    """현재 DB 상태를 출력합니다."""
    tier = get_tier_config()
    print(f"\n{'='*50}")
    print(f"  Airbnb Seoul Demand Crawler - Status")
    print(f"{'='*50}")
    print(f"  Tier: {CRAWL_TIER}")
    print(f"  Station priorities: {tier['station_priority']}")
    print(f"  Proxy required: {tier['proxy_required']}")
    print(f"  Max requests/hour: {tier['max_requests_per_hour']}")
    print()

    with session_scope() as session:
        station_count = session.query(Station).count()
        listing_count = session.query(Listing).count()
        snapshot_count = session.query(SearchSnapshot).count()

        last_crawl = (
            session.query(CrawlLog)
            .order_by(CrawlLog.started_at.desc())
            .first()
        )

        print(f"  Stations in DB: {station_count}")
        print(f"  Listings discovered: {listing_count}")
        print(f"  Search snapshots: {snapshot_count}")

        if last_crawl:
            print(f"\n  Last crawl:")
            print(f"    Type: {last_crawl.job_type}")
            print(f"    Time: {last_crawl.started_at}")
            print(f"    Status: {last_crawl.status}")
            print(f"    Success/Total: {last_crawl.successful_requests}/{last_crawl.total_requests}")
            if last_crawl.blocked_requests:
                print(f"    Blocked: {last_crawl.blocked_requests}")

    print(f"{'='*50}\n")


async def run_once(mode: str):
    """단일 실행 모드."""
    logger = logging.getLogger(__name__)

    if mode in ("search", "all"):
        logger.info("Running search crawl (one-time)...")
        await run_search_job()

    if mode in ("calendar", "all"):
        logger.info("Running calendar crawl (one-time)...")
        await run_calendar_job()

    if mode in ("detail", "all"):
        logger.info("Running listing detail crawl (one-time)...")
        await run_listing_detail_job()


async def run_scheduler():
    """스케줄러 모드로 실행합니다."""
    logger = logging.getLogger(__name__)
    scheduler = setup_scheduler()

    # 시작 시 즉시 검색 크롤 1회 실행
    logger.info("Running initial search crawl...")
    await run_search_job()

    # 스케줄러 시작
    scheduler.start()
    logger.info("Scheduler started. Press Ctrl+C to stop.")

    # SIGINT/SIGTERM 처리
    stop_event = asyncio.Event()

    def _signal_handler(sig, frame):
        logger.info("Received signal %s, shutting down...", sig)
        stop_event.set()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    try:
        await stop_event.wait()
    finally:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped.")


def main():
    parser = argparse.ArgumentParser(
        description="Airbnb Seoul Demand Crawler",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--init", action="store_true",
        help="Initialize DB and load station data",
    )
    parser.add_argument(
        "--once", choices=["search", "calendar", "detail", "all"],
        help="Run a single crawl job and exit",
    )
    parser.add_argument(
        "--status", action="store_true",
        help="Show current crawler status",
    )

    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)

    # DB 초기화
    init_db()
    logger.info("Database initialized (tier=%s)", CRAWL_TIER)

    if args.init:
        load_stations_from_json()
        show_status()
        return

    if args.status:
        show_status()
        return

    if args.once:
        asyncio.run(run_once(args.once))
        show_status()
        return

    # 기본: 스케줄러 모드
    asyncio.run(run_scheduler())


if __name__ == "__main__":
    main()
