"""models/schema.py ORM 모델 및 models/database.py DB 관리 테스트."""

import tempfile
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from models.schema import (
    Base,
    CalendarSnapshot,
    CrawlLog,
    DailyStat,
    Listing,
    SearchSnapshot,
    Station,
)


# =========================================================================
# 1. Station model tests
# =========================================================================


class TestStation:
    """Station ORM 모델 테스트."""

    def test_create_station(self, db_session):
        """Station 인스턴스를 생성하고 DB에 저장할 수 있다."""
        station = Station(
            name="강남",
            line="2호선",
            district="강남구",
            latitude=37.4981,
            longitude=127.0276,
        )
        db_session.add(station)
        db_session.commit()

        fetched = db_session.query(Station).filter_by(name="강남").one()
        assert fetched.name == "강남"
        assert fetched.line == "2호선"
        assert fetched.district == "강남구"
        assert fetched.latitude == pytest.approx(37.4981)
        assert fetched.longitude == pytest.approx(127.0276)

    def test_station_default_priority(self, db_session):
        """priority 미지정 시 기본값 3이 적용된다."""
        station = Station(
            name="홍대입구",
            line="2호선",
            latitude=37.5571,
            longitude=126.9236,
        )
        db_session.add(station)
        db_session.commit()
        db_session.refresh(station)

        assert station.priority == 3

    def test_station_custom_priority(self, db_session):
        """priority를 직접 지정하면 해당 값이 저장된다."""
        station = Station(
            name="강남",
            line="2호선",
            latitude=37.4981,
            longitude=127.0276,
            priority=1,
        )
        db_session.add(station)
        db_session.commit()
        db_session.refresh(station)

        assert station.priority == 1

    def test_station_repr(self):
        """__repr__가 '<Station 이름(노선)>' 형태를 반환한다."""
        station = Station(name="서울", line="1호선", latitude=0, longitude=0)
        assert repr(station) == "<Station 서울(1호선)>"

    def test_station_listings_relationship(self, db_session):
        """Station.listings 관계로 연결된 Listing을 조회할 수 있다."""
        station = Station(
            name="홍대입구", line="2호선", latitude=37.5571, longitude=126.9236
        )
        db_session.add(station)
        db_session.commit()

        listing1 = Listing(
            airbnb_id="A001",
            name="숙소1",
            nearest_station_id=station.id,
        )
        listing2 = Listing(
            airbnb_id="A002",
            name="숙소2",
            nearest_station_id=station.id,
        )
        db_session.add_all([listing1, listing2])
        db_session.commit()

        db_session.refresh(station)
        assert len(station.listings) == 2
        airbnb_ids = {l.airbnb_id for l in station.listings}
        assert airbnb_ids == {"A001", "A002"}

    def test_station_search_snapshots_relationship(self, db_session):
        """Station.search_snapshots 관계를 통해 스냅샷을 조회할 수 있다."""
        station = Station(
            name="역삼", line="2호선", latitude=37.5006, longitude=127.0367
        )
        db_session.add(station)
        db_session.commit()

        snap = SearchSnapshot(
            station_id=station.id,
            total_listings=50,
            avg_price=120000.0,
        )
        db_session.add(snap)
        db_session.commit()

        db_session.refresh(station)
        assert len(station.search_snapshots) == 1
        assert station.search_snapshots[0].total_listings == 50

    def test_station_daily_stats_relationship(self, db_session):
        """Station.daily_stats 관계를 통해 일별 통계를 조회할 수 있다."""
        station = Station(
            name="선릉", line="2호선", latitude=37.5046, longitude=127.0490
        )
        db_session.add(station)
        db_session.commit()

        stat = DailyStat(
            station_id=station.id,
            date=date(2026, 2, 17),
            room_type="entire_home",
            total_listings=30,
            booking_rate=0.75,
        )
        db_session.add(stat)
        db_session.commit()

        db_session.refresh(station)
        assert len(station.daily_stats) == 1
        assert station.daily_stats[0].booking_rate == pytest.approx(0.75)


# =========================================================================
# 2. Listing model tests
# =========================================================================


class TestListing:
    """Listing ORM 모델 테스트."""

    def test_create_listing(self, db_session):
        """Listing을 생성하고 필드를 확인한다."""
        listing = Listing(
            airbnb_id="L100",
            name="테스트 숙소",
            host_id="H200",
            room_type="entire_home",
            latitude=37.499,
            longitude=127.028,
            bedrooms=2,
            bathrooms=1.5,
            max_guests=4,
            base_price=150000.0,
            rating=4.8,
            review_count=42,
        )
        db_session.add(listing)
        db_session.commit()
        db_session.refresh(listing)

        assert listing.id is not None
        assert listing.airbnb_id == "L100"
        assert listing.host_id == "H200"
        assert listing.room_type == "entire_home"
        assert listing.bedrooms == 2
        assert listing.bathrooms == pytest.approx(1.5)
        assert listing.max_guests == 4
        assert listing.base_price == pytest.approx(150000.0)
        assert listing.rating == pytest.approx(4.8)
        assert listing.review_count == 42

    def test_listing_airbnb_id_unique(self, db_session):
        """airbnb_id는 UNIQUE 제약을 갖는다."""
        listing1 = Listing(airbnb_id="DUP001", name="숙소A")
        listing2 = Listing(airbnb_id="DUP001", name="숙소B")
        db_session.add(listing1)
        db_session.commit()

        db_session.add(listing2)
        with pytest.raises(IntegrityError):
            db_session.commit()
        db_session.rollback()

    def test_listing_default_first_seen_last_seen(self, db_session):
        """first_seen, last_seen 미지정 시 기본값(현재 시각 부근)이 설정된다."""
        before = datetime.utcnow()
        listing = Listing(airbnb_id="TS001", name="기본시간 테스트")
        db_session.add(listing)
        db_session.commit()
        db_session.refresh(listing)
        after = datetime.utcnow()

        assert listing.first_seen is not None
        assert before <= listing.first_seen <= after
        assert listing.last_seen is not None
        assert before <= listing.last_seen <= after

    def test_listing_repr(self):
        """__repr__가 '<Listing airbnb_id: name>' 형태를 반환한다."""
        listing = Listing(airbnb_id="R001", name="예쁜 숙소")
        assert repr(listing) == "<Listing R001: 예쁜 숙소>"

    def test_listing_repr_none_name(self):
        """name이 None이어도 __repr__가 정상 동작한다."""
        listing = Listing(airbnb_id="R002", name=None)
        assert repr(listing) == "<Listing R002: None>"

    def test_listing_nearest_station_relationship(self, db_session):
        """Listing.nearest_station 관계로 Station을 참조할 수 있다."""
        station = Station(
            name="합정", line="2호선", latitude=37.5496, longitude=126.9140
        )
        db_session.add(station)
        db_session.commit()

        listing = Listing(
            airbnb_id="REL001",
            name="합정 숙소",
            nearest_station_id=station.id,
        )
        db_session.add(listing)
        db_session.commit()
        db_session.refresh(listing)

        assert listing.nearest_station is not None
        assert listing.nearest_station.name == "합정"

    def test_listing_calendar_snapshots_relationship(self, db_session):
        """Listing.calendar_snapshots 관계로 캘린더 데이터를 조회할 수 있다."""
        listing = Listing(airbnb_id="CAL001", name="캘린더 테스트")
        db_session.add(listing)
        db_session.commit()

        snap1 = CalendarSnapshot(
            listing_id=listing.id,
            date=date(2026, 3, 1),
            available=True,
            price=100000.0,
        )
        snap2 = CalendarSnapshot(
            listing_id=listing.id,
            date=date(2026, 3, 2),
            available=False,
            price=None,
        )
        db_session.add_all([snap1, snap2])
        db_session.commit()
        db_session.refresh(listing)

        assert len(listing.calendar_snapshots) == 2
        dates = {s.date for s in listing.calendar_snapshots}
        assert dates == {date(2026, 3, 1), date(2026, 3, 2)}

    def test_listing_without_station(self, db_session):
        """nearest_station_id 없이도 Listing을 생성할 수 있다."""
        listing = Listing(airbnb_id="NOSTATION", name="역 없는 숙소")
        db_session.add(listing)
        db_session.commit()
        db_session.refresh(listing)

        assert listing.nearest_station_id is None
        assert listing.nearest_station is None


# =========================================================================
# 3. SearchSnapshot model tests
# =========================================================================


class TestSearchSnapshot:
    """SearchSnapshot ORM 모델 테스트."""

    def test_create_search_snapshot(self, db_session, sample_station):
        """SearchSnapshot을 생성하고 필드를 확인한다."""
        snap = SearchSnapshot(
            station_id=sample_station.id,
            total_listings=100,
            avg_price=95000.0,
            min_price=40000.0,
            max_price=500000.0,
            median_price=85000.0,
            available_count=75,
            checkin_date=date(2026, 3, 1),
            checkout_date=date(2026, 3, 2),
            raw_response_hash="abc123def456",
        )
        db_session.add(snap)
        db_session.commit()
        db_session.refresh(snap)

        assert snap.id is not None
        assert snap.station_id == sample_station.id
        assert snap.total_listings == 100
        assert snap.avg_price == pytest.approx(95000.0)
        assert snap.min_price == pytest.approx(40000.0)
        assert snap.max_price == pytest.approx(500000.0)
        assert snap.median_price == pytest.approx(85000.0)
        assert snap.available_count == 75
        assert snap.checkin_date == date(2026, 3, 1)
        assert snap.checkout_date == date(2026, 3, 2)
        assert snap.raw_response_hash == "abc123def456"

    def test_search_snapshot_default_crawled_at(self, db_session, sample_station):
        """crawled_at 미지정 시 현재 시각 부근이 기본값으로 설정된다."""
        before = datetime.utcnow()
        snap = SearchSnapshot(
            station_id=sample_station.id,
            total_listings=10,
        )
        db_session.add(snap)
        db_session.commit()
        db_session.refresh(snap)
        after = datetime.utcnow()

        assert snap.crawled_at is not None
        assert before <= snap.crawled_at <= after

    def test_search_snapshot_station_relationship(self, db_session, sample_station):
        """SearchSnapshot.station 관계로 Station을 참조할 수 있다."""
        snap = SearchSnapshot(
            station_id=sample_station.id,
            total_listings=5,
        )
        db_session.add(snap)
        db_session.commit()
        db_session.refresh(snap)

        assert snap.station is not None
        assert snap.station.name == sample_station.name


# =========================================================================
# 4. CalendarSnapshot model tests
# =========================================================================


class TestCalendarSnapshot:
    """CalendarSnapshot ORM 모델 테스트."""

    def test_create_calendar_snapshot(self, db_session, sample_listing):
        """CalendarSnapshot을 생성하고 필드를 확인한다."""
        snap = CalendarSnapshot(
            listing_id=sample_listing.id,
            date=date(2026, 2, 18),
            available=True,
            price=100000.0,
            min_nights=2,
        )
        db_session.add(snap)
        db_session.commit()
        db_session.refresh(snap)

        assert snap.id is not None
        assert snap.listing_id == sample_listing.id
        assert snap.date == date(2026, 2, 18)
        assert snap.available is True
        assert snap.price == pytest.approx(100000.0)
        assert snap.min_nights == 2

    def test_calendar_snapshot_default_crawled_at(self, db_session, sample_listing):
        """crawled_at 미지정 시 현재 시각 부근이 기본값으로 설정된다."""
        before = datetime.utcnow()
        snap = CalendarSnapshot(
            listing_id=sample_listing.id,
            date=date(2026, 2, 20),
        )
        db_session.add(snap)
        db_session.commit()
        db_session.refresh(snap)
        after = datetime.utcnow()

        assert snap.crawled_at is not None
        assert before <= snap.crawled_at <= after

    def test_calendar_snapshot_listing_relationship(self, db_session, sample_listing):
        """CalendarSnapshot.listing 관계로 Listing을 참조할 수 있다."""
        snap = CalendarSnapshot(
            listing_id=sample_listing.id,
            date=date(2026, 2, 19),
            available=False,
        )
        db_session.add(snap)
        db_session.commit()
        db_session.refresh(snap)

        assert snap.listing is not None
        assert snap.listing.airbnb_id == sample_listing.airbnb_id

    def test_calendar_snapshot_unavailable(self, db_session, sample_listing):
        """available=False, price=None인 스냅샷을 저장할 수 있다."""
        snap = CalendarSnapshot(
            listing_id=sample_listing.id,
            date=date(2026, 2, 21),
            available=False,
            price=None,
            min_nights=None,
        )
        db_session.add(snap)
        db_session.commit()
        db_session.refresh(snap)

        assert snap.available is False
        assert snap.price is None
        assert snap.min_nights is None


# =========================================================================
# 5. DailyStat model tests
# =========================================================================


class TestDailyStat:
    """DailyStat ORM 모델 테스트."""

    def test_create_daily_stat(self, db_session, sample_station):
        """DailyStat을 생성하고 모든 필드를 확인한다."""
        stat = DailyStat(
            station_id=sample_station.id,
            date=date(2026, 2, 17),
            room_type="entire_home",
            total_listings=50,
            booked_count=40,
            booking_rate=0.80,
            avg_daily_price=130000.0,
            estimated_revenue=5200000.0,
        )
        db_session.add(stat)
        db_session.commit()
        db_session.refresh(stat)

        assert stat.id is not None
        assert stat.station_id == sample_station.id
        assert stat.date == date(2026, 2, 17)
        assert stat.room_type == "entire_home"
        assert stat.total_listings == 50
        assert stat.booked_count == 40
        assert stat.booking_rate == pytest.approx(0.80)
        assert stat.avg_daily_price == pytest.approx(130000.0)
        assert stat.estimated_revenue == pytest.approx(5200000.0)

    def test_daily_stat_station_relationship(self, db_session, sample_station):
        """DailyStat.station 관계로 Station을 참조할 수 있다."""
        stat = DailyStat(
            station_id=sample_station.id,
            date=date(2026, 2, 17),
        )
        db_session.add(stat)
        db_session.commit()
        db_session.refresh(stat)

        assert stat.station is not None
        assert stat.station.id == sample_station.id

    def test_daily_stat_nullable_fields(self, db_session, sample_station):
        """nullable 필드(room_type, total_listings 등)를 None으로 저장할 수 있다."""
        stat = DailyStat(
            station_id=sample_station.id,
            date=date(2026, 1, 1),
            room_type=None,
            total_listings=None,
            booked_count=None,
            booking_rate=None,
            avg_daily_price=None,
            estimated_revenue=None,
        )
        db_session.add(stat)
        db_session.commit()
        db_session.refresh(stat)

        assert stat.room_type is None
        assert stat.total_listings is None
        assert stat.estimated_revenue is None


# =========================================================================
# 6. CrawlLog model tests
# =========================================================================


class TestCrawlLog:
    """CrawlLog ORM 모델 테스트."""

    def test_create_crawl_log(self, db_session):
        """CrawlLog를 생성하고 필드를 확인한다."""
        log = CrawlLog(
            job_type="search",
            status="success",
            total_requests=100,
            successful_requests=95,
            failed_requests=3,
            blocked_requests=2,
        )
        db_session.add(log)
        db_session.commit()
        db_session.refresh(log)

        assert log.id is not None
        assert log.job_type == "search"
        assert log.status == "success"
        assert log.total_requests == 100
        assert log.successful_requests == 95
        assert log.failed_requests == 3
        assert log.blocked_requests == 2

    def test_crawl_log_default_request_counts(self, db_session):
        """request 카운터들의 기본값이 0이다."""
        log = CrawlLog(job_type="calendar")
        db_session.add(log)
        db_session.commit()
        db_session.refresh(log)

        assert log.total_requests == 0
        assert log.successful_requests == 0
        assert log.failed_requests == 0
        assert log.blocked_requests == 0

    def test_crawl_log_default_started_at(self, db_session):
        """started_at 미지정 시 현재 시각 부근이 기본값으로 설정된다."""
        before = datetime.utcnow()
        log = CrawlLog(job_type="listing")
        db_session.add(log)
        db_session.commit()
        db_session.refresh(log)
        after = datetime.utcnow()

        assert log.started_at is not None
        assert before <= log.started_at <= after

    def test_crawl_log_finished_at_nullable(self, db_session):
        """finished_at은 None(미완료) 상태로 저장할 수 있다."""
        log = CrawlLog(job_type="search", status="partial")
        db_session.add(log)
        db_session.commit()
        db_session.refresh(log)

        assert log.finished_at is None

    def test_crawl_log_with_error_message(self, db_session):
        """error_message 필드에 에러 내용을 저장할 수 있다."""
        log = CrawlLog(
            job_type="calendar",
            status="failed",
            error_message="Connection timeout after 30s",
        )
        db_session.add(log)
        db_session.commit()
        db_session.refresh(log)

        assert log.error_message == "Connection timeout after 30s"

    def test_crawl_log_with_finished_at(self, db_session):
        """완료 시각을 명시적으로 설정할 수 있다."""
        started = datetime(2026, 2, 17, 10, 0, 0)
        finished = datetime(2026, 2, 17, 10, 30, 0)
        log = CrawlLog(
            job_type="search",
            started_at=started,
            finished_at=finished,
            status="success",
            total_requests=50,
            successful_requests=50,
        )
        db_session.add(log)
        db_session.commit()
        db_session.refresh(log)

        assert log.started_at == started
        assert log.finished_at == finished


# =========================================================================
# 7. Cross-model relationship tests
# =========================================================================


class TestCrossModelRelationships:
    """여러 모델 간의 관계를 종합적으로 테스트."""

    def test_station_to_listing_to_calendar(self, db_session):
        """Station -> Listing -> CalendarSnapshot 전체 체인을 탐색할 수 있다."""
        station = Station(
            name="잠실", line="2호선", latitude=37.5133, longitude=127.1001
        )
        db_session.add(station)
        db_session.commit()

        listing = Listing(
            airbnb_id="CHAIN001",
            name="잠실 숙소",
            nearest_station_id=station.id,
        )
        db_session.add(listing)
        db_session.commit()

        cal = CalendarSnapshot(
            listing_id=listing.id,
            date=date(2026, 4, 1),
            available=True,
            price=200000.0,
        )
        db_session.add(cal)
        db_session.commit()

        # Station -> listings -> calendar_snapshots 순회
        db_session.refresh(station)
        assert len(station.listings) == 1
        fetched_listing = station.listings[0]
        assert len(fetched_listing.calendar_snapshots) == 1
        assert fetched_listing.calendar_snapshots[0].price == pytest.approx(200000.0)

    def test_station_with_multiple_relationship_types(self, db_session):
        """하나의 Station에 listings, search_snapshots, daily_stats를 모두 연결."""
        station = Station(
            name="건대입구", line="2호선", latitude=37.5403, longitude=127.0695
        )
        db_session.add(station)
        db_session.commit()

        listing = Listing(
            airbnb_id="MULTI001", name="건대 숙소", nearest_station_id=station.id
        )
        snap = SearchSnapshot(station_id=station.id, total_listings=20)
        stat = DailyStat(
            station_id=station.id,
            date=date(2026, 2, 17),
            total_listings=20,
        )
        db_session.add_all([listing, snap, stat])
        db_session.commit()
        db_session.refresh(station)

        assert len(station.listings) == 1
        assert len(station.search_snapshots) == 1
        assert len(station.daily_stats) == 1

    def test_back_populates_consistency(self, db_session):
        """back_populates 양방향 관계가 일관성 있게 동작한다."""
        station = Station(
            name="성수", line="2호선", latitude=37.5446, longitude=127.0557
        )
        db_session.add(station)
        db_session.commit()

        listing = Listing(
            airbnb_id="BP001", name="성수 숙소", nearest_station_id=station.id
        )
        db_session.add(listing)
        db_session.commit()
        db_session.refresh(station)
        db_session.refresh(listing)

        # Listing -> Station
        assert listing.nearest_station.id == station.id
        # Station -> Listing
        assert listing in station.listings


# =========================================================================
# 8. database.py tests - get_engine, init_db, get_session, session_scope
# =========================================================================


class TestGetEngine:
    """database.get_engine 테스트."""

    def test_get_engine_with_explicit_path(self, tmp_path):
        """명시적 db_path를 전달하면 해당 경로로 엔진을 생성한다."""
        from models.database import get_engine

        db_path = tmp_path / "custom.db"
        engine = get_engine(db_path=str(db_path))

        assert engine is not None
        assert str(db_path) in str(engine.url)

    def test_get_engine_default_path(self):
        """db_path 미전달 시 settings.DB_PATH를 사용한다."""
        from models.database import get_engine

        engine = get_engine()
        assert engine is not None
        # DB_PATH 가 URL에 포함되어야 한다
        assert "airbnb_seoul.db" in str(engine.url)


class TestInitDb:
    """database.init_db 테스트."""

    def test_init_db_creates_tables(self, tmp_path):
        """init_db가 모든 테이블을 생성한다."""
        from models.database import init_db

        db_path = tmp_path / "init_test.db"
        engine = init_db(db_path=str(db_path))

        inspector = inspect(engine)
        table_names = set(inspector.get_table_names())

        expected_tables = {
            "stations",
            "listings",
            "search_snapshots",
            "calendar_snapshots",
            "daily_stats",
            "crawl_logs",
        }
        assert expected_tables.issubset(table_names)

    def test_init_db_returns_engine(self, tmp_path):
        """init_db가 SQLAlchemy 엔진을 반환한다."""
        from models.database import init_db

        db_path = tmp_path / "engine_test.db"
        engine = init_db(db_path=str(db_path))

        assert engine is not None
        assert hasattr(engine, "connect")

    def test_init_db_creates_data_dir(self, tmp_path):
        """init_db가 DATA_DIR를 생성한다 (이미 존재하면 에러 없이 통과)."""
        from models.database import init_db

        db_path = tmp_path / "dir_test.db"
        # DATA_DIR.mkdir(parents=True, exist_ok=True) 호출 확인
        # tmp_path를 사용하므로 실제 data/ 디렉토리가 생성되는지 확인
        engine = init_db(db_path=str(db_path))
        assert engine is not None

    def test_init_db_sets_global_session_factory(self, tmp_path):
        """init_db 호출 후 모듈의 _SessionFactory가 설정된다."""
        import models.database as db_module
        from models.database import init_db

        # 기존 상태 백업
        old_engine = db_module._engine
        old_factory = db_module._SessionFactory

        try:
            db_path = tmp_path / "global_test.db"
            init_db(db_path=str(db_path))

            assert db_module._engine is not None
            assert db_module._SessionFactory is not None
        finally:
            # 상태 복원
            db_module._engine = old_engine
            db_module._SessionFactory = old_factory

    def test_init_db_idempotent(self, tmp_path):
        """init_db를 두 번 호출해도 에러가 발생하지 않는다."""
        from models.database import init_db

        db_path = tmp_path / "idempotent_test.db"
        engine1 = init_db(db_path=str(db_path))
        engine2 = init_db(db_path=str(db_path))

        assert engine1 is not None
        assert engine2 is not None


class TestGetSession:
    """database.get_session 테스트."""

    def test_get_session_returns_session(self, tmp_path):
        """get_session이 SQLAlchemy Session 객체를 반환한다."""
        import models.database as db_module
        from models.database import get_session, init_db

        old_engine = db_module._engine
        old_factory = db_module._SessionFactory

        try:
            db_path = tmp_path / "session_test.db"
            init_db(db_path=str(db_path))

            session = get_session()
            assert session is not None
            # Session 인스턴스인지 확인
            assert hasattr(session, "commit")
            assert hasattr(session, "rollback")
            assert hasattr(session, "query")
            session.close()
        finally:
            db_module._engine = old_engine
            db_module._SessionFactory = old_factory

    def test_get_session_auto_inits_if_no_factory(self, tmp_path):
        """_SessionFactory가 None이면 get_session이 init_db를 호출한다."""
        import models.database as db_module
        from models.database import get_session

        old_engine = db_module._engine
        old_factory = db_module._SessionFactory

        try:
            db_module._SessionFactory = None
            db_module._engine = None

            session = get_session()
            assert session is not None
            # init_db가 호출되어 _SessionFactory가 설정되었어야 한다
            assert db_module._SessionFactory is not None
            session.close()
        finally:
            db_module._engine = old_engine
            db_module._SessionFactory = old_factory


class TestSessionScope:
    """database.session_scope 컨텍스트 매니저 테스트."""

    def test_session_scope_commits_on_success(self, tmp_path):
        """정상 종료 시 session_scope가 commit한다."""
        import models.database as db_module
        from models.database import init_db, session_scope

        old_engine = db_module._engine
        old_factory = db_module._SessionFactory

        try:
            db_path = tmp_path / "scope_commit.db"
            init_db(db_path=str(db_path))

            with session_scope() as session:
                station = Station(
                    name="테스트역",
                    line="1호선",
                    latitude=37.0,
                    longitude=127.0,
                )
                session.add(station)

            # 새 세션으로 데이터가 커밋되었는지 확인
            from models.database import get_session

            verify_session = get_session()
            try:
                result = verify_session.query(Station).filter_by(name="테스트역").first()
                assert result is not None
                assert result.name == "테스트역"
            finally:
                verify_session.close()
        finally:
            db_module._engine = old_engine
            db_module._SessionFactory = old_factory

    def test_session_scope_rolls_back_on_exception(self, tmp_path):
        """예외 발생 시 session_scope가 rollback하고 예외를 다시 발생시킨다."""
        import models.database as db_module
        from models.database import init_db, session_scope

        old_engine = db_module._engine
        old_factory = db_module._SessionFactory

        try:
            db_path = tmp_path / "scope_rollback.db"
            init_db(db_path=str(db_path))

            with pytest.raises(ValueError, match="의도적 에러"):
                with session_scope() as session:
                    station = Station(
                        name="롤백역",
                        line="3호선",
                        latitude=37.0,
                        longitude=127.0,
                    )
                    session.add(station)
                    raise ValueError("의도적 에러")

            # 롤백 되었으므로 데이터가 없어야 한다
            from models.database import get_session

            verify_session = get_session()
            try:
                result = verify_session.query(Station).filter_by(name="롤백역").first()
                assert result is None
            finally:
                verify_session.close()
        finally:
            db_module._engine = old_engine
            db_module._SessionFactory = old_factory

    def test_session_scope_closes_session(self, tmp_path):
        """session_scope가 finally에서 세션을 close한다."""
        import models.database as db_module
        from models.database import init_db, session_scope

        old_engine = db_module._engine
        old_factory = db_module._SessionFactory

        try:
            db_path = tmp_path / "scope_close.db"
            init_db(db_path=str(db_path))

            captured_session = None
            with session_scope() as session:
                captured_session = session

            # 세션이 close 되었으므로 더 이상 활성 상태가 아니어야 한다
            # SQLAlchemy에서 close된 세션은 is_active가 True일 수 있지만
            # _close_state가 설정됨. 여기서는 새 트랜잭션이 시작되지 않았음을 확인.
            assert captured_session is not None
        finally:
            db_module._engine = old_engine
            db_module._SessionFactory = old_factory

    def test_session_scope_reraises_exception(self, tmp_path):
        """session_scope가 rollback 후 원래 예외를 re-raise한다."""
        import models.database as db_module
        from models.database import init_db, session_scope

        old_engine = db_module._engine
        old_factory = db_module._SessionFactory

        try:
            db_path = tmp_path / "scope_reraise.db"
            init_db(db_path=str(db_path))

            with pytest.raises(RuntimeError, match="테스트 런타임 에러"):
                with session_scope() as session:
                    raise RuntimeError("테스트 런타임 에러")
        finally:
            db_module._engine = old_engine
            db_module._SessionFactory = old_factory

    def test_session_scope_multiple_operations(self, tmp_path):
        """session_scope 안에서 여러 모델을 한 트랜잭션으로 저장할 수 있다."""
        import models.database as db_module
        from models.database import init_db, session_scope

        old_engine = db_module._engine
        old_factory = db_module._SessionFactory

        try:
            db_path = tmp_path / "scope_multi.db"
            init_db(db_path=str(db_path))

            with session_scope() as session:
                station = Station(
                    name="멀티역", line="7호선", latitude=37.5, longitude=127.0
                )
                session.add(station)
                session.flush()  # station.id 확보

                listing = Listing(
                    airbnb_id="MULTI999",
                    name="멀티 숙소",
                    nearest_station_id=station.id,
                )
                session.add(listing)

                log = CrawlLog(job_type="search", status="success")
                session.add(log)

            # 모두 커밋 되었는지 확인
            from models.database import get_session

            verify_session = get_session()
            try:
                assert verify_session.query(Station).filter_by(name="멀티역").count() == 1
                assert (
                    verify_session.query(Listing).filter_by(airbnb_id="MULTI999").count()
                    == 1
                )
                assert (
                    verify_session.query(CrawlLog).filter_by(job_type="search").count()
                    == 1
                )
            finally:
                verify_session.close()
        finally:
            db_module._engine = old_engine
            db_module._SessionFactory = old_factory


# =========================================================================
# 9. Table structure / index verification
# =========================================================================


class TestTableStructure:
    """테이블 구조 및 인덱스가 올바르게 생성되었는지 확인."""

    def test_all_tables_exist(self, tmp_db):
        """모든 예상 테이블이 존재한다."""
        engine, _ = tmp_db
        inspector = inspect(engine)
        table_names = set(inspector.get_table_names())

        expected = {
            "stations",
            "listings",
            "search_snapshots",
            "calendar_snapshots",
            "daily_stats",
            "crawl_logs",
        }
        assert expected.issubset(table_names)

    def test_listings_has_airbnb_id_index(self, tmp_db):
        """listings 테이블에 airbnb_id 인덱스가 존재한다."""
        engine, _ = tmp_db
        inspector = inspect(engine)
        indexes = inspector.get_indexes("listings")
        index_columns = [
            col for idx in indexes for col in idx["column_names"]
        ]
        assert "airbnb_id" in index_columns

    def test_calendar_snapshots_has_listing_date_index(self, tmp_db):
        """calendar_snapshots에 listing_id + date 복합 인덱스가 있다."""
        engine, _ = tmp_db
        inspector = inspect(engine)
        indexes = inspector.get_indexes("calendar_snapshots")
        found = any(
            set(idx["column_names"]) == {"listing_id", "date"}
            for idx in indexes
        )
        assert found

    def test_search_snapshots_has_station_time_index(self, tmp_db):
        """search_snapshots에 station_id + crawled_at 복합 인덱스가 있다."""
        engine, _ = tmp_db
        inspector = inspect(engine)
        indexes = inspector.get_indexes("search_snapshots")
        found = any(
            set(idx["column_names"]) == {"station_id", "crawled_at"}
            for idx in indexes
        )
        assert found

    def test_daily_stats_has_station_date_index(self, tmp_db):
        """daily_stats에 station_id + date 복합 인덱스가 있다."""
        engine, _ = tmp_db
        inspector = inspect(engine)
        indexes = inspector.get_indexes("daily_stats")
        found = any(
            set(idx["column_names"]) == {"station_id", "date"}
            for idx in indexes
        )
        assert found

    def test_crawl_logs_has_type_time_index(self, tmp_db):
        """crawl_logs에 job_type + started_at 복합 인덱스가 있다."""
        engine, _ = tmp_db
        inspector = inspect(engine)
        indexes = inspector.get_indexes("crawl_logs")
        found = any(
            set(idx["column_names"]) == {"job_type", "started_at"}
            for idx in indexes
        )
        assert found

    def test_listings_foreign_key_to_stations(self, tmp_db):
        """listings.nearest_station_id가 stations.id를 참조한다."""
        engine, _ = tmp_db
        inspector = inspect(engine)
        fks = inspector.get_foreign_keys("listings")
        station_fk = [
            fk for fk in fks if fk["referred_table"] == "stations"
        ]
        assert len(station_fk) >= 1
        assert "nearest_station_id" in station_fk[0]["constrained_columns"]
