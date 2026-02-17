"""SQLAlchemy ORM 모델 정의"""

from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


class Station(Base):
    """지하철역"""
    __tablename__ = "stations"

    id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=False)
    line = Column(String(20), nullable=False)
    district = Column(String(30))
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    priority = Column(Integer, default=3)  # 1=최우선, 2=중간, 3=일반

    listings = relationship("Listing", back_populates="nearest_station")
    search_snapshots = relationship("SearchSnapshot", back_populates="station")
    daily_stats = relationship("DailyStat", back_populates="station")

    def __repr__(self):
        return f"<Station {self.name}({self.line})>"


class Listing(Base):
    """Airbnb 숙소"""
    __tablename__ = "listings"

    id = Column(Integer, primary_key=True)
    airbnb_id = Column(String(30), unique=True, nullable=False, index=True)
    name = Column(Text)
    host_id = Column(String(30))
    room_type = Column(String(30))  # entire_home / private_room / shared_room / hotel
    latitude = Column(Float)
    longitude = Column(Float)
    nearest_station_id = Column(Integer, ForeignKey("stations.id"))
    bedrooms = Column(Integer)
    bathrooms = Column(Float)
    max_guests = Column(Integer)
    base_price = Column(Float)      # 기본 1박 가격 (KRW)
    rating = Column(Float)
    review_count = Column(Integer)
    first_seen = Column(DateTime, default=datetime.utcnow)
    last_seen = Column(DateTime, default=datetime.utcnow)

    nearest_station = relationship("Station", back_populates="listings")
    calendar_snapshots = relationship("CalendarSnapshot", back_populates="listing")

    __table_args__ = (
        Index("ix_listing_station", "nearest_station_id"),
        Index("ix_listing_room_type", "room_type"),
    )

    def __repr__(self):
        return f"<Listing {self.airbnb_id}: {self.name}>"


class SearchSnapshot(Base):
    """검색 스냅샷 (시간별)"""
    __tablename__ = "search_snapshots"

    id = Column(Integer, primary_key=True)
    station_id = Column(Integer, ForeignKey("stations.id"), nullable=False)
    crawled_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    total_listings = Column(Integer)
    avg_price = Column(Float)
    min_price = Column(Float)
    max_price = Column(Float)
    median_price = Column(Float)
    available_count = Column(Integer)
    checkin_date = Column(Date)
    checkout_date = Column(Date)
    raw_response_hash = Column(String(64))  # 중복 감지용

    station = relationship("Station", back_populates="search_snapshots")

    __table_args__ = (
        Index("ix_snapshot_station_time", "station_id", "crawled_at"),
    )


class CalendarSnapshot(Base):
    """캘린더 스냅샷 (숙소별 날짜별)"""
    __tablename__ = "calendar_snapshots"

    id = Column(Integer, primary_key=True)
    listing_id = Column(Integer, ForeignKey("listings.id"), nullable=False)
    crawled_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    date = Column(Date, nullable=False)
    available = Column(Boolean)
    price = Column(Float)
    min_nights = Column(Integer)

    listing = relationship("Listing", back_populates="calendar_snapshots")

    __table_args__ = (
        Index("ix_calendar_listing_date", "listing_id", "date"),
        Index("ix_calendar_crawled", "crawled_at"),
    )


class DailyStat(Base):
    """일별 집계 통계"""
    __tablename__ = "daily_stats"

    id = Column(Integer, primary_key=True)
    station_id = Column(Integer, ForeignKey("stations.id"), nullable=False)
    date = Column(Date, nullable=False)
    room_type = Column(String(30))
    total_listings = Column(Integer)
    booked_count = Column(Integer)
    booking_rate = Column(Float)        # 0~1
    avg_daily_price = Column(Float)
    estimated_revenue = Column(Float)

    station = relationship("Station", back_populates="daily_stats")

    __table_args__ = (
        Index("ix_daily_station_date", "station_id", "date"),
    )


class CrawlLog(Base):
    """크롤링 실행 로그 (모니터링용)"""
    __tablename__ = "crawl_logs"

    id = Column(Integer, primary_key=True)
    job_type = Column(String(30), nullable=False)   # search / calendar / listing
    started_at = Column(DateTime, default=datetime.utcnow)
    finished_at = Column(DateTime)
    status = Column(String(20))         # success / partial / failed
    total_requests = Column(Integer, default=0)
    successful_requests = Column(Integer, default=0)
    failed_requests = Column(Integer, default=0)
    blocked_requests = Column(Integer, default=0)   # 차단 감지 횟수
    error_message = Column(Text)

    __table_args__ = (
        Index("ix_crawllog_type_time", "job_type", "started_at"),
    )
