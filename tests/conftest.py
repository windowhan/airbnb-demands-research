"""공유 pytest 픽스처"""

import json
import os
import tempfile
from datetime import date, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import create_engine
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


@pytest.fixture
def tmp_db(tmp_path):
    """인메모리 SQLite DB + 테이블 생성."""
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return engine, Session


@pytest.fixture
def db_session(tmp_db):
    """DB 세션 픽스처."""
    engine, Session = tmp_db
    session = Session()
    yield session
    session.close()


@pytest.fixture
def sample_station(db_session):
    """테스트용 역 데이터."""
    station = Station(
        name="강남",
        line="2호선",
        district="강남구",
        latitude=37.4981,
        longitude=127.0276,
        priority=1,
    )
    db_session.add(station)
    db_session.commit()
    db_session.refresh(station)
    return station


@pytest.fixture
def sample_listing(db_session, sample_station):
    """테스트용 리스팅 데이터."""
    listing = Listing(
        airbnb_id="1234567890",
        name="강남 테스트 숙소",
        room_type="entire_home",
        latitude=37.499,
        longitude=127.028,
        nearest_station_id=sample_station.id,
        base_price=100000.0,
        rating=4.5,
        review_count=10,
        first_seen=datetime.utcnow(),
        last_seen=datetime.utcnow(),
    )
    db_session.add(listing)
    db_session.commit()
    db_session.refresh(listing)
    return listing


@pytest.fixture
def mock_session_scope(db_session):
    """session_scope를 테스트 DB 세션으로 모킹."""
    from contextlib import contextmanager

    @contextmanager
    def _mock_scope():
        try:
            yield db_session
            db_session.flush()
        except Exception:
            db_session.rollback()
            raise

    return _mock_scope


@pytest.fixture
def sample_search_response():
    """Airbnb StaysSearch API 응답 샘플."""
    return {
        "data": {
            "presentation": {
                "staysSearch": {
                    "results": {
                        "searchResults": [
                            {
                                "propertyId": None,
                                "nameLocalized": {
                                    "localizedStringWithTranslationPreference": "강남 테스트 숙소 A"
                                },
                                "avgRatingLocalized": "4.89",
                                "structuredDisplayPrice": {
                                    "primaryLine": {
                                        "discountedPrice": "₩119,824",
                                        "price": None,
                                    }
                                },
                                "demandStayListing": {
                                    "id": "RGVtYW5kU3RheUxpc3Rpbmc6MTIzNDU2Nzg5MA==",
                                    "roomTypeCategory": "entire_home",
                                    "reviewsCount": 25,
                                    "location": {
                                        "coordinate": {
                                            "latitude": 37.499,
                                            "longitude": 127.028,
                                        }
                                    },
                                },
                            },
                            {
                                "propertyId": "9876543210",
                                "nameLocalized": "홍대 테스트 숙소 B",
                                "avgRatingLocalized": "4.5",
                                "structuredDisplayPrice": {
                                    "primaryLine": {
                                        "price": "₩80,000",
                                    }
                                },
                                "demandStayListing": {
                                    "id": "",
                                    "roomTypeCategory": "private_room",
                                    "reviewsCount": 5,
                                    "location": {
                                        "coordinate": {
                                            "latitude": 37.556,
                                            "longitude": 126.923,
                                        }
                                    },
                                },
                            },
                            {
                                "propertyId": None,
                                "nameLocalized": None,
                                "avgRatingLocalized": None,
                                "structuredDisplayPrice": {},
                                "demandStayListing": None,
                                "listing": {
                                    "id": "5555555",
                                    "name": "구버전 숙소 C",
                                    "roomTypeCategory": "shared_room",
                                    "coordinate": {
                                        "latitude": 37.5,
                                        "longitude": 127.0,
                                    },
                                    "avgRating": 3.8,
                                    "reviewsCount": 2,
                                },
                                "pricingQuote": {
                                    "price": {
                                        "total": {"amount": 60000}
                                    }
                                },
                            },
                        ]
                    }
                }
            }
        }
    }


@pytest.fixture
def sample_calendar_response():
    """Airbnb PdpAvailabilityCalendar API 응답 샘플."""
    return {
        "data": {
            "merlin": {
                "__typename": "MerlinQuery",
                "pdpAvailabilityCalendar": {
                    "calendarMonths": [
                        {
                            "month": 2,
                            "year": 2026,
                            "days": [
                                {
                                    "calendarDate": "2026-02-01",
                                    "available": False,
                                    "minNights": 1,
                                    "maxNights": 365,
                                    "bookable": None,
                                    "price": {"localPriceFormatted": None},
                                },
                                {
                                    "calendarDate": "2026-02-18",
                                    "available": True,
                                    "minNights": 2,
                                    "maxNights": 365,
                                    "bookable": True,
                                    "price": {"localPriceFormatted": "₩100,000"},
                                },
                                {
                                    "calendarDate": "2026-02-19",
                                    "available": True,
                                    "minNights": 1,
                                    "maxNights": 365,
                                    "bookable": True,
                                    "price": {"localPriceFormatted": None},
                                },
                            ],
                        }
                    ]
                },
            }
        }
    }


@pytest.fixture
def sample_pdp_sections_response():
    """Airbnb StaysPdpSections API 응답 샘플."""
    return {
        "data": {
            "presentation": {
                "stayProductDetailPage": {
                    "sections": {
                        "sections": [
                            {
                                "sectionComponentType": "BOOK_IT_SIDEBAR",
                                "section": {
                                    "maxGuestCapacity": 4,
                                    "descriptionItems": None,
                                },
                            },
                            {
                                "sectionComponentType": "AVAILABILITY_CALENDAR_DEFAULT",
                                "section": {
                                    "title": "날짜 선택",
                                    "descriptionItems": [
                                        {"title": "공동 주택 전체"},
                                        {"title": "침실 2개"},
                                        {"title": "욕실 1개"},
                                    ],
                                },
                            },
                            {
                                "sectionComponentType": "MEET_YOUR_HOST",
                                "section": {
                                    "cardData": {
                                        "userId": "RGVtYW5kVXNlcjoxMjM0NTY=",
                                        "name": "테스트 호스트",
                                        "isSuperhost": True,
                                        "ratingAverage": 4.9,
                                        "stats": [
                                            {"type": "REVIEW_COUNT", "value": "150"},
                                            {"type": "RATING", "value": "4.9"},
                                        ],
                                    },
                                },
                            },
                            {
                                "sectionComponentType": "POLICIES_DEFAULT",
                                "section": {
                                    "houseRules": [
                                        {"title": "체크인 가능 시간: 오후 3:00 이후"},
                                        {"title": "게스트 정원 4명"},
                                    ],
                                },
                            },
                            {
                                "sectionComponentType": "AMENITIES_DEFAULT",
                                "section": {
                                    "previewAmenitiesGroups": [
                                        {
                                            "amenities": [
                                                {"title": "와이파이", "available": True},
                                                {"title": "주방", "available": True},
                                            ]
                                        }
                                    ],
                                },
                            },
                        ]
                    }
                }
            },
            "node": {"__typename": "DemandStayListing"},
        }
    }


@pytest.fixture
def mock_airbnb_client():
    """모킹된 AirbnbClient."""
    client = MagicMock()
    client.search_stays = AsyncMock(return_value=None)
    client.get_calendar = AsyncMock(return_value=None)
    client.get_listing_detail = AsyncMock(return_value=None)
    client.close = AsyncMock()
    client.compute_response_hash = MagicMock(return_value="abc123hash")
    client.get_stats = MagicMock(return_value={
        "rate_limiter": {},
        "proxy_manager": {"total": 0},
    })
    return client


@pytest.fixture
def tmp_cache_file(tmp_path):
    """임시 API credentials 캐시 파일."""
    cache = tmp_path / ".api_credentials.json"
    return cache


@pytest.fixture
def sample_credentials():
    """테스트용 API credentials."""
    return {
        "api_key": "d306zoyjsyarp7ifhu67rjxn52tv0t20",
        "hashes": {
            "StaysSearch": "e75ccaa7c9468e19d7613208b37d05f9b680529490ca9bc9d3361202ca0a4e43",
            "PdpAvailabilityCalendar": "b23335819df0dc391a338d665e2ee2f5d3bff19181d05c0b39bc6c5aac403914",
            "StaysPdpSections": "6bf07ebb4b297ecd3b4b6898a8dd300180d0db014e80e907e001c11b58cbe7b5",
        },
        "cached_at": 9999999999.0,
    }
