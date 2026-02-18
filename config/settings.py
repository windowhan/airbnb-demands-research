"""
전역 설정 - 옵션 A(최소) ~ 옵션 C(풀스케일) 확장 가능 구조

CRAWL_TIER를 변경하면 요청량, 딜레이, 프록시 전략이 자동 조정됩니다.
  - "A": 1순위 30개역, 프록시 없이, 보수적 딜레이
  - "B": 전체 역, 프록시 3~5개, 중간 딜레이
  - "C": 전체 역 + 전체 캘린더 + 상세, 프록시 10+개
"""

import os
from pathlib import Path

# ── 프로젝트 경로 ──────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "airbnb_seoul.db"
LOG_DIR = BASE_DIR / "logs"

# ── 크롤링 티어 (A / B / C) ──────────────────────────────────
CRAWL_TIER = os.getenv("CRAWL_TIER", "A").upper()

# ── 검색 파라미터 ─────────────────────────────────────────────
SEARCH_RADIUS_KM = 3.0          # 역 중심 검색 반경
DEFAULT_GUESTS = 2              # 기본 게스트 수
CALENDAR_LOOKAHEAD_DAYS = 90    # 캘린더 조회 범위 (일)
CURRENCY = "KRW"

# ── 티어별 설정 ───────────────────────────────────────────────
TIER_CONFIG = {
    "A": {
        "station_priority": [1],          # 1순위 역만 (약 30개)
        "search_interval_minutes": 60,    # 검색 스냅샷 주기
        "calendar_enabled": True,
        "calendar_hour": 3,               # 캘린더 크롤링 시각 (새벽 3시)
        "listing_detail_enabled": False,   # 상세 크롤링 비활성
        "max_concurrent_requests": 1,
        "delay_base": 7.0,                # 기본 딜레이 (초)
        "delay_jitter": (2.0, 8.0),       # 랜덤 지터 범위
        "proxy_required": False,
        "requests_per_ip_before_rotate": 500,
        "max_requests_per_hour": 500,
        "daily_limit_per_ip": 8000,
    },
    "B": {
        "station_priority": [1, 2],       # 1, 2순위 역 (약 100개)
        "search_interval_minutes": 60,
        "calendar_enabled": True,
        "calendar_hour": 2,
        "listing_detail_enabled": True,
        "max_concurrent_requests": 2,
        "delay_base": 5.0,
        "delay_jitter": (1.0, 5.0),
        "proxy_required": True,
        "requests_per_ip_before_rotate": 30,
        "max_requests_per_hour": 80,
        "daily_limit_per_ip": 600,
    },
    "C": {
        "station_priority": [1, 2, 3],    # 전체 역 (약 300개)
        "search_interval_minutes": 60,
        "calendar_enabled": True,
        "calendar_hour": 1,
        "listing_detail_enabled": True,
        "max_concurrent_requests": 3,
        "delay_base": 4.0,
        "delay_jitter": (1.0, 4.0),
        "proxy_required": True,
        "requests_per_ip_before_rotate": 25,
        "max_requests_per_hour": 100,
        "daily_limit_per_ip": 500,
    },
}


def get_tier_config() -> dict:
    """현재 티어의 설정을 반환합니다."""
    if CRAWL_TIER not in TIER_CONFIG:
        raise ValueError(f"Unknown CRAWL_TIER: {CRAWL_TIER}. Must be A, B, or C.")
    return TIER_CONFIG[CRAWL_TIER]


# ── Airbnb API ────────────────────────────────────────────────
AIRBNB_API_BASE = "https://www.airbnb.co.kr"
AIRBNB_API_KEY = os.getenv("AIRBNB_API_KEY", "")  # 브라우저에서 추출 필요

# ── 프록시 설정 ───────────────────────────────────────────────
# 환경변수 또는 파일에서 프록시 목록 로드
# 형식: "protocol://user:pass@host:port" (한 줄에 하나씩)
PROXY_LIST_FILE = BASE_DIR / "config" / "proxies.txt"
PROXY_LIST_ENV = os.getenv("PROXY_LIST", "")  # 쉼표 구분

# ── User-Agent 로테이션 풀 ────────────────────────────────────
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
]

# ── 로깅 ──────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
