"""
Rate Limiter + Circuit Breaker - Airbnb 차단 방지 핵심 모듈

기능:
- 적응형 딜레이: 성공 시 딜레이 줄이고, 실패 시 증가
- 시간당/일일 요청 한도 관리
- Circuit Breaker: 연속 실패 시 자동 일시정지
- 차단 응답 감지 (403, 429, CAPTCHA, skeleton page)
"""

import asyncio
import hashlib
import logging
import random
import time
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class BlockType(Enum):
    """차단 유형 분류"""
    NONE = "none"
    RATE_LIMIT = "rate_limit"       # 429
    FORBIDDEN = "forbidden"         # 403
    CAPTCHA = "captcha"             # CAPTCHA 감지
    SKELETON = "skeleton"           # 200이지만 빈 페이지
    SERVER_ERROR = "server_error"   # 503


@dataclass
class RequestStats:
    """요청 통계를 추적합니다."""
    total: int = 0
    success: int = 0
    failed: int = 0
    blocked: int = 0
    consecutive_failures: int = 0
    hourly_count: int = 0
    daily_count: int = 0
    hour_start: float = field(default_factory=time.time)
    day_start: float = field(default_factory=time.time)

    def reset_hourly(self):
        self.hourly_count = 0
        self.hour_start = time.time()

    def reset_daily(self):
        self.daily_count = 0
        self.day_start = time.time()


class RateLimiter:
    """
    적응형 Rate Limiter + Circuit Breaker.

    티어 설정에 따라 딜레이와 한도가 자동 조정됩니다.
    """

    # Circuit Breaker 설정
    CB_FAILURE_THRESHOLD = 5        # 연속 N회 실패 시 서킷 오픈
    CB_OPEN_DURATION = 300          # 서킷 오픈 시 대기 (초)
    CB_HALF_OPEN_REQUESTS = 2       # 반오픈 상태에서 시험 요청 수

    def __init__(self, delay_base: float = 7.0,
                 delay_jitter: tuple[float, float] = (2.0, 8.0),
                 max_requests_per_hour: int = 50,
                 daily_limit: int = 800):
        self._delay_base = delay_base
        self._delay_jitter = delay_jitter
        self._max_per_hour = max_requests_per_hour
        self._daily_limit = daily_limit

        self._stats = RequestStats()
        self._current_delay_multiplier = 1.0   # 적응형 딜레이 배수

        # Circuit Breaker 상태
        self._circuit_open = False
        self._circuit_open_until = 0.0
        self._half_open_count = 0

    @classmethod
    def from_config(cls) -> "RateLimiter":
        """티어 설정에서 Rate Limiter를 생성합니다."""
        from config.settings import get_tier_config
        tier = get_tier_config()
        return cls(
            delay_base=tier["delay_base"],
            delay_jitter=tier["delay_jitter"],
            max_requests_per_hour=tier["max_requests_per_hour"],
            daily_limit=tier["daily_limit_per_ip"],
        )

    async def wait(self):
        """
        다음 요청 전 적절한 시간만큼 대기합니다.
        한도 초과 시 추가 대기, Circuit Breaker 오픈 시 장시간 대기.
        """
        # Circuit Breaker 체크
        if self._circuit_open:
            remaining = self._circuit_open_until - time.time()
            if remaining > 0:
                logger.warning("Circuit breaker OPEN. Waiting %.0fs...", remaining)
                await asyncio.sleep(remaining)
            self._circuit_open = False
            self._half_open_count = 0
            logger.info("Circuit breaker → HALF-OPEN (testing with %d requests)",
                        self.CB_HALF_OPEN_REQUESTS)

        # 시간/일 카운터 리셋
        now = time.time()
        if now - self._stats.hour_start >= 3600:
            self._stats.reset_hourly()
        if now - self._stats.day_start >= 86400:
            self._stats.reset_daily()

        # 시간당 한도 체크
        if self._stats.hourly_count >= self._max_per_hour:
            wait_secs = 3600 - (now - self._stats.hour_start)
            if wait_secs > 0:
                logger.warning("Hourly limit reached (%d). Waiting %.0fs...",
                               self._max_per_hour, wait_secs)
                await asyncio.sleep(wait_secs)
                self._stats.reset_hourly()

        # 일일 한도 체크
        if self._stats.daily_count >= self._daily_limit:
            wait_secs = 86400 - (now - self._stats.day_start)
            logger.warning("Daily limit reached (%d). Waiting %.0fs...",
                           self._daily_limit, wait_secs)
            await asyncio.sleep(wait_secs)
            self._stats.reset_daily()

        # 적응형 딜레이 계산
        jitter = random.uniform(*self._delay_jitter)
        delay = (self._delay_base + jitter) * self._current_delay_multiplier
        await asyncio.sleep(delay)

        self._stats.total += 1
        self._stats.hourly_count += 1
        self._stats.daily_count += 1

    def report_success(self):
        """요청 성공을 기록합니다."""
        self._stats.success += 1
        self._stats.consecutive_failures = 0

        # 적응형 딜레이: 성공 시 천천히 정상화
        if self._current_delay_multiplier > 1.0:
            self._current_delay_multiplier = max(1.0, self._current_delay_multiplier * 0.9)

        # Half-open 상태에서 성공 시
        if self._half_open_count > 0:
            self._half_open_count += 1
            if self._half_open_count >= self.CB_HALF_OPEN_REQUESTS:
                logger.info("Circuit breaker → CLOSED (recovery confirmed)")
                self._half_open_count = 0

    def report_failure(self, block_type: BlockType = BlockType.NONE):
        """요청 실패를 기록합니다."""
        self._stats.failed += 1
        self._stats.consecutive_failures += 1

        if block_type != BlockType.NONE:
            self._stats.blocked += 1
            # 차단 유형별 딜레이 배수 조정
            if block_type == BlockType.RATE_LIMIT:
                self._current_delay_multiplier *= 2.0
            elif block_type == BlockType.FORBIDDEN:
                self._current_delay_multiplier *= 3.0
            elif block_type == BlockType.CAPTCHA:
                self._current_delay_multiplier *= 4.0
            else:
                self._current_delay_multiplier *= 1.5

            self._current_delay_multiplier = min(self._current_delay_multiplier, 10.0)
            logger.warning(
                "Block detected (%s). Delay multiplier → %.1fx",
                block_type.value, self._current_delay_multiplier,
            )

        # Circuit Breaker: 연속 실패 임계값 도달
        if self._stats.consecutive_failures >= self.CB_FAILURE_THRESHOLD:
            self._circuit_open = True
            self._circuit_open_until = time.time() + self.CB_OPEN_DURATION
            self._stats.consecutive_failures = 0
            logger.error(
                "Circuit breaker OPENED! %d consecutive failures. "
                "Pausing for %ds.",
                self.CB_FAILURE_THRESHOLD, self.CB_OPEN_DURATION,
            )

    @staticmethod
    def detect_block(status_code: int, response_text: str) -> BlockType:
        """HTTP 응답에서 차단 유형을 감지합니다."""
        if status_code == 429:
            return BlockType.RATE_LIMIT
        if status_code == 403:
            return BlockType.FORBIDDEN
        if status_code == 503:
            return BlockType.SERVER_ERROR
        if status_code == 200:
            text_lower = response_text[:5000].lower()
            # CAPTCHA 감지
            if any(kw in text_lower for kw in
                   ["captcha", "recaptcha", "hcaptcha", "challenge-platform"]):
                return BlockType.CAPTCHA
            # Skeleton/빈 페이지 감지
            if any(kw in text_lower for kw in
                   ["pardon our interruption", "access denied"]):
                return BlockType.FORBIDDEN
            # Airbnb API 응답이 비정상적으로 짧은 경우
            if len(response_text) < 100 and "error" not in text_lower:
                return BlockType.SKELETON
        return BlockType.NONE

    def get_stats(self) -> dict:
        """현재 통계를 반환합니다."""
        return {
            "total": self._stats.total,
            "success": self._stats.success,
            "failed": self._stats.failed,
            "blocked": self._stats.blocked,
            "consecutive_failures": self._stats.consecutive_failures,
            "hourly_count": self._stats.hourly_count,
            "daily_count": self._stats.daily_count,
            "delay_multiplier": round(self._current_delay_multiplier, 2),
            "circuit_open": self._circuit_open,
        }
