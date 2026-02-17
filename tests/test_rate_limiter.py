"""
Tests for crawler/rate_limiter.py

Covers:
- BlockType enum values
- RequestStats dataclass: creation, reset_hourly, reset_daily
- RateLimiter.__init__ with custom params
- RateLimiter.from_config() with mocked tier config
- RateLimiter.wait() with small delays
- RateLimiter.report_success() and report_failure()
- RateLimiter.detect_block() for all block types
- RateLimiter.get_stats()
"""

import asyncio
import time
from unittest.mock import patch, MagicMock

import pytest

from crawler.rate_limiter import BlockType, RateLimiter, RequestStats


# ─── BlockType enum ──────────────────────────────────────────────────

class TestBlockType:
    """BlockType enum value tests."""

    def test_none_value(self):
        assert BlockType.NONE.value == "none"

    def test_rate_limit_value(self):
        assert BlockType.RATE_LIMIT.value == "rate_limit"

    def test_forbidden_value(self):
        assert BlockType.FORBIDDEN.value == "forbidden"

    def test_captcha_value(self):
        assert BlockType.CAPTCHA.value == "captcha"

    def test_skeleton_value(self):
        assert BlockType.SKELETON.value == "skeleton"

    def test_server_error_value(self):
        assert BlockType.SERVER_ERROR.value == "server_error"

    def test_all_members(self):
        names = {bt.name for bt in BlockType}
        assert names == {"NONE", "RATE_LIMIT", "FORBIDDEN", "CAPTCHA", "SKELETON", "SERVER_ERROR"}


# ─── RequestStats dataclass ──────────────────────────────────────────

class TestRequestStats:
    """RequestStats dataclass tests."""

    def test_creation_defaults(self):
        stats = RequestStats()
        assert stats.total == 0
        assert stats.success == 0
        assert stats.failed == 0
        assert stats.blocked == 0
        assert stats.consecutive_failures == 0
        assert stats.hourly_count == 0
        assert stats.daily_count == 0
        assert isinstance(stats.hour_start, float)
        assert isinstance(stats.day_start, float)

    def test_creation_custom_values(self):
        stats = RequestStats(total=10, success=8, failed=2, blocked=1)
        assert stats.total == 10
        assert stats.success == 8
        assert stats.failed == 2
        assert stats.blocked == 1

    def test_reset_hourly(self):
        stats = RequestStats(hourly_count=50)
        old_hour_start = stats.hour_start
        time.sleep(0.01)
        stats.reset_hourly()
        assert stats.hourly_count == 0
        assert stats.hour_start > old_hour_start

    def test_reset_daily(self):
        stats = RequestStats(daily_count=800)
        old_day_start = stats.day_start
        time.sleep(0.01)
        stats.reset_daily()
        assert stats.daily_count == 0
        assert stats.day_start > old_day_start

    def test_reset_hourly_does_not_affect_daily(self):
        stats = RequestStats(hourly_count=50, daily_count=100)
        stats.reset_hourly()
        assert stats.hourly_count == 0
        assert stats.daily_count == 100

    def test_reset_daily_does_not_affect_hourly(self):
        stats = RequestStats(hourly_count=50, daily_count=100)
        stats.reset_daily()
        assert stats.daily_count == 0
        assert stats.hourly_count == 50


# ─── RateLimiter.__init__ ────────────────────────────────────────────

class TestRateLimiterInit:
    """RateLimiter initialization tests."""

    def test_default_params(self):
        rl = RateLimiter()
        assert rl._delay_base == 7.0
        assert rl._delay_jitter == (2.0, 8.0)
        assert rl._max_per_hour == 50
        assert rl._daily_limit == 800
        assert rl._current_delay_multiplier == 1.0
        assert rl._circuit_open is False
        assert rl._circuit_open_until == 0.0
        assert rl._half_open_count == 0

    def test_custom_params(self):
        rl = RateLimiter(
            delay_base=2.0,
            delay_jitter=(0.5, 1.5),
            max_requests_per_hour=100,
            daily_limit=500,
        )
        assert rl._delay_base == 2.0
        assert rl._delay_jitter == (0.5, 1.5)
        assert rl._max_per_hour == 100
        assert rl._daily_limit == 500

    def test_stats_initialized(self):
        rl = RateLimiter()
        assert isinstance(rl._stats, RequestStats)
        assert rl._stats.total == 0


# ─── RateLimiter.from_config() ───────────────────────────────────────

class TestRateLimiterFromConfig:
    """RateLimiter.from_config() tests with mocked tier config."""

    @patch("crawler.rate_limiter.RateLimiter.__init__", return_value=None)
    def test_from_config_calls_init_with_tier_values(self, mock_init):
        tier_cfg = {
            "delay_base": 5.0,
            "delay_jitter": (1.0, 5.0),
            "max_requests_per_hour": 80,
            "daily_limit_per_ip": 600,
        }
        with patch("config.settings.get_tier_config", return_value=tier_cfg):
            RateLimiter.from_config()
        mock_init.assert_called_once_with(
            delay_base=5.0,
            delay_jitter=(1.0, 5.0),
            max_requests_per_hour=80,
            daily_limit=600,
        )

    def test_from_config_returns_rate_limiter_instance(self):
        tier_cfg = {
            "delay_base": 4.0,
            "delay_jitter": (1.0, 4.0),
            "max_requests_per_hour": 100,
            "daily_limit_per_ip": 500,
        }
        with patch("config.settings.get_tier_config", return_value=tier_cfg):
            rl = RateLimiter.from_config()
        assert isinstance(rl, RateLimiter)
        assert rl._delay_base == 4.0
        assert rl._max_per_hour == 100


# ─── RateLimiter.wait() ──────────────────────────────────────────────

class TestRateLimiterWait:
    """RateLimiter.wait() tests with minimal delays for speed."""

    @pytest.fixture
    def fast_limiter(self):
        """A rate limiter with very small delays for fast tests."""
        return RateLimiter(
            delay_base=0.001,
            delay_jitter=(0.0, 0.001),
            max_requests_per_hour=1000,
            daily_limit=10000,
        )

    async def test_wait_increments_counters(self, fast_limiter):
        await fast_limiter.wait()
        assert fast_limiter._stats.total == 1
        assert fast_limiter._stats.hourly_count == 1
        assert fast_limiter._stats.daily_count == 1

    async def test_wait_increments_counters_multiple(self, fast_limiter):
        await fast_limiter.wait()
        await fast_limiter.wait()
        await fast_limiter.wait()
        assert fast_limiter._stats.total == 3
        assert fast_limiter._stats.hourly_count == 3
        assert fast_limiter._stats.daily_count == 3

    async def test_wait_does_not_block_long(self, fast_limiter):
        start = time.time()
        await fast_limiter.wait()
        elapsed = time.time() - start
        # Should be very fast with tiny delay_base/jitter
        assert elapsed < 1.0

    async def test_wait_hourly_limit_resets_after_hour(self, fast_limiter):
        """When hourly limit is reached but hour has elapsed, resets counter."""
        fast_limiter._max_per_hour = 2
        fast_limiter._stats.hourly_count = 2
        # Set hour_start to more than 1 hour ago
        fast_limiter._stats.hour_start = time.time() - 3700
        await fast_limiter.wait()
        # Counter should have been reset, then incremented
        assert fast_limiter._stats.hourly_count == 1

    async def test_wait_daily_limit_resets_after_day(self, fast_limiter):
        """When daily limit is reached but day has elapsed, resets counter."""
        fast_limiter._daily_limit = 2
        fast_limiter._stats.daily_count = 2
        # Set day_start to more than 1 day ago
        fast_limiter._stats.day_start = time.time() - 86500
        await fast_limiter.wait()
        # Counter should have been reset, then incremented
        assert fast_limiter._stats.daily_count == 1

    async def test_wait_circuit_breaker_closed_after_wait(self, fast_limiter):
        """When circuit breaker is open but time has passed, it closes."""
        fast_limiter._circuit_open = True
        fast_limiter._circuit_open_until = time.time() - 1  # Already past
        await fast_limiter.wait()
        assert fast_limiter._circuit_open is False
        assert fast_limiter._half_open_count == 0

    async def test_wait_circuit_breaker_open_waits(self):
        """When circuit breaker is open with small remaining time, waits."""
        rl = RateLimiter(
            delay_base=0.001,
            delay_jitter=(0.0, 0.001),
            max_requests_per_hour=1000,
            daily_limit=10000,
        )
        rl._circuit_open = True
        rl._circuit_open_until = time.time() + 0.05  # Very short wait
        start = time.time()
        await rl.wait()
        elapsed = time.time() - start
        # Should have waited at least the circuit breaker time
        assert elapsed >= 0.04
        assert rl._circuit_open is False

    async def test_wait_delay_multiplier_increases_delay(self):
        """Higher delay multiplier should increase wait time."""
        rl = RateLimiter(
            delay_base=0.01,
            delay_jitter=(0.0, 0.001),
            max_requests_per_hour=1000,
            daily_limit=10000,
        )
        rl._current_delay_multiplier = 3.0
        start = time.time()
        await rl.wait()
        elapsed = time.time() - start
        # With 3x multiplier, delay should be at least 0.01*3 = 0.03
        assert elapsed >= 0.02


# ─── RateLimiter.report_success() ────────────────────────────────────

class TestRateLimiterReportSuccess:
    """RateLimiter.report_success() tests."""

    def test_increments_success_count(self):
        rl = RateLimiter()
        rl.report_success()
        assert rl._stats.success == 1

    def test_resets_consecutive_failures(self):
        rl = RateLimiter()
        rl._stats.consecutive_failures = 3
        rl.report_success()
        assert rl._stats.consecutive_failures == 0

    def test_reduces_delay_multiplier(self):
        rl = RateLimiter()
        rl._current_delay_multiplier = 2.0
        rl.report_success()
        assert rl._current_delay_multiplier == pytest.approx(2.0 * 0.9)

    def test_delay_multiplier_does_not_go_below_one(self):
        rl = RateLimiter()
        rl._current_delay_multiplier = 1.05
        rl.report_success()
        # 1.05 * 0.9 = 0.945, should be clamped to 1.0
        assert rl._current_delay_multiplier == 1.0

    def test_delay_multiplier_stays_at_one_if_normal(self):
        rl = RateLimiter()
        rl._current_delay_multiplier = 1.0
        rl.report_success()
        assert rl._current_delay_multiplier == 1.0

    def test_half_open_circuit_increments(self):
        rl = RateLimiter()
        # CB_HALF_OPEN_REQUESTS is 2, so set to a value where incrementing
        # does NOT yet reach the threshold
        # With threshold=2: count=1 -> increments to 2 -> hits threshold -> resets to 0
        # So we need a higher threshold to test simple increment behavior
        original_threshold = RateLimiter.CB_HALF_OPEN_REQUESTS
        RateLimiter.CB_HALF_OPEN_REQUESTS = 10  # Temporarily raise threshold
        try:
            rl._half_open_count = 1
            rl.report_success()
            assert rl._half_open_count == 2
        finally:
            RateLimiter.CB_HALF_OPEN_REQUESTS = original_threshold

    def test_half_open_circuit_closes_at_threshold(self):
        rl = RateLimiter()
        rl._half_open_count = RateLimiter.CB_HALF_OPEN_REQUESTS - 1
        rl.report_success()
        # Should close circuit (reset half_open_count to 0)
        assert rl._half_open_count == 0

    def test_half_open_zero_means_no_increment(self):
        rl = RateLimiter()
        rl._half_open_count = 0
        rl.report_success()
        assert rl._half_open_count == 0


# ─── RateLimiter.report_failure() ────────────────────────────────────

class TestRateLimiterReportFailure:
    """RateLimiter.report_failure() tests."""

    def test_increments_failed_count(self):
        rl = RateLimiter()
        rl.report_failure()
        assert rl._stats.failed == 1

    def test_increments_consecutive_failures(self):
        rl = RateLimiter()
        rl.report_failure()
        rl.report_failure()
        assert rl._stats.consecutive_failures == 2

    def test_no_block_type_does_not_change_blocked_or_multiplier(self):
        rl = RateLimiter()
        original_multiplier = rl._current_delay_multiplier
        rl.report_failure(BlockType.NONE)
        assert rl._stats.blocked == 0
        assert rl._current_delay_multiplier == original_multiplier

    def test_rate_limit_doubles_multiplier(self):
        rl = RateLimiter()
        rl.report_failure(BlockType.RATE_LIMIT)
        assert rl._stats.blocked == 1
        assert rl._current_delay_multiplier == pytest.approx(2.0)

    def test_forbidden_triples_multiplier(self):
        rl = RateLimiter()
        rl.report_failure(BlockType.FORBIDDEN)
        assert rl._stats.blocked == 1
        assert rl._current_delay_multiplier == pytest.approx(3.0)

    def test_captcha_quadruples_multiplier(self):
        rl = RateLimiter()
        rl.report_failure(BlockType.CAPTCHA)
        assert rl._stats.blocked == 1
        assert rl._current_delay_multiplier == pytest.approx(4.0)

    def test_server_error_multiplier_1_5x(self):
        rl = RateLimiter()
        rl.report_failure(BlockType.SERVER_ERROR)
        assert rl._stats.blocked == 1
        assert rl._current_delay_multiplier == pytest.approx(1.5)

    def test_skeleton_multiplier_1_5x(self):
        rl = RateLimiter()
        rl.report_failure(BlockType.SKELETON)
        assert rl._stats.blocked == 1
        assert rl._current_delay_multiplier == pytest.approx(1.5)

    def test_multiplier_capped_at_10(self):
        rl = RateLimiter()
        rl._current_delay_multiplier = 5.0
        rl.report_failure(BlockType.CAPTCHA)  # 5.0 * 4.0 = 20.0, capped at 10
        assert rl._current_delay_multiplier == 10.0

    def test_circuit_breaker_triggers_at_threshold(self):
        rl = RateLimiter()
        for _ in range(RateLimiter.CB_FAILURE_THRESHOLD):
            rl.report_failure()
        assert rl._circuit_open is True
        assert rl._circuit_open_until > time.time()
        # consecutive_failures reset after circuit opens
        assert rl._stats.consecutive_failures == 0

    def test_circuit_breaker_does_not_trigger_below_threshold(self):
        rl = RateLimiter()
        for _ in range(RateLimiter.CB_FAILURE_THRESHOLD - 1):
            rl.report_failure()
        assert rl._circuit_open is False

    def test_circuit_breaker_open_duration(self):
        rl = RateLimiter()
        before = time.time()
        for _ in range(RateLimiter.CB_FAILURE_THRESHOLD):
            rl.report_failure()
        assert rl._circuit_open_until >= before + RateLimiter.CB_OPEN_DURATION


# ─── RateLimiter.detect_block() ──────────────────────────────────────

class TestRateLimiterDetectBlock:
    """RateLimiter.detect_block() tests for all block types."""

    def test_429_returns_rate_limit(self):
        assert RateLimiter.detect_block(429, "") == BlockType.RATE_LIMIT

    def test_403_returns_forbidden(self):
        assert RateLimiter.detect_block(403, "") == BlockType.FORBIDDEN

    def test_503_returns_server_error(self):
        assert RateLimiter.detect_block(503, "") == BlockType.SERVER_ERROR

    def test_200_with_captcha_keyword(self):
        assert RateLimiter.detect_block(200, "Please solve the captcha") == BlockType.CAPTCHA

    def test_200_with_recaptcha_keyword(self):
        assert RateLimiter.detect_block(200, "<div class='recaptcha'></div>") == BlockType.CAPTCHA

    def test_200_with_hcaptcha_keyword(self):
        assert RateLimiter.detect_block(200, "hcaptcha verification") == BlockType.CAPTCHA

    def test_200_with_challenge_platform(self):
        assert RateLimiter.detect_block(200, "challenge-platform enabled") == BlockType.CAPTCHA

    def test_200_with_pardon_our_interruption(self):
        assert RateLimiter.detect_block(200, "Pardon our interruption") == BlockType.FORBIDDEN

    def test_200_with_access_denied(self):
        assert RateLimiter.detect_block(200, "Access Denied - you do not have permission") == BlockType.FORBIDDEN

    def test_200_short_response_is_skeleton(self):
        # Short text (< 100 chars) without "error" => SKELETON
        assert RateLimiter.detect_block(200, "{}") == BlockType.SKELETON

    def test_200_short_response_with_error_not_skeleton(self):
        # Short text with "error" should NOT be skeleton
        result = RateLimiter.detect_block(200, '{"error": "something went wrong"}')
        assert result == BlockType.NONE

    def test_200_normal_response_is_none(self):
        normal_text = '{"data": {"results": [' + '"x" ,' * 50 + '"x"]}}'
        assert RateLimiter.detect_block(200, normal_text) == BlockType.NONE

    def test_200_empty_string_is_skeleton(self):
        assert RateLimiter.detect_block(200, "") == BlockType.SKELETON

    def test_other_status_code_is_none(self):
        assert RateLimiter.detect_block(500, "Internal Server Error") == BlockType.NONE
        assert RateLimiter.detect_block(404, "Not Found") == BlockType.NONE
        assert RateLimiter.detect_block(301, "Moved") == BlockType.NONE

    def test_captcha_detection_case_insensitive(self):
        assert RateLimiter.detect_block(200, "CAPTCHA required") == BlockType.CAPTCHA

    def test_only_first_5000_chars_checked(self):
        # CAPTCHA keyword after 5000 chars should NOT be detected
        text = "x" * 5001 + "captcha"
        assert RateLimiter.detect_block(200, text) == BlockType.NONE


# ─── RateLimiter.get_stats() ─────────────────────────────────────────

class TestRateLimiterGetStats:
    """RateLimiter.get_stats() tests."""

    def test_initial_stats(self):
        rl = RateLimiter()
        stats = rl.get_stats()
        assert stats["total"] == 0
        assert stats["success"] == 0
        assert stats["failed"] == 0
        assert stats["blocked"] == 0
        assert stats["consecutive_failures"] == 0
        assert stats["hourly_count"] == 0
        assert stats["daily_count"] == 0
        assert stats["delay_multiplier"] == 1.0
        assert stats["circuit_open"] is False

    def test_stats_after_operations(self):
        rl = RateLimiter()
        rl._stats.total = 10
        rl._stats.success = 7
        rl._stats.failed = 3
        rl._stats.blocked = 2
        rl._stats.consecutive_failures = 1
        rl._stats.hourly_count = 10
        rl._stats.daily_count = 10
        rl._current_delay_multiplier = 2.567
        rl._circuit_open = True

        stats = rl.get_stats()
        assert stats["total"] == 10
        assert stats["success"] == 7
        assert stats["failed"] == 3
        assert stats["blocked"] == 2
        assert stats["consecutive_failures"] == 1
        assert stats["hourly_count"] == 10
        assert stats["daily_count"] == 10
        assert stats["delay_multiplier"] == 2.57  # Rounded to 2 decimals
        assert stats["circuit_open"] is True

    def test_stats_returns_dict(self):
        rl = RateLimiter()
        stats = rl.get_stats()
        assert isinstance(stats, dict)
        expected_keys = {
            "total", "success", "failed", "blocked",
            "consecutive_failures", "hourly_count", "daily_count",
            "delay_multiplier", "circuit_open",
        }
        assert set(stats.keys()) == expected_keys


# ─── RateLimiter.wait() hourly/daily limit paths ────────────────────

class TestRateLimiterWaitLimits:
    """wait() 메서드의 시간당/일일 한도 대기 경로 테스트."""

    async def test_hourly_limit_triggers_wait(self):
        """시간당 한도 도달 시 대기 후 카운터를 리셋한다."""
        rl = RateLimiter(delay_base=0.0, delay_jitter=(0.0, 0.0),
                         max_requests_per_hour=2, daily_limit=9999)
        rl._stats.hourly_count = 2
        rl._stats.hour_start = time.time() - 100  # 100초 전 (아직 1시간 미만)

        with patch("crawler.rate_limiter.asyncio.sleep") as mock_sleep:
            mock_sleep.return_value = None
            await rl.wait()

        # asyncio.sleep이 호출되었어야 함 (한도 대기 + 일반 딜레이)
        assert mock_sleep.call_count >= 2
        # 한도 대기 후 리셋되어야 함
        assert rl._stats.hourly_count == 1  # reset 후 +1

    async def test_daily_limit_triggers_wait(self):
        """일일 한도 도달 시 대기 후 카운터를 리셋한다."""
        rl = RateLimiter(delay_base=0.0, delay_jitter=(0.0, 0.0),
                         max_requests_per_hour=9999, daily_limit=2)
        rl._stats.daily_count = 2
        rl._stats.day_start = time.time() - 100  # 100초 전 (아직 1일 미만)

        with patch("crawler.rate_limiter.asyncio.sleep") as mock_sleep:
            mock_sleep.return_value = None
            await rl.wait()

        assert mock_sleep.call_count >= 2
        assert rl._stats.daily_count == 1  # reset 후 +1
