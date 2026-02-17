"""
Tests for crawler/proxy_manager.py

Covers:
- ProxyState dataclass: creation, reset_count, mark_blocked, is_available
- ProxyManager.__init__ with no proxies, with proxies
- ProxyManager.from_config() with mocked config settings
- ProxyManager.has_proxies and available_count properties
- ProxyManager.get_proxy() - various scenarios
- ProxyManager.report_success() and report_blocked()
- ProxyManager.get_stats()
- ProxyManager._rotate() wraps around
"""

import time
from unittest.mock import patch, MagicMock

import pytest

from crawler.proxy_manager import ProxyManager, ProxyState


# ─── ProxyState dataclass ────────────────────────────────────────────

class TestProxyState:
    """ProxyState dataclass tests."""

    def test_creation_defaults(self):
        ps = ProxyState(url="http://proxy1:8080")
        assert ps.url == "http://proxy1:8080"
        assert ps.request_count == 0
        assert ps.total_requests == 0
        assert ps.blocked_count == 0
        assert ps.last_used == 0.0
        assert ps.cooldown_until == 0.0
        assert ps.is_healthy is True

    def test_creation_custom_values(self):
        ps = ProxyState(
            url="http://proxy2:9090",
            request_count=5,
            total_requests=100,
            blocked_count=2,
            is_healthy=False,
        )
        assert ps.url == "http://proxy2:9090"
        assert ps.request_count == 5
        assert ps.total_requests == 100
        assert ps.blocked_count == 2
        assert ps.is_healthy is False

    def test_reset_count(self):
        ps = ProxyState(url="http://proxy1:8080", request_count=30)
        ps.reset_count()
        assert ps.request_count == 0

    def test_reset_count_does_not_affect_total(self):
        ps = ProxyState(url="http://proxy1:8080", request_count=30, total_requests=150)
        ps.reset_count()
        assert ps.request_count == 0
        assert ps.total_requests == 150

    def test_mark_blocked_default_cooldown(self):
        ps = ProxyState(url="http://proxy1:8080")
        before = time.time()
        ps.mark_blocked()
        assert ps.blocked_count == 1
        assert ps.is_healthy is False
        assert ps.cooldown_until >= before + 300

    def test_mark_blocked_custom_cooldown(self):
        ps = ProxyState(url="http://proxy1:8080")
        before = time.time()
        ps.mark_blocked(cooldown_seconds=60)
        assert ps.cooldown_until >= before + 60
        assert ps.cooldown_until < before + 300

    def test_mark_blocked_increments_count(self):
        ps = ProxyState(url="http://proxy1:8080")
        ps.mark_blocked()
        ps.mark_blocked()
        assert ps.blocked_count == 2

    def test_is_available_when_healthy(self):
        ps = ProxyState(url="http://proxy1:8080", is_healthy=True, cooldown_until=0.0)
        assert ps.is_available() is True

    def test_is_available_when_blocked_but_cooldown_expired(self):
        ps = ProxyState(url="http://proxy1:8080", is_healthy=False)
        ps.cooldown_until = time.time() - 10  # Expired
        assert ps.is_available() is True
        # Should also restore is_healthy
        assert ps.is_healthy is True

    def test_is_available_when_blocked_and_cooldown_active(self):
        ps = ProxyState(url="http://proxy1:8080", is_healthy=False)
        ps.cooldown_until = time.time() + 300  # Still active
        assert ps.is_available() is False


# ─── ProxyManager.__init__ ───────────────────────────────────────────

class TestProxyManagerInit:
    """ProxyManager initialization tests."""

    def test_no_proxies(self):
        pm = ProxyManager()
        assert pm._proxies == []
        assert pm._current_index == 0

    def test_none_proxies(self):
        pm = ProxyManager(proxy_urls=None)
        assert pm._proxies == []

    def test_empty_list(self):
        pm = ProxyManager(proxy_urls=[])
        assert pm._proxies == []

    def test_with_proxies(self):
        urls = ["http://p1:8080", "http://p2:8080", "http://p3:8080"]
        pm = ProxyManager(proxy_urls=urls)
        assert len(pm._proxies) == 3
        assert pm._proxies[0].url == "http://p1:8080"
        assert pm._proxies[1].url == "http://p2:8080"
        assert pm._proxies[2].url == "http://p3:8080"

    def test_strips_whitespace_from_urls(self):
        urls = ["  http://p1:8080  ", " http://p2:8080\n"]
        pm = ProxyManager(proxy_urls=urls)
        assert len(pm._proxies) == 2
        assert pm._proxies[0].url == "http://p1:8080"
        assert pm._proxies[1].url == "http://p2:8080"

    def test_skips_empty_strings(self):
        urls = ["http://p1:8080", "", "   ", "http://p2:8080"]
        pm = ProxyManager(proxy_urls=urls)
        assert len(pm._proxies) == 2

    def test_custom_requests_per_rotate(self):
        pm = ProxyManager(proxy_urls=["http://p1:8080"], requests_per_rotate=10)
        assert pm._requests_per_rotate == 10

    def test_custom_block_cooldown(self):
        pm = ProxyManager(proxy_urls=["http://p1:8080"], block_cooldown=60)
        assert pm._block_cooldown == 60


# ─── ProxyManager.from_config() ──────────────────────────────────────

class TestProxyManagerFromConfig:
    """ProxyManager.from_config() tests with mocked config."""

    @patch("crawler.proxy_manager.Path")
    def test_from_config_with_env_proxies(self, mock_path_cls):
        tier_cfg = {
            "proxy_required": True,
            "requests_per_ip_before_rotate": 25,
        }
        mock_path_instance = MagicMock()
        mock_path_instance.exists.return_value = False
        mock_path_cls.return_value = mock_path_instance

        with patch("config.settings.get_tier_config", return_value=tier_cfg), \
             patch("config.settings.PROXY_LIST_ENV", "http://a:1,http://b:2"), \
             patch("config.settings.PROXY_LIST_FILE", "/fake/proxies.txt"):
            pm = ProxyManager.from_config()

        assert len(pm._proxies) == 2
        assert pm._requests_per_rotate == 25

    @patch("crawler.proxy_manager.Path")
    def test_from_config_no_proxies_no_env(self, mock_path_cls):
        tier_cfg = {
            "proxy_required": False,
            "requests_per_ip_before_rotate": 50,
        }
        mock_path_instance = MagicMock()
        mock_path_instance.exists.return_value = False
        mock_path_cls.return_value = mock_path_instance

        with patch("config.settings.get_tier_config", return_value=tier_cfg), \
             patch("config.settings.PROXY_LIST_ENV", ""), \
             patch("config.settings.PROXY_LIST_FILE", "/fake/proxies.txt"):
            pm = ProxyManager.from_config()

        assert len(pm._proxies) == 0

    @patch("builtins.open")
    @patch("crawler.proxy_manager.Path")
    def test_from_config_with_file_proxies(self, mock_path_cls, mock_open):
        tier_cfg = {
            "proxy_required": True,
            "requests_per_ip_before_rotate": 30,
        }
        mock_path_instance = MagicMock()
        mock_path_instance.exists.return_value = True
        mock_path_cls.return_value = mock_path_instance

        # Simulate file content
        mock_open.return_value.__enter__ = MagicMock(return_value=iter([
            "http://file-proxy1:1234\n",
            "# comment line\n",
            "http://file-proxy2:5678\n",
            "\n",
        ]))
        mock_open.return_value.__exit__ = MagicMock(return_value=False)

        with patch("config.settings.get_tier_config", return_value=tier_cfg), \
             patch("config.settings.PROXY_LIST_ENV", ""), \
             patch("config.settings.PROXY_LIST_FILE", "/fake/proxies.txt"):
            pm = ProxyManager.from_config()

        assert len(pm._proxies) == 2


# ─── ProxyManager properties ─────────────────────────────────────────

class TestProxyManagerProperties:
    """Tests for has_proxies and available_count properties."""

    def test_has_proxies_true(self):
        pm = ProxyManager(proxy_urls=["http://p1:8080"])
        assert pm.has_proxies is True

    def test_has_proxies_false(self):
        pm = ProxyManager()
        assert pm.has_proxies is False

    def test_available_count_all_healthy(self):
        pm = ProxyManager(proxy_urls=["http://p1:8080", "http://p2:8080", "http://p3:8080"])
        assert pm.available_count == 3

    def test_available_count_some_blocked(self):
        pm = ProxyManager(proxy_urls=["http://p1:8080", "http://p2:8080", "http://p3:8080"])
        pm._proxies[1].is_healthy = False
        pm._proxies[1].cooldown_until = time.time() + 300
        assert pm.available_count == 2

    def test_available_count_all_blocked(self):
        pm = ProxyManager(proxy_urls=["http://p1:8080", "http://p2:8080"])
        for p in pm._proxies:
            p.is_healthy = False
            p.cooldown_until = time.time() + 300
        assert pm.available_count == 0

    def test_available_count_no_proxies(self):
        pm = ProxyManager()
        assert pm.available_count == 0


# ─── ProxyManager.get_proxy() ────────────────────────────────────────

class TestProxyManagerGetProxy:
    """ProxyManager.get_proxy() tests."""

    def test_no_proxies_returns_none(self):
        pm = ProxyManager()
        assert pm.get_proxy() is None

    def test_returns_first_proxy(self):
        pm = ProxyManager(proxy_urls=["http://p1:8080", "http://p2:8080"])
        proxy = pm.get_proxy()
        assert proxy == "http://p1:8080"

    def test_increments_request_count(self):
        pm = ProxyManager(proxy_urls=["http://p1:8080"])
        pm.get_proxy()
        assert pm._proxies[0].request_count == 1
        assert pm._proxies[0].total_requests == 1

    def test_sets_last_used(self):
        pm = ProxyManager(proxy_urls=["http://p1:8080"])
        before = time.time()
        pm.get_proxy()
        assert pm._proxies[0].last_used >= before

    def test_rotation_after_n_requests(self):
        pm = ProxyManager(
            proxy_urls=["http://p1:8080", "http://p2:8080"],
            requests_per_rotate=3,
        )
        # Use proxy 1 for 3 requests (hits limit)
        pm._proxies[0].request_count = 3

        proxy = pm.get_proxy()
        # Should have rotated to p2 after reset
        assert proxy == "http://p2:8080"

    def test_skips_blocked_proxies(self):
        pm = ProxyManager(
            proxy_urls=["http://p1:8080", "http://p2:8080", "http://p3:8080"],
        )
        pm._proxies[0].is_healthy = False
        pm._proxies[0].cooldown_until = time.time() + 300

        proxy = pm.get_proxy()
        assert proxy == "http://p2:8080"

    def test_all_blocked_returns_none(self):
        pm = ProxyManager(proxy_urls=["http://p1:8080", "http://p2:8080"])
        for p in pm._proxies:
            p.is_healthy = False
            p.cooldown_until = time.time() + 300

        assert pm.get_proxy() is None

    def test_wraps_around_to_first_proxy(self):
        pm = ProxyManager(
            proxy_urls=["http://p1:8080", "http://p2:8080"],
            requests_per_rotate=2,
        )
        # Position at last proxy with count at limit
        pm._current_index = 1
        pm._proxies[1].request_count = 2

        proxy = pm.get_proxy()
        # Should wrap around to p1
        assert proxy == "http://p1:8080"

    def test_multiple_gets_stay_on_same_proxy(self):
        pm = ProxyManager(
            proxy_urls=["http://p1:8080", "http://p2:8080"],
            requests_per_rotate=10,
        )
        proxy1 = pm.get_proxy()
        proxy2 = pm.get_proxy()
        proxy3 = pm.get_proxy()
        assert proxy1 == proxy2 == proxy3 == "http://p1:8080"
        assert pm._proxies[0].request_count == 3


# ─── ProxyManager.report_success() ───────────────────────────────────

class TestProxyManagerReportSuccess:
    """ProxyManager.report_success() tests."""

    def test_marks_current_proxy_healthy(self):
        pm = ProxyManager(proxy_urls=["http://p1:8080"])
        pm._proxies[0].is_healthy = False
        pm.report_success()
        assert pm._proxies[0].is_healthy is True

    def test_no_proxies_no_error(self):
        pm = ProxyManager()
        pm.report_success()  # Should not raise


# ─── ProxyManager.report_blocked() ───────────────────────────────────

class TestProxyManagerReportBlocked:
    """ProxyManager.report_blocked() tests."""

    def test_marks_current_proxy_blocked(self):
        pm = ProxyManager(
            proxy_urls=["http://p1:8080", "http://p2:8080"],
            block_cooldown=120,
        )
        pm.report_blocked()
        assert pm._proxies[0].is_healthy is False
        assert pm._proxies[0].blocked_count == 1
        assert pm._proxies[0].cooldown_until > time.time()

    def test_rotates_to_next_proxy(self):
        pm = ProxyManager(proxy_urls=["http://p1:8080", "http://p2:8080"])
        pm.report_blocked()
        assert pm._current_index == 1

    def test_no_proxies_no_error(self):
        pm = ProxyManager()
        pm.report_blocked()  # Should not raise


# ─── ProxyManager.get_stats() ────────────────────────────────────────

class TestProxyManagerGetStats:
    """ProxyManager.get_stats() tests."""

    def test_empty_pool_stats(self):
        pm = ProxyManager()
        stats = pm.get_stats()
        assert stats["total"] == 0
        assert stats["available"] == 0
        assert stats["proxies"] == []

    def test_populated_pool_stats(self):
        pm = ProxyManager(proxy_urls=["http://p1:8080", "http://p2:8080"])
        pm._proxies[0].total_requests = 50
        pm._proxies[0].blocked_count = 1
        pm._proxies[1].total_requests = 30

        stats = pm.get_stats()
        assert stats["total"] == 2
        assert stats["available"] == 2
        assert len(stats["proxies"]) == 2
        assert stats["proxies"][0]["index"] == 0
        assert stats["proxies"][0]["requests"] == 50
        assert stats["proxies"][0]["blocked"] == 1
        assert stats["proxies"][0]["healthy"] is True
        assert stats["proxies"][1]["index"] == 1
        assert stats["proxies"][1]["requests"] == 30

    def test_stats_reflects_blocked_proxies(self):
        pm = ProxyManager(proxy_urls=["http://p1:8080", "http://p2:8080"])
        pm._proxies[0].is_healthy = False
        pm._proxies[0].cooldown_until = time.time() + 300
        stats = pm.get_stats()
        assert stats["available"] == 1
        assert stats["proxies"][0]["healthy"] is False


# ─── ProxyManager._rotate() ─────────────────────────────────────────

class TestProxyManagerRotate:
    """ProxyManager._rotate() tests."""

    def test_rotate_increments_index(self):
        pm = ProxyManager(proxy_urls=["http://p1:8080", "http://p2:8080", "http://p3:8080"])
        assert pm._current_index == 0
        pm._rotate()
        assert pm._current_index == 1
        pm._rotate()
        assert pm._current_index == 2

    def test_rotate_wraps_around(self):
        pm = ProxyManager(proxy_urls=["http://p1:8080", "http://p2:8080"])
        pm._current_index = 1
        pm._rotate()
        assert pm._current_index == 0

    def test_rotate_single_proxy(self):
        pm = ProxyManager(proxy_urls=["http://p1:8080"])
        pm._rotate()
        assert pm._current_index == 0

    def test_rotate_no_proxies(self):
        pm = ProxyManager()
        pm._rotate()  # Should not raise
        assert pm._current_index == 0


# ─── proxy_required 경고 테스트 ────────────────────────────────────────

class TestProxyManagerWarning:
    """proxy_required=True but no proxies → warning (line 99)."""

    def test_proxy_required_warning(self):
        """proxy가 필수인 tier인데 프록시가 없으면 경고를 출력한다."""
        tier_b = {
            "proxy_required": True,
            "requests_per_ip_before_rotate": 50,
            "max_requests_per_hour": 200,
            "daily_limit": 5000,
        }
        with patch("config.settings.get_tier_config", return_value=tier_b), \
             patch("config.settings.PROXY_LIST_ENV", ""), \
             patch("config.settings.PROXY_LIST_FILE", "/nonexistent/proxies.txt"), \
             patch("crawler.proxy_manager.logger") as mock_logger:
            pm = ProxyManager.from_config()
            mock_logger.warning.assert_called_once()
            assert pm._proxies == []
