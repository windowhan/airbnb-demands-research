"""config/settings.py 단위 테스트."""

from unittest.mock import patch

import pytest


class TestGetTierConfig:
    """get_tier_config 함수 테스트."""

    def test_tier_a(self):
        with patch("config.settings.CRAWL_TIER", "A"):
            from config.settings import get_tier_config, TIER_CONFIG
            result = get_tier_config()
        assert result == TIER_CONFIG["A"]
        assert result["proxy_required"] is False

    def test_tier_b(self):
        with patch("config.settings.CRAWL_TIER", "B"):
            from config.settings import get_tier_config, TIER_CONFIG
            result = get_tier_config()
        assert result == TIER_CONFIG["B"]
        assert result["proxy_required"] is True

    def test_tier_c(self):
        with patch("config.settings.CRAWL_TIER", "C"):
            from config.settings import get_tier_config, TIER_CONFIG
            result = get_tier_config()
        assert result == TIER_CONFIG["C"]
        assert result["listing_detail_enabled"] is True

    def test_invalid_tier_raises(self):
        with patch("config.settings.CRAWL_TIER", "Z"):
            from config.settings import get_tier_config
            with pytest.raises(ValueError, match="Unknown CRAWL_TIER"):
                get_tier_config()


class TestSettingsConstants:
    """설정 상수 테스트."""

    def test_tier_config_has_all_tiers(self):
        from config.settings import TIER_CONFIG
        assert "A" in TIER_CONFIG
        assert "B" in TIER_CONFIG
        assert "C" in TIER_CONFIG

    def test_tier_config_required_keys(self):
        from config.settings import TIER_CONFIG
        required_keys = [
            "station_priority", "search_interval_minutes",
            "calendar_enabled", "delay_base", "delay_jitter",
            "proxy_required", "requests_per_ip_before_rotate",
            "max_requests_per_hour", "daily_limit_per_ip",
        ]
        for tier in TIER_CONFIG.values():
            for key in required_keys:
                assert key in tier, f"Missing key '{key}' in tier config"

    def test_paths_exist(self):
        from config.settings import BASE_DIR, DATA_DIR, DB_PATH, LOG_DIR
        assert BASE_DIR.exists()
        assert str(DB_PATH).endswith(".db")

    def test_user_agents_not_empty(self):
        from config.settings import USER_AGENTS
        assert len(USER_AGENTS) > 0
        assert all(isinstance(ua, str) for ua in USER_AGENTS)
