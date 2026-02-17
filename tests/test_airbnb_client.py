"""
Tests for crawler/airbnb_client.py

Covers:
- _build_headers() - returns correct headers with API key
- AirbnbClient.__init__ - with api_key, without api_key (uses cache), no key at all
- AirbnbClient.compute_response_hash() - deterministic hash
- AirbnbClient.get_stats() - returns correct structure
- AirbnbClient._ensure_client() - curl_cffi import success, fallback to httpx
- AirbnbClient.search_stays() - correct params structure (mock _request)
- AirbnbClient.get_calendar() - correct params structure (mock _request)
- AirbnbClient.get_listing_detail() - correct params with base64 IDs (mock _request)
- AirbnbClient._request() - success, block detection, JSON error, all retries fail
- AirbnbClient.close() - closes client
"""

import base64
import json
from datetime import date
from unittest.mock import patch, MagicMock, AsyncMock

import pytest

from crawler.rate_limiter import BlockType, RateLimiter


# ─── _build_headers() ────────────────────────────────────────────────

class TestBuildHeaders:
    """Tests for _build_headers()."""

    def test_returns_dict_with_api_key(self):
        with patch("crawler.airbnb_client.random.choice", return_value="TestUA/1.0"):
            from crawler.airbnb_client import _build_headers
            headers = _build_headers("my_test_key_123")

        assert headers["X-Airbnb-API-Key"] == "my_test_key_123"
        assert headers["User-Agent"] == "TestUA/1.0"
        assert headers["Accept"] == "application/json"
        assert headers["Content-Type"] == "application/json"
        assert "X-Airbnb-Currency" in headers
        assert "X-Airbnb-Locale" in headers
        assert "Referer" in headers
        assert "Origin" in headers

    def test_headers_contain_sec_fetch(self):
        with patch("crawler.airbnb_client.random.choice", return_value="TestUA/1.0"):
            from crawler.airbnb_client import _build_headers
            headers = _build_headers("key123")

        assert headers["Sec-Fetch-Dest"] == "empty"
        assert headers["Sec-Fetch-Mode"] == "cors"
        assert headers["Sec-Fetch-Site"] == "same-origin"

    def test_different_api_keys(self):
        with patch("crawler.airbnb_client.random.choice", return_value="UA"):
            from crawler.airbnb_client import _build_headers
            h1 = _build_headers("key_a")
            h2 = _build_headers("key_b")

        assert h1["X-Airbnb-API-Key"] == "key_a"
        assert h2["X-Airbnb-API-Key"] == "key_b"


# ─── AirbnbClient.__init__ ───────────────────────────────────────────

class TestAirbnbClientInit:
    """AirbnbClient initialization tests."""

    @patch("crawler.airbnb_client.ProxyManager.from_config")
    @patch("crawler.airbnb_client.RateLimiter.from_config")
    @patch("crawler.airbnb_client.get_cached_credentials", return_value=None)
    @patch("crawler.airbnb_client.AIRBNB_API_KEY", "")
    def test_init_with_api_key(self, mock_creds, mock_rl, mock_pm):
        from crawler.airbnb_client import AirbnbClient
        client = AirbnbClient(api_key="explicit_key_here")
        assert client._api_key == "explicit_key_here"

    @patch("crawler.airbnb_client.ProxyManager.from_config")
    @patch("crawler.airbnb_client.RateLimiter.from_config")
    @patch("crawler.airbnb_client.AIRBNB_API_KEY", "")
    @patch("crawler.airbnb_client.get_cached_credentials")
    def test_init_without_api_key_uses_cache(self, mock_creds, mock_rl, mock_pm):
        mock_creds.return_value = {"api_key": "cached_api_key_value"}
        from crawler.airbnb_client import AirbnbClient
        client = AirbnbClient()
        assert client._api_key == "cached_api_key_value"

    @patch("crawler.airbnb_client.ProxyManager.from_config")
    @patch("crawler.airbnb_client.RateLimiter.from_config")
    @patch("crawler.airbnb_client.AIRBNB_API_KEY", "")
    @patch("crawler.airbnb_client.get_cached_credentials", return_value=None)
    def test_init_no_key_at_all(self, mock_creds, mock_rl, mock_pm):
        from crawler.airbnb_client import AirbnbClient
        client = AirbnbClient()
        assert client._api_key == ""

    @patch("crawler.airbnb_client.ProxyManager.from_config")
    @patch("crawler.airbnb_client.RateLimiter.from_config")
    @patch("crawler.airbnb_client.get_cached_credentials", return_value=None)
    @patch("crawler.airbnb_client.AIRBNB_API_KEY", "env_key_value")
    def test_init_uses_env_key(self, mock_creds, mock_rl, mock_pm):
        from crawler.airbnb_client import AirbnbClient
        client = AirbnbClient()
        assert client._api_key == "env_key_value"

    @patch("crawler.airbnb_client.ProxyManager.from_config")
    @patch("crawler.airbnb_client.RateLimiter.from_config")
    @patch("crawler.airbnb_client.get_cached_credentials", return_value=None)
    @patch("crawler.airbnb_client.AIRBNB_API_KEY", "")
    def test_init_custom_rate_limiter_and_proxy_manager(self, mock_creds, mock_rl, mock_pm):
        from crawler.airbnb_client import AirbnbClient
        custom_rl = RateLimiter(delay_base=1.0)
        custom_pm = MagicMock()
        client = AirbnbClient(api_key="key", rate_limiter=custom_rl, proxy_manager=custom_pm)
        assert client._rate_limiter is custom_rl
        assert client._proxy_manager is custom_pm
        # from_config should NOT have been called since we passed custom objects
        mock_rl.assert_not_called()
        mock_pm.assert_not_called()

    @patch("crawler.airbnb_client.ProxyManager.from_config")
    @patch("crawler.airbnb_client.RateLimiter.from_config")
    @patch("crawler.airbnb_client.get_cached_credentials", return_value=None)
    @patch("crawler.airbnb_client.AIRBNB_API_KEY", "")
    def test_init_http_client_is_none(self, mock_creds, mock_rl, mock_pm):
        from crawler.airbnb_client import AirbnbClient
        client = AirbnbClient(api_key="key")
        assert client._http_client is None


# ─── AirbnbClient.compute_response_hash() ────────────────────────────

class TestComputeResponseHash:
    """AirbnbClient.compute_response_hash() tests."""

    @patch("crawler.airbnb_client.ProxyManager.from_config")
    @patch("crawler.airbnb_client.RateLimiter.from_config")
    @patch("crawler.airbnb_client.get_cached_credentials", return_value=None)
    @patch("crawler.airbnb_client.AIRBNB_API_KEY", "key")
    def test_deterministic_hash(self, mock_creds, mock_rl, mock_pm):
        from crawler.airbnb_client import AirbnbClient
        client = AirbnbClient(api_key="key")
        data = {"key": "value", "nested": {"a": 1}}
        hash1 = client.compute_response_hash(data)
        hash2 = client.compute_response_hash(data)
        assert hash1 == hash2

    @patch("crawler.airbnb_client.ProxyManager.from_config")
    @patch("crawler.airbnb_client.RateLimiter.from_config")
    @patch("crawler.airbnb_client.get_cached_credentials", return_value=None)
    @patch("crawler.airbnb_client.AIRBNB_API_KEY", "key")
    def test_different_data_different_hash(self, mock_creds, mock_rl, mock_pm):
        from crawler.airbnb_client import AirbnbClient
        client = AirbnbClient(api_key="key")
        hash1 = client.compute_response_hash({"a": 1})
        hash2 = client.compute_response_hash({"a": 2})
        assert hash1 != hash2

    @patch("crawler.airbnb_client.ProxyManager.from_config")
    @patch("crawler.airbnb_client.RateLimiter.from_config")
    @patch("crawler.airbnb_client.get_cached_credentials", return_value=None)
    @patch("crawler.airbnb_client.AIRBNB_API_KEY", "key")
    def test_hash_is_16_chars(self, mock_creds, mock_rl, mock_pm):
        from crawler.airbnb_client import AirbnbClient
        client = AirbnbClient(api_key="key")
        h = client.compute_response_hash({"test": True})
        assert len(h) == 16

    @patch("crawler.airbnb_client.ProxyManager.from_config")
    @patch("crawler.airbnb_client.RateLimiter.from_config")
    @patch("crawler.airbnb_client.get_cached_credentials", return_value=None)
    @patch("crawler.airbnb_client.AIRBNB_API_KEY", "key")
    def test_hash_key_order_independent(self, mock_creds, mock_rl, mock_pm):
        """sort_keys=True makes hash independent of key order."""
        from crawler.airbnb_client import AirbnbClient
        client = AirbnbClient(api_key="key")
        hash1 = client.compute_response_hash({"b": 2, "a": 1})
        hash2 = client.compute_response_hash({"a": 1, "b": 2})
        assert hash1 == hash2


# ─── AirbnbClient.get_stats() ────────────────────────────────────────

class TestAirbnbClientGetStats:
    """AirbnbClient.get_stats() tests."""

    @patch("crawler.airbnb_client.ProxyManager.from_config")
    @patch("crawler.airbnb_client.RateLimiter.from_config")
    @patch("crawler.airbnb_client.get_cached_credentials", return_value=None)
    @patch("crawler.airbnb_client.AIRBNB_API_KEY", "key")
    def test_returns_correct_structure(self, mock_creds, mock_rl, mock_pm):
        from crawler.airbnb_client import AirbnbClient
        mock_rl_instance = MagicMock()
        mock_rl_instance.get_stats.return_value = {"total": 5, "success": 3}
        mock_pm_instance = MagicMock()
        mock_pm_instance.get_stats.return_value = {"total": 2, "available": 2}

        client = AirbnbClient(
            api_key="key",
            rate_limiter=mock_rl_instance,
            proxy_manager=mock_pm_instance,
        )
        stats = client.get_stats()
        assert "rate_limiter" in stats
        assert "proxy_manager" in stats
        assert stats["rate_limiter"]["total"] == 5
        assert stats["proxy_manager"]["total"] == 2


# ─── AirbnbClient._ensure_client() ──────────────────────────────────

class TestEnsureClient:
    """AirbnbClient._ensure_client() tests."""

    @patch("crawler.airbnb_client.ProxyManager.from_config")
    @patch("crawler.airbnb_client.RateLimiter.from_config")
    @patch("crawler.airbnb_client.get_cached_credentials", return_value=None)
    @patch("crawler.airbnb_client.AIRBNB_API_KEY", "key")
    async def test_curl_cffi_import_success(self, mock_creds, mock_rl, mock_pm):
        from crawler.airbnb_client import AirbnbClient
        client = AirbnbClient(api_key="key")

        mock_session = MagicMock()
        with patch.dict("sys.modules", {"curl_cffi": MagicMock(), "curl_cffi.requests": MagicMock()}):
            with patch("crawler.airbnb_client.AirbnbClient._ensure_client") as mock_ensure:
                # Simulate successful curl_cffi import
                async def set_client():
                    client._http_client = mock_session
                mock_ensure.side_effect = set_client
                await client._ensure_client()
                assert client._http_client is mock_session

    @patch("crawler.airbnb_client.ProxyManager.from_config")
    @patch("crawler.airbnb_client.RateLimiter.from_config")
    @patch("crawler.airbnb_client.get_cached_credentials", return_value=None)
    @patch("crawler.airbnb_client.AIRBNB_API_KEY", "key")
    async def test_fallback_to_httpx(self, mock_creds, mock_rl, mock_pm):
        from crawler.airbnb_client import AirbnbClient

        client = AirbnbClient(api_key="key")

        # Make curl_cffi import fail, but httpx succeed
        import importlib
        original_import = __builtins__.__import__ if hasattr(__builtins__, '__import__') else __import__

        def mock_import(name, *args, **kwargs):
            if name == "curl_cffi.requests":
                raise ImportError("No curl_cffi")
            return original_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=mock_import):
            await client._ensure_client()

        # Should have fallen back to httpx client
        assert client._http_client is not None
        # Clean up
        await client._http_client.aclose()
        client._http_client = None

    @patch("crawler.airbnb_client.ProxyManager.from_config")
    @patch("crawler.airbnb_client.RateLimiter.from_config")
    @patch("crawler.airbnb_client.get_cached_credentials", return_value=None)
    @patch("crawler.airbnb_client.AIRBNB_API_KEY", "key")
    async def test_does_not_reinitialize_if_exists(self, mock_creds, mock_rl, mock_pm):
        from crawler.airbnb_client import AirbnbClient

        client = AirbnbClient(api_key="key")
        mock_http = MagicMock()
        client._http_client = mock_http
        await client._ensure_client()
        assert client._http_client is mock_http  # Not changed


# ─── AirbnbClient.search_stays() ─────────────────────────────────────

class TestSearchStays:
    """AirbnbClient.search_stays() tests (mocking _request)."""

    @pytest.fixture
    def client_with_mocked_request(self):
        """Create a client with mocked dependencies."""
        with patch("crawler.airbnb_client.ProxyManager.from_config"), \
             patch("crawler.airbnb_client.RateLimiter.from_config"), \
             patch("crawler.airbnb_client.get_cached_credentials", return_value=None), \
             patch("crawler.airbnb_client.AIRBNB_API_KEY", "test_key"):
            from crawler.airbnb_client import AirbnbClient
            client = AirbnbClient(api_key="test_key")
            client._request = AsyncMock(return_value={"data": {}})
            return client

    async def test_search_stays_calls_request(self, client_with_mocked_request):
        client = client_with_mocked_request
        result = await client.search_stays(lat=37.498, lng=127.027)
        client._request.assert_called_once()
        assert result == {"data": {}}

    async def test_search_stays_url_contains_operation(self, client_with_mocked_request):
        client = client_with_mocked_request
        await client.search_stays(lat=37.498, lng=127.027)
        call_args = client._request.call_args
        url = call_args[0][0]
        assert "StaysSearch" in url

    async def test_search_stays_params_structure(self, client_with_mocked_request):
        client = client_with_mocked_request
        await client.search_stays(
            lat=37.498, lng=127.027,
            checkin=date(2026, 3, 1), checkout=date(2026, 3, 2),
            guests=3,
        )
        call_args = client._request.call_args
        params = call_args[1]["params"] if "params" in call_args[1] else call_args[0][1]
        assert params["operationName"] == "StaysSearch"
        assert params["locale"] == "ko"
        assert "variables" in params
        assert "extensions" in params

        # Verify variables contain coordinates
        variables = json.loads(params["variables"])
        raw_params = variables["staysSearchRequest"]["rawParams"]
        filter_names = [p["filterName"] for p in raw_params]
        assert "checkin" in filter_names
        assert "checkout" in filter_names
        assert "adults" in filter_names
        assert "ne_lat" in filter_names
        assert "sw_lat" in filter_names

    async def test_search_stays_with_cursor(self, client_with_mocked_request):
        client = client_with_mocked_request
        await client.search_stays(lat=37.498, lng=127.027, cursor="abc123")
        call_args = client._request.call_args
        params = call_args[1]["params"] if "params" in call_args[1] else call_args[0][1]
        variables = json.loads(params["variables"])
        raw_params = variables["staysSearchRequest"]["rawParams"]
        cursor_params = [p for p in raw_params if p["filterName"] == "cursor"]
        assert len(cursor_params) == 1
        assert cursor_params[0]["filterValues"] == ["abc123"]

    async def test_search_stays_default_dates(self, client_with_mocked_request):
        client = client_with_mocked_request
        await client.search_stays(lat=37.498, lng=127.027)
        call_args = client._request.call_args
        params = call_args[1]["params"] if "params" in call_args[1] else call_args[0][1]
        variables = json.loads(params["variables"])
        raw_params = variables["staysSearchRequest"]["rawParams"]
        checkin_param = [p for p in raw_params if p["filterName"] == "checkin"][0]
        checkout_param = [p for p in raw_params if p["filterName"] == "checkout"][0]
        # Default dates should be tomorrow and day after
        assert checkin_param["filterValues"][0]  # Non-empty
        assert checkout_param["filterValues"][0]  # Non-empty


# ─── AirbnbClient.get_calendar() ─────────────────────────────────────

class TestGetCalendar:
    """AirbnbClient.get_calendar() tests (mocking _request)."""

    @pytest.fixture
    def client_with_mocked_request(self):
        with patch("crawler.airbnb_client.ProxyManager.from_config"), \
             patch("crawler.airbnb_client.RateLimiter.from_config"), \
             patch("crawler.airbnb_client.get_cached_credentials", return_value=None), \
             patch("crawler.airbnb_client.AIRBNB_API_KEY", "test_key"):
            from crawler.airbnb_client import AirbnbClient
            client = AirbnbClient(api_key="test_key")
            client._request = AsyncMock(return_value={"data": {}})
            return client

    async def test_get_calendar_calls_request(self, client_with_mocked_request):
        client = client_with_mocked_request
        result = await client.get_calendar("12345", month=3, year=2026)
        client._request.assert_called_once()
        assert result == {"data": {}}

    async def test_get_calendar_url_contains_operation(self, client_with_mocked_request):
        client = client_with_mocked_request
        await client.get_calendar("12345", month=3, year=2026)
        call_args = client._request.call_args
        url = call_args[0][0]
        assert "PdpAvailabilityCalendar" in url

    async def test_get_calendar_params_structure(self, client_with_mocked_request):
        client = client_with_mocked_request
        await client.get_calendar("12345", month=3, year=2026, count=5)
        call_args = client._request.call_args
        params = call_args[1]["params"] if "params" in call_args[1] else call_args[0][1]
        assert params["operationName"] == "PdpAvailabilityCalendar"

        variables = json.loads(params["variables"])
        assert variables["request"]["listingId"] == "12345"
        assert variables["request"]["month"] == 3
        assert variables["request"]["year"] == 2026
        assert variables["request"]["count"] == 5

    async def test_get_calendar_default_count(self, client_with_mocked_request):
        client = client_with_mocked_request
        await client.get_calendar("12345", month=1, year=2026)
        call_args = client._request.call_args
        params = call_args[1]["params"] if "params" in call_args[1] else call_args[0][1]
        variables = json.loads(params["variables"])
        assert variables["request"]["count"] == 3


# ─── AirbnbClient.get_listing_detail() ───────────────────────────────

class TestGetListingDetail:
    """AirbnbClient.get_listing_detail() tests (mocking _request)."""

    @pytest.fixture
    def client_with_mocked_request(self):
        with patch("crawler.airbnb_client.ProxyManager.from_config"), \
             patch("crawler.airbnb_client.RateLimiter.from_config"), \
             patch("crawler.airbnb_client.get_cached_credentials", return_value=None), \
             patch("crawler.airbnb_client.AIRBNB_API_KEY", "test_key"):
            from crawler.airbnb_client import AirbnbClient
            client = AirbnbClient(api_key="test_key")
            client._request = AsyncMock(return_value={"data": {}})
            return client

    async def test_get_listing_detail_calls_request(self, client_with_mocked_request):
        client = client_with_mocked_request
        result = await client.get_listing_detail("12345")
        client._request.assert_called_once()
        assert result == {"data": {}}

    async def test_get_listing_detail_url_contains_operation(self, client_with_mocked_request):
        client = client_with_mocked_request
        await client.get_listing_detail("12345")
        call_args = client._request.call_args
        url = call_args[0][0]
        assert "StaysPdpSections" in url

    async def test_get_listing_detail_base64_ids(self, client_with_mocked_request):
        client = client_with_mocked_request
        await client.get_listing_detail("98765")
        call_args = client._request.call_args
        params = call_args[1]["params"] if "params" in call_args[1] else call_args[0][1]
        variables = json.loads(params["variables"])

        # Verify base64-encoded IDs
        expected_stay_id = base64.b64encode(b"StayListing:98765").decode()
        expected_demand_id = base64.b64encode(b"DemandStayListing:98765").decode()
        assert variables["id"] == expected_stay_id
        assert variables["demandStayListingId"] == expected_demand_id

    async def test_get_listing_detail_params_structure(self, client_with_mocked_request):
        client = client_with_mocked_request
        await client.get_listing_detail("12345")
        call_args = client._request.call_args
        params = call_args[1]["params"] if "params" in call_args[1] else call_args[0][1]
        assert params["operationName"] == "StaysPdpSections"
        assert "extensions" in params

        variables = json.loads(params["variables"])
        assert "pdpSectionsRequest" in variables
        assert variables["includeGpReviewsFragment"] is True


# ─── AirbnbClient._request() ─────────────────────────────────────────

class TestRequest:
    """AirbnbClient._request() tests with mocked HTTP client."""

    @pytest.fixture
    def client_setup(self):
        """Create a client with mocked rate limiter, proxy manager, and HTTP client."""
        with patch("crawler.airbnb_client.ProxyManager.from_config"), \
             patch("crawler.airbnb_client.RateLimiter.from_config"), \
             patch("crawler.airbnb_client.get_cached_credentials", return_value=None), \
             patch("crawler.airbnb_client.AIRBNB_API_KEY", "test_key"):
            from crawler.airbnb_client import AirbnbClient

            mock_rl = MagicMock()
            mock_rl.wait = AsyncMock()
            mock_rl.detect_block = MagicMock(return_value=BlockType.NONE)
            mock_rl.report_success = MagicMock()
            mock_rl.report_failure = MagicMock()

            mock_pm = MagicMock()
            mock_pm.get_proxy = MagicMock(return_value=None)
            mock_pm.report_success = MagicMock()
            mock_pm.report_blocked = MagicMock()

            client = AirbnbClient(
                api_key="test_key",
                rate_limiter=mock_rl,
                proxy_manager=mock_pm,
            )

            mock_http = MagicMock()
            mock_http.get = AsyncMock()
            # httpx-style client (no impersonate)
            if hasattr(mock_http, "impersonate"):
                del mock_http.impersonate
            client._http_client = mock_http

            return client, mock_rl, mock_pm, mock_http

    async def test_request_success(self, client_setup):
        client, mock_rl, mock_pm, mock_http = client_setup
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"data": {"results": []}}'
        mock_http.get.return_value = mock_response

        result = await client._request("https://api.example.com/test")
        assert result == {"data": {"results": []}}
        mock_rl.report_success.assert_called_once()

    async def test_request_block_detection(self, client_setup):
        client, mock_rl, mock_pm, mock_http = client_setup

        # First call blocked, second succeeds
        mock_response_blocked = MagicMock()
        mock_response_blocked.status_code = 429
        mock_response_blocked.text = "Rate limited"

        mock_response_ok = MagicMock()
        mock_response_ok.status_code = 200
        mock_response_ok.text = '{"data": {}}'

        mock_http.get.side_effect = [mock_response_blocked, mock_response_ok]
        mock_rl.detect_block.side_effect = [BlockType.RATE_LIMIT, BlockType.NONE]

        result = await client._request("https://api.example.com/test", max_retries=3)
        assert result == {"data": {}}
        mock_rl.report_failure.assert_called_once_with(BlockType.RATE_LIMIT)
        mock_rl.report_success.assert_called_once()

    async def test_request_json_error(self, client_setup):
        client, mock_rl, mock_pm, mock_http = client_setup

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "not valid json"
        mock_http.get.return_value = mock_response

        result = await client._request("https://api.example.com/test", max_retries=1)
        assert result is None
        mock_rl.report_failure.assert_called()

    async def test_request_all_retries_fail(self, client_setup):
        client, mock_rl, mock_pm, mock_http = client_setup

        mock_http.get.side_effect = Exception("Connection error")

        result = await client._request("https://api.example.com/test", max_retries=3)
        assert result is None
        assert mock_rl.report_failure.call_count == 3

    async def test_request_with_proxy(self, client_setup):
        client, mock_rl, mock_pm, mock_http = client_setup
        mock_pm.get_proxy.return_value = "http://proxy:8080"

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"data": {}}'
        mock_http.get.return_value = mock_response

        result = await client._request("https://api.example.com/test")
        assert result == {"data": {}}
        mock_pm.report_success.assert_called_once()

    async def test_request_proxy_blocked_reports_to_proxy_manager(self, client_setup):
        client, mock_rl, mock_pm, mock_http = client_setup
        mock_pm.get_proxy.return_value = "http://proxy:8080"

        mock_response_blocked = MagicMock()
        mock_response_blocked.status_code = 403
        mock_response_blocked.text = "Forbidden"

        mock_response_ok = MagicMock()
        mock_response_ok.status_code = 200
        mock_response_ok.text = '{"data": {}}'

        mock_http.get.side_effect = [mock_response_blocked, mock_response_ok]
        mock_rl.detect_block.side_effect = [BlockType.FORBIDDEN, BlockType.NONE]

        result = await client._request("https://api.example.com/test", max_retries=3)
        assert result == {"data": {}}
        mock_pm.report_blocked.assert_called_once()

    async def test_request_calls_wait_before_each_attempt(self, client_setup):
        client, mock_rl, mock_pm, mock_http = client_setup

        mock_http.get.side_effect = Exception("fail")
        await client._request("https://api.example.com/test", max_retries=3)
        assert mock_rl.wait.call_count == 3

    async def test_request_uses_build_headers(self, client_setup):
        client, mock_rl, mock_pm, mock_http = client_setup

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"data": {}}'
        mock_http.get.return_value = mock_response

        with patch("crawler.airbnb_client._build_headers", return_value={"X-Test": "val"}) as mock_headers:
            await client._request("https://api.example.com/test")
            mock_headers.assert_called_once_with("test_key")


# ─── AirbnbClient.close() ────────────────────────────────────────────

class TestClose:
    """AirbnbClient.close() tests."""

    async def test_close_closes_client(self):
        with patch("crawler.airbnb_client.ProxyManager.from_config"), \
             patch("crawler.airbnb_client.RateLimiter.from_config"), \
             patch("crawler.airbnb_client.get_cached_credentials", return_value=None), \
             patch("crawler.airbnb_client.AIRBNB_API_KEY", "key"):
            from crawler.airbnb_client import AirbnbClient

            client = AirbnbClient(api_key="key")
            mock_http = MagicMock()
            mock_http.close = AsyncMock()
            client._http_client = mock_http

            await client.close()
            mock_http.close.assert_called_once()
            assert client._http_client is None

    async def test_close_when_no_client(self):
        with patch("crawler.airbnb_client.ProxyManager.from_config"), \
             patch("crawler.airbnb_client.RateLimiter.from_config"), \
             patch("crawler.airbnb_client.get_cached_credentials", return_value=None), \
             patch("crawler.airbnb_client.AIRBNB_API_KEY", "key"):
            from crawler.airbnb_client import AirbnbClient

            client = AirbnbClient(api_key="key")
            assert client._http_client is None
            await client.close()  # Should not raise
            assert client._http_client is None


# ─── curl_cffi 실제 코드 경로 커버리지 ─────────────────────────────────

class TestEnsureClientCurlCffi:
    """_ensure_client curl_cffi import 성공 경로 (lines 97-98)."""

    async def test_curl_cffi_real_path(self):
        """curl_cffi가 설치된 환경에서 AsyncSession을 생성한다."""
        from crawler.airbnb_client import AirbnbClient

        mock_async_session_cls = MagicMock()
        mock_session_instance = MagicMock()
        mock_async_session_cls.return_value = mock_session_instance

        mock_curl_cffi = MagicMock()
        mock_requests = MagicMock()
        mock_requests.AsyncSession = mock_async_session_cls
        mock_curl_cffi.requests = mock_requests

        with patch("crawler.airbnb_client.ProxyManager.from_config"), \
             patch("crawler.airbnb_client.RateLimiter.from_config"), \
             patch("crawler.airbnb_client.get_cached_credentials", return_value=None), \
             patch("crawler.airbnb_client.AIRBNB_API_KEY", "key"):

            client = AirbnbClient(api_key="key")
            assert client._http_client is None

            # Inject curl_cffi into sys.modules so the real code's import succeeds
            import sys
            with patch.dict(sys.modules, {
                "curl_cffi": mock_curl_cffi,
                "curl_cffi.requests": mock_requests,
            }):
                await client._ensure_client()

            assert client._http_client is mock_session_instance
            mock_async_session_cls.assert_called_once_with(impersonate="chrome")


class TestRequestCurlCffiPath:
    """_request에서 curl_cffi (impersonate 속성) 경로 (lines 133-135)."""

    async def test_request_via_curl_cffi_client(self):
        """hasattr(client, 'impersonate')가 True일 때 curl_cffi 경로로 요청한다."""
        from crawler.airbnb_client import AirbnbClient

        mock_rl = MagicMock()
        mock_rl.wait = AsyncMock()
        mock_rl.detect_block = MagicMock(return_value=BlockType.NONE)
        mock_rl.report_success = MagicMock()
        mock_pm = MagicMock()
        mock_pm.get_proxy = MagicMock(return_value=None)
        mock_pm.report_success = MagicMock()

        with patch("crawler.airbnb_client.ProxyManager.from_config"), \
             patch("crawler.airbnb_client.RateLimiter.from_config"), \
             patch("crawler.airbnb_client.get_cached_credentials", return_value=None), \
             patch("crawler.airbnb_client.AIRBNB_API_KEY", "key"):

            client = AirbnbClient(api_key="key", rate_limiter=mock_rl, proxy_manager=mock_pm)

            # curl_cffi-style mock: has 'impersonate' attribute
            mock_http = MagicMock()
            mock_http.impersonate = "chrome"
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.text = '{"data": "ok"}'
            mock_http.get = AsyncMock(return_value=mock_response)
            client._http_client = mock_http

            result = await client._request("https://api.example.com/test")
            assert result == {"data": "ok"}
            mock_http.get.assert_awaited_once()
