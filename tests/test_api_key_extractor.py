"""
Tests for crawler/api_key_extractor.py

Covers:
- _extract_api_key_from_html() - all 4 regex patterns + no match
- _extract_hashes_from_text() - operationId pattern, sha256Hash pattern, no match
- _load_cache() - file not exists, expired cache, valid cache, invalid JSON
- _save_cache() - creates file with cached_at
- get_cached_credentials() - delegates to _load_cache
- get_operation_hash() - with cached data, without cache
- get_api_key_sync() - with cache, without cache (mocks asyncio.run)
"""

import json
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from crawler.api_key_extractor import (
    _extract_api_key_from_html,
    _extract_hashes_from_text,
    _load_cache,
    _save_cache,
    get_cached_credentials,
    get_operation_hash,
    get_api_key_sync,
    CACHE_MAX_AGE_HOURS,
)


# ─── _extract_api_key_from_html() ────────────────────────────────────

class TestExtractApiKeyFromHtml:
    """Tests for _extract_api_key_from_html with all 4 regex patterns."""

    def test_pattern1_key_colon(self):
        html = 'some data "key":"d306zoyjsyarp7ifhu67rjxn52tv0t20" more data'
        result = _extract_api_key_from_html(html)
        assert result == "d306zoyjsyarp7ifhu67rjxn52tv0t20"

    def test_pattern1_with_spaces(self):
        html = '"key" : "abcdef1234567890abcdef1234567890ab" end'
        result = _extract_api_key_from_html(html)
        assert result == "abcdef1234567890abcdef1234567890ab"

    def test_pattern2_api_key(self):
        html = '"api_key":"abcdef1234567890abcdef1234567890ab"'
        result = _extract_api_key_from_html(html)
        assert result == "abcdef1234567890abcdef1234567890ab"

    def test_pattern3_airbnb_api_key(self):
        html = '"AIRBNB_API_KEY":"abcdef1234567890abcdef1234567890ab"'
        result = _extract_api_key_from_html(html)
        assert result == "abcdef1234567890abcdef1234567890ab"

    def test_pattern4_x_airbnb_api_key_header(self):
        html = 'x-airbnb-api-key: abcdef1234567890abcdef1234567890ab something'
        result = _extract_api_key_from_html(html)
        assert result == "abcdef1234567890abcdef1234567890ab"

    def test_pattern4_x_airbnb_api_key_json(self):
        html = '"x-airbnb-api-key":"abcdef1234567890abcdef1234567890ab"'
        result = _extract_api_key_from_html(html)
        assert result == "abcdef1234567890abcdef1234567890ab"

    def test_no_match_returns_empty(self):
        html = '<html><body>No API key here</body></html>'
        result = _extract_api_key_from_html(html)
        assert result == ""

    def test_short_key_not_matched(self):
        # Key must be at least 32 chars
        html = '"key":"shortkey123"'
        result = _extract_api_key_from_html(html)
        assert result == ""

    def test_case_insensitive(self):
        html = '"KEY":"abcdef1234567890abcdef1234567890ab"'
        result = _extract_api_key_from_html(html)
        assert result == "abcdef1234567890abcdef1234567890ab"

    def test_first_pattern_wins(self):
        """When multiple patterns match, the first regex pattern should win."""
        # Keys must be at least 32 chars of [a-z0-9]
        html = '"key":"firstkey1234567890firstkey12345678" "api_key":"secondkey23456789secondkey23456789"'
        result = _extract_api_key_from_html(html)
        assert result == "firstkey1234567890firstkey12345678"


# ─── _extract_hashes_from_text() ─────────────────────────────────────

class TestExtractHashesFromText:
    """Tests for _extract_hashes_from_text."""

    def test_operation_id_pattern(self):
        """Test name:'OpName'...operationId:'hex64' pattern."""
        hash_val = "a" * 64
        text = f"name:'StaysSearch' some stuff operationId:'{hash_val}'"
        result = _extract_hashes_from_text(text)
        assert result.get("StaysSearch") == hash_val

    def test_sha256hash_after_operation_name(self):
        """Test "OpName"...sha256Hash:"hex64" pattern."""
        hash_val = "b" * 64
        text = f'"StaysSearch" some stuff "sha256Hash":"{hash_val}"'
        result = _extract_hashes_from_text(text)
        assert result.get("StaysSearch") == hash_val

    def test_sha256hash_before_operation_name(self):
        """Test sha256Hash:"hex64"..."OpName" pattern."""
        hash_val = "c" * 64
        text = f'"sha256Hash":"{hash_val}" some stuff "PdpAvailabilityCalendar"'
        result = _extract_hashes_from_text(text)
        assert result.get("PdpAvailabilityCalendar") == hash_val

    def test_multiple_operations(self):
        hash1 = "a" * 64
        hash2 = "b" * 64
        # Use '}' separator so that [^}]{0,300} regex boundary works correctly
        text = (
            f"name:'StaysSearch' operationId:'{hash1}'}}"
            f"name:'PdpAvailabilityCalendar' operationId:'{hash2}'}}"
        )
        result = _extract_hashes_from_text(text)
        assert result.get("StaysSearch") == hash1
        assert result.get("PdpAvailabilityCalendar") == hash2

    def test_no_match_returns_empty_dict(self):
        text = "just some random javascript code without any hashes"
        result = _extract_hashes_from_text(text)
        assert result == {}

    def test_non_target_operation_not_extracted(self):
        hash_val = "d" * 64
        text = f"name:'SomeOtherOperation' operationId:'{hash_val}'"
        result = _extract_hashes_from_text(text)
        assert "SomeOtherOperation" not in result
        assert result == {}

    def test_all_target_operations(self):
        """Verify all TARGET_OPS can be extracted."""
        from crawler.api_key_extractor import TARGET_OPS
        text_parts = []
        expected = {}
        for i, op in enumerate(TARGET_OPS):
            h = f"{i}" * 64
            # Truncate to exactly 64 hex chars
            h = h[:64]
            # Ensure it's valid hex (use 'a' padded with index)
            h = format(i, 'x') * 64
            h = h[:64]
            text_parts.append(f"name:'{op}' operationId:'{h}'")
            expected[op] = h
        text = " ".join(text_parts)
        result = _extract_hashes_from_text(text)
        for op in TARGET_OPS:
            assert op in result


# ─── _load_cache() ───────────────────────────────────────────────────

class TestLoadCache:
    """Tests for _load_cache() with patched CACHE_FILE."""

    def test_file_not_exists(self, tmp_cache_file):
        with patch("crawler.api_key_extractor.CACHE_FILE", tmp_cache_file):
            result = _load_cache()
        assert result is None

    def test_expired_cache(self, tmp_cache_file):
        expired_time = time.time() - (CACHE_MAX_AGE_HOURS + 1) * 3600
        data = {
            "api_key": "testkey1234567890testkey1234567890",
            "hashes": {},
            "cached_at": expired_time,
        }
        tmp_cache_file.write_text(json.dumps(data))
        with patch("crawler.api_key_extractor.CACHE_FILE", tmp_cache_file):
            result = _load_cache()
        assert result is None

    def test_valid_cache(self, tmp_cache_file):
        data = {
            "api_key": "testkey1234567890testkey1234567890",
            "hashes": {"StaysSearch": "a" * 64},
            "cached_at": time.time(),
        }
        tmp_cache_file.write_text(json.dumps(data))
        with patch("crawler.api_key_extractor.CACHE_FILE", tmp_cache_file):
            result = _load_cache()
        assert result is not None
        assert result["api_key"] == "testkey1234567890testkey1234567890"
        assert "StaysSearch" in result["hashes"]

    def test_invalid_json(self, tmp_cache_file):
        tmp_cache_file.write_text("not valid json{{{")
        with patch("crawler.api_key_extractor.CACHE_FILE", tmp_cache_file):
            result = _load_cache()
        assert result is None

    def test_missing_api_key_returns_none(self, tmp_cache_file):
        data = {
            "api_key": "",
            "hashes": {},
            "cached_at": time.time(),
        }
        tmp_cache_file.write_text(json.dumps(data))
        with patch("crawler.api_key_extractor.CACHE_FILE", tmp_cache_file):
            result = _load_cache()
        assert result is None

    def test_missing_cached_at_defaults_to_zero(self, tmp_cache_file):
        """If cached_at is missing, it defaults to 0, which is expired."""
        data = {
            "api_key": "testkey1234567890testkey1234567890",
            "hashes": {},
        }
        tmp_cache_file.write_text(json.dumps(data))
        with patch("crawler.api_key_extractor.CACHE_FILE", tmp_cache_file):
            result = _load_cache()
        # cached_at=0 makes age very large, so it should be expired
        assert result is None


# ─── _save_cache() ───────────────────────────────────────────────────

class TestSaveCache:
    """Tests for _save_cache() with patched CACHE_FILE."""

    def test_creates_file(self, tmp_cache_file):
        data = {
            "api_key": "testkey1234567890testkey1234567890",
            "hashes": {"StaysSearch": "a" * 64},
        }
        with patch("crawler.api_key_extractor.CACHE_FILE", tmp_cache_file):
            _save_cache(data)
        assert tmp_cache_file.exists()

    def test_file_contains_cached_at(self, tmp_cache_file):
        data = {
            "api_key": "testkey1234567890testkey1234567890",
            "hashes": {},
        }
        before = time.time()
        with patch("crawler.api_key_extractor.CACHE_FILE", tmp_cache_file):
            _save_cache(data)
        saved = json.loads(tmp_cache_file.read_text())
        assert "cached_at" in saved
        assert saved["cached_at"] >= before

    def test_file_contains_original_data(self, tmp_cache_file):
        data = {
            "api_key": "mykey12345678901234567890123456",
            "hashes": {"Op1": "hash1" * 13},  # 65 chars, but we just check it's there
        }
        with patch("crawler.api_key_extractor.CACHE_FILE", tmp_cache_file):
            _save_cache(data)
        saved = json.loads(tmp_cache_file.read_text())
        assert saved["api_key"] == data["api_key"]
        assert saved["hashes"] == data["hashes"]

    def test_creates_parent_directory(self, tmp_path):
        cache_file = tmp_path / "subdir" / "nested" / "cache.json"
        data = {"api_key": "test", "hashes": {}}
        with patch("crawler.api_key_extractor.CACHE_FILE", cache_file):
            _save_cache(data)
        assert cache_file.exists()

    def test_overwrites_existing_file(self, tmp_cache_file):
        tmp_cache_file.write_text('{"old": "data"}')
        data = {"api_key": "newkey", "hashes": {}}
        with patch("crawler.api_key_extractor.CACHE_FILE", tmp_cache_file):
            _save_cache(data)
        saved = json.loads(tmp_cache_file.read_text())
        assert saved["api_key"] == "newkey"
        assert "old" not in saved


# ─── get_cached_credentials() ────────────────────────────────────────

class TestGetCachedCredentials:
    """Tests for get_cached_credentials()."""

    def test_delegates_to_load_cache(self, tmp_cache_file):
        data = {
            "api_key": "testkey1234567890testkey1234567890",
            "hashes": {},
            "cached_at": time.time(),
        }
        tmp_cache_file.write_text(json.dumps(data))
        with patch("crawler.api_key_extractor.CACHE_FILE", tmp_cache_file):
            result = get_cached_credentials()
        assert result is not None
        assert result["api_key"] == data["api_key"]

    def test_returns_none_when_no_cache(self, tmp_cache_file):
        with patch("crawler.api_key_extractor.CACHE_FILE", tmp_cache_file):
            result = get_cached_credentials()
        assert result is None


# ─── get_operation_hash() ────────────────────────────────────────────

class TestGetOperationHash:
    """Tests for get_operation_hash()."""

    def test_with_cached_data(self, tmp_cache_file):
        hash_val = "e" * 64
        data = {
            "api_key": "testkey1234567890testkey1234567890",
            "hashes": {"StaysSearch": hash_val},
            "cached_at": time.time(),
        }
        tmp_cache_file.write_text(json.dumps(data))
        with patch("crawler.api_key_extractor.CACHE_FILE", tmp_cache_file):
            result = get_operation_hash("StaysSearch")
        assert result == hash_val

    def test_without_cache_returns_empty(self, tmp_cache_file):
        with patch("crawler.api_key_extractor.CACHE_FILE", tmp_cache_file):
            result = get_operation_hash("StaysSearch")
        assert result == ""

    def test_missing_operation_returns_empty(self, tmp_cache_file):
        data = {
            "api_key": "testkey1234567890testkey1234567890",
            "hashes": {"OtherOp": "f" * 64},
            "cached_at": time.time(),
        }
        tmp_cache_file.write_text(json.dumps(data))
        with patch("crawler.api_key_extractor.CACHE_FILE", tmp_cache_file):
            result = get_operation_hash("StaysSearch")
        assert result == ""


# ─── get_api_key_sync() ─────────────────────────────────────────────

class TestGetApiKeySync:
    """Tests for get_api_key_sync()."""

    def test_with_cache(self, tmp_cache_file):
        data = {
            "api_key": "cached_key_1234567890abcdef12345",
            "hashes": {},
            "cached_at": time.time(),
        }
        tmp_cache_file.write_text(json.dumps(data))
        with patch("crawler.api_key_extractor.CACHE_FILE", tmp_cache_file):
            result = get_api_key_sync()
        assert result == "cached_key_1234567890abcdef12345"

    def test_without_cache_calls_asyncio_run(self, tmp_cache_file):
        """When no cache, should call asyncio.run(extract_api_credentials())."""
        with patch("crawler.api_key_extractor.CACHE_FILE", tmp_cache_file), \
             patch("crawler.api_key_extractor.asyncio") as mock_asyncio:
            mock_asyncio.run.return_value = {
                "api_key": "extracted_key_abcdef1234567890ab",
                "hashes": {},
            }
            result = get_api_key_sync()
        assert result == "extracted_key_abcdef1234567890ab"
        mock_asyncio.run.assert_called_once()

    def test_without_cache_no_key_found(self, tmp_cache_file):
        """When no cache and extraction fails, returns empty string."""
        with patch("crawler.api_key_extractor.CACHE_FILE", tmp_cache_file), \
             patch("crawler.api_key_extractor.asyncio") as mock_asyncio:
            mock_asyncio.run.return_value = {"api_key": "", "hashes": {}}
            result = get_api_key_sync()
        assert result == ""


# ─── _extract_via_httpx() ─────────────────────────────────────────

class TestExtractViaHttpx:
    """Tests for _extract_via_httpx async function."""

    async def test_successful_extraction(self):
        """httpx로 API 키와 해시를 추출한다."""
        from unittest.mock import AsyncMock as AM
        from crawler.api_key_extractor import _extract_via_httpx

        mock_html = (
            '<html>"key":"d306zoyjsyarp7ifhu67rjxn52tv0t20"'
            '<script src="https://a0.muscache.com/bundle1.js"></script></html>'
        )
        mock_js = f"name:'StaysSearch' operationId:'{'a' * 64}'}}"

        mock_listing_text = (
            f"name:'StaysPdpSections' operationId:'{'b' * 64}'}}"
            f"name:'PdpAvailabilityCalendar' operationId:'{'c' * 64}'}}"
        )

        async def mock_get(url, **kwargs):
            resp = MagicMock()
            resp.status_code = 200
            if ".js" in url or "bundle" in url:
                resp.text = mock_js
            elif "/rooms/" in url:
                resp.text = mock_listing_text
            else:
                resp.text = mock_html
            return resp

        mock_client = MagicMock()
        mock_client.get = AM(side_effect=mock_get)
        mock_client.__aenter__ = AM(return_value=mock_client)
        mock_client.__aexit__ = AM(return_value=False)

        with patch("httpx.AsyncClient", return_value=mock_client):
            result = await _extract_via_httpx()

        assert result["api_key"] == "d306zoyjsyarp7ifhu67rjxn52tv0t20"
        assert "StaysSearch" in result["hashes"]

    async def test_non_200_returns_empty(self):
        """HTTP 200이 아니면 빈 credentials를 반환한다."""
        from unittest.mock import AsyncMock as AM
        from crawler.api_key_extractor import _extract_via_httpx

        mock_resp = MagicMock()
        mock_resp.status_code = 403
        mock_resp.text = ""

        mock_client = MagicMock()
        mock_client.get = AM(return_value=mock_resp)
        mock_client.__aenter__ = AM(return_value=mock_client)
        mock_client.__aexit__ = AM(return_value=False)

        with patch("httpx.AsyncClient", return_value=mock_client):
            result = await _extract_via_httpx()

        assert result["api_key"] == ""
        assert result["hashes"] == {}


# ─── _scan_js_bundles() ──────────────────────────────────────────────

class TestScanJsBundles:
    """Tests for _scan_js_bundles."""

    async def test_scans_bundles_and_extracts(self):
        """JS 번들에서 API 키와 해시를 추출한다."""
        from unittest.mock import AsyncMock as AM
        from crawler.api_key_extractor import _scan_js_bundles

        html = '<script src="https://a0.muscache.com/test.js"></script>'
        credentials = {"api_key": "", "hashes": {}}

        hash_val = "a" * 64
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = (
            '"key":"d306zoyjsyarp7ifhu67rjxn52tv0t20" '
            f"name:'StaysSearch' operationId:'{hash_val}'}}"
            f"name:'PdpAvailabilityCalendar' operationId:'{'b' * 64}'}}"
            f"name:'StaysPdpSections' operationId:'{'c' * 64}'}}"
        )

        mock_client = MagicMock()
        mock_client.get = AM(return_value=mock_response)

        await _scan_js_bundles(mock_client, html, credentials)

        assert credentials["api_key"] == "d306zoyjsyarp7ifhu67rjxn52tv0t20"
        assert "StaysSearch" in credentials["hashes"]

    async def test_handles_failed_requests(self):
        """JS 번들 요청 실패 시 에러 없이 계속한다."""
        from unittest.mock import AsyncMock as AM
        from crawler.api_key_extractor import _scan_js_bundles

        html = '<script src="https://a0.muscache.com/test.js"></script>'
        credentials = {"api_key": "", "hashes": {}}

        mock_client = MagicMock()
        mock_client.get = AM(side_effect=Exception("Network error"))

        await _scan_js_bundles(mock_client, html, credentials)
        assert credentials["api_key"] == ""

    async def test_skips_non_200_responses(self):
        """200이 아닌 JS 번들 응답은 건너뛴다."""
        from unittest.mock import AsyncMock as AM
        from crawler.api_key_extractor import _scan_js_bundles

        html = '<script src="https://a0.muscache.com/test.js"></script>'
        credentials = {"api_key": "", "hashes": {}}

        mock_response = MagicMock()
        mock_response.status_code = 404

        mock_client = MagicMock()
        mock_client.get = AM(return_value=mock_response)

        await _scan_js_bundles(mock_client, html, credentials)
        assert credentials["api_key"] == ""

    async def test_stops_when_all_found(self):
        """필요한 모든 해시를 찾으면 조기 종료한다."""
        from unittest.mock import AsyncMock as AM
        from crawler.api_key_extractor import _scan_js_bundles, REQUIRED_OPS

        html = (
            '<script src="https://a0.muscache.com/a.js"></script>'
            '<script src="https://a0.muscache.com/b.js"></script>'
        )
        credentials = {
            "api_key": "d306zoyjsyarp7ifhu67rjxn52tv0t20",
            "hashes": {op: "x" * 64 for op in REQUIRED_OPS},
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "nothing here"

        mock_client = MagicMock()
        mock_client.get = AM(return_value=mock_response)

        await _scan_js_bundles(mock_client, html, credentials)
        # Should have stopped early; only 1 call (break after first check)
        assert mock_client.get.call_count <= 1


# ─── _scan_listing_page() ────────────────────────────────────────────

class TestScanListingPage:
    """Tests for _scan_listing_page."""

    async def test_finds_listing_from_rooms_url(self):
        """검색 HTML에서 /rooms/ URL을 찾아 리스팅 페이지를 스캔한다."""
        from unittest.mock import AsyncMock as AM
        from crawler.api_key_extractor import _scan_listing_page, REQUIRED_OPS

        search_html = 'href="/rooms/12345678" class="link"'
        # Pre-fill all required hashes so _scan_lazy_bundles is not called
        credentials = {
            "api_key": "key",
            "hashes": {op: "x" * 64 for op in REQUIRED_OPS},
        }
        # Remove StaysPdpSections so we can detect it being added
        del credentials["hashes"]["StaysPdpSections"]

        hash_val = "b" * 64
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = f"name:'StaysPdpSections' operationId:'{hash_val}'}}"

        mock_client = MagicMock()
        mock_client.get = AM(return_value=mock_response)

        await _scan_listing_page(mock_client, search_html, credentials)

        assert "StaysPdpSections" in credentials["hashes"]

    async def test_uses_fallback_listing_url(self):
        """리스팅 ID를 찾을 수 없으면 fallback URL을 사용한다."""
        from unittest.mock import AsyncMock as AM
        from crawler.api_key_extractor import _scan_listing_page, REQUIRED_OPS

        search_html = "<html>no listing IDs here</html>"
        credentials = {"api_key": "key", "hashes": {op: "x" * 64 for op in REQUIRED_OPS}}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "no hashes here either"

        mock_client = MagicMock()
        mock_client.get = AM(return_value=mock_response)

        await _scan_listing_page(mock_client, search_html, credentials)

        called_urls = [str(call.args[0]) for call in mock_client.get.call_args_list]
        assert any("/rooms/" in url for url in called_urls)

    async def test_finds_listing_from_base64_id(self):
        """base64 인코딩된 DemandStayListing ID에서 리스팅 ID를 추출한다."""
        import base64
        from unittest.mock import AsyncMock as AM
        from crawler.api_key_extractor import _scan_listing_page, REQUIRED_OPS

        encoded = base64.b64encode(b"DemandStayListing:99887766").decode()
        search_html = f'id:"{encoded}"'
        credentials = {"api_key": "key", "hashes": {op: "x" * 64 for op in REQUIRED_OPS}}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "no hashes"

        mock_client = MagicMock()
        mock_client.get = AM(return_value=mock_response)

        await _scan_listing_page(mock_client, search_html, credentials)

        called_urls = [str(call.args[0]) for call in mock_client.get.call_args_list]
        assert any("99887766" in url for url in called_urls)

    async def test_finds_listing_from_property_id(self):
        """propertyId에서 리스팅 ID를 추출한다."""
        from unittest.mock import AsyncMock as AM
        from crawler.api_key_extractor import _scan_listing_page, REQUIRED_OPS

        search_html = '"propertyId":"55667788" some other stuff'
        credentials = {"api_key": "key", "hashes": {op: "x" * 64 for op in REQUIRED_OPS}}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "no hashes"

        mock_client = MagicMock()
        mock_client.get = AM(return_value=mock_response)

        await _scan_listing_page(mock_client, search_html, credentials)

        called_urls = [str(call.args[0]) for call in mock_client.get.call_args_list]
        assert any("55667788" in url for url in called_urls)

    async def test_handles_listing_page_error(self):
        """리스팅 페이지 요청 실패 시 에러 없이 반환한다."""
        from unittest.mock import AsyncMock as AM
        from crawler.api_key_extractor import _scan_listing_page

        search_html = 'href="/rooms/12345678"'
        credentials = {"api_key": "key", "hashes": {}}

        mock_client = MagicMock()
        mock_client.get = AM(side_effect=Exception("Network error"))

        await _scan_listing_page(mock_client, search_html, credentials)

    async def test_handles_non_200_listing_page(self):
        """리스팅 페이지가 200이 아닐 때 반환한다."""
        from unittest.mock import AsyncMock as AM
        from crawler.api_key_extractor import _scan_listing_page

        search_html = 'href="/rooms/12345678"'
        credentials = {"api_key": "key", "hashes": {}}

        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.text = ""

        mock_client = MagicMock()
        mock_client.get = AM(return_value=mock_response)

        await _scan_listing_page(mock_client, search_html, credentials)


# ─── _scan_lazy_bundles() ───────────────────────────────────────────

class TestScanLazyBundles:
    """Tests for _scan_lazy_bundles."""

    async def test_finds_calendar_hash_from_lazy_bundle(self):
        """lazy-loaded 번들에서 Calendar 해시를 찾는다."""
        from unittest.mock import AsyncMock as AM
        from crawler.api_key_extractor import _scan_lazy_bundles

        html = '<script src="https://a0.muscache.com/RoomCalendar.abc.js"></script>'
        credentials = {"api_key": "key", "hashes": {"StaysSearch": "a" * 64}}

        hash_val = "d" * 64
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = f"name:'PdpAvailabilityCalendar' operationId:'{hash_val}'}}"

        mock_client = MagicMock()
        mock_client.get = AM(return_value=mock_response)

        await _scan_lazy_bundles(mock_client, html, credentials)

        assert credentials["hashes"].get("PdpAvailabilityCalendar") == hash_val

    async def test_scans_async_require_bundles(self):
        """asyncRequire 번들에서 참조하는 lazy URL을 추출한다."""
        from unittest.mock import AsyncMock as AM
        from crawler.api_key_extractor import _scan_lazy_bundles

        html = '<script src="https://a0.muscache.com/asyncRequire.bundle.js"></script>'
        credentials = {"api_key": "key", "hashes": {"StaysSearch": "a" * 64}}

        async_req_text = '"RoomCalendar.lazy.js" "AvailabilityCalendar.lazy.js"'
        mock_ar_response = MagicMock()
        mock_ar_response.status_code = 200
        mock_ar_response.text = async_req_text

        hash_val = "e" * 64
        mock_lazy_response = MagicMock()
        mock_lazy_response.status_code = 200
        mock_lazy_response.text = f"name:'PdpAvailabilityCalendar' operationId:'{hash_val}'}}"

        async def mock_get(url):
            if "asyncRequire" in url:
                return mock_ar_response
            return mock_lazy_response

        mock_client = MagicMock()
        mock_client.get = AM(side_effect=mock_get)

        await _scan_lazy_bundles(mock_client, html, credentials)

    async def test_handles_errors_gracefully(self):
        """lazy 번들 요청 에러 시 에러 없이 계속한다."""
        from unittest.mock import AsyncMock as AM
        from crawler.api_key_extractor import _scan_lazy_bundles

        html = '<script src="https://a0.muscache.com/RoomCalendar.abc.js"></script>'
        credentials = {"api_key": "key", "hashes": {}}

        mock_client = MagicMock()
        mock_client.get = AM(side_effect=Exception("Failed"))

        await _scan_lazy_bundles(mock_client, html, credentials)

    async def test_stops_when_all_required_found(self):
        """필요한 해시를 모두 찾으면 조기 종료한다."""
        from unittest.mock import AsyncMock as AM
        from crawler.api_key_extractor import _scan_lazy_bundles, REQUIRED_OPS

        html = '<script src="https://a0.muscache.com/RoomCalendar.a.js"></script>'
        credentials = {
            "api_key": "key",
            "hashes": {op: "x" * 64 for op in REQUIRED_OPS},
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "nothing"

        mock_client = MagicMock()
        mock_client.get = AM(return_value=mock_response)

        await _scan_lazy_bundles(mock_client, html, credentials)
        assert mock_client.get.call_count <= 1


# ─── extract_api_credentials() ──────────────────────────────────────

class TestExtractApiCredentials:
    """Tests for extract_api_credentials main function."""

    async def test_returns_cached_when_available(self, tmp_cache_file):
        """캐시가 유효하면 캐시된 값을 반환한다."""
        from crawler.api_key_extractor import extract_api_credentials

        data = {
            "api_key": "cached_key_1234567890abcdef12345",
            "hashes": {"StaysSearch": "a" * 64},
            "cached_at": time.time(),
        }
        tmp_cache_file.write_text(json.dumps(data))

        with patch("crawler.api_key_extractor.CACHE_FILE", tmp_cache_file):
            result = await extract_api_credentials(force_refresh=False)

        assert result["api_key"] == "cached_key_1234567890abcdef12345"

    async def test_force_refresh_skips_cache(self, tmp_cache_file):
        """force_refresh=True이면 캐시를 건너뛴다."""
        from crawler.api_key_extractor import extract_api_credentials

        data = {
            "api_key": "cached_key_1234567890abcdef12345",
            "hashes": {},
            "cached_at": time.time(),
        }
        tmp_cache_file.write_text(json.dumps(data))

        with patch("crawler.api_key_extractor.CACHE_FILE", tmp_cache_file), \
             patch("crawler.api_key_extractor._extract_via_httpx") as mock_httpx:
            mock_httpx.return_value = {
                "api_key": "new_key_12345678901234567890abc",
                "hashes": {"StaysSearch": "b" * 64},
            }
            result = await extract_api_credentials(force_refresh=True)

        assert result["api_key"] == "new_key_12345678901234567890abc"
        mock_httpx.assert_awaited_once()

    async def test_falls_back_to_playwright(self, tmp_cache_file):
        """httpx가 실패하면 Playwright로 fallback한다."""
        from crawler.api_key_extractor import extract_api_credentials

        with patch("crawler.api_key_extractor.CACHE_FILE", tmp_cache_file), \
             patch("crawler.api_key_extractor._extract_via_httpx") as mock_httpx, \
             patch("crawler.api_key_extractor._extract_via_playwright") as mock_pw:
            mock_httpx.return_value = {"api_key": "", "hashes": {}}
            mock_pw.return_value = {
                "api_key": "pw_key_123456789012345678901234",
                "hashes": {},
            }
            result = await extract_api_credentials(force_refresh=True)

        assert result["api_key"] == "pw_key_123456789012345678901234"
        mock_pw.assert_awaited_once()

    async def test_playwright_exception_handled(self, tmp_cache_file):
        """Playwright 에러 시 빈 결과를 반환한다."""
        from crawler.api_key_extractor import extract_api_credentials

        with patch("crawler.api_key_extractor.CACHE_FILE", tmp_cache_file), \
             patch("crawler.api_key_extractor._extract_via_httpx") as mock_httpx, \
             patch("crawler.api_key_extractor._extract_via_playwright") as mock_pw:
            mock_httpx.return_value = {"api_key": "", "hashes": {}}
            mock_pw.side_effect = Exception("Playwright not installed")
            result = await extract_api_credentials(force_refresh=True)

        assert result["api_key"] == ""

    async def test_saves_cache_on_success(self, tmp_cache_file):
        """추출 성공 시 캐시에 저장한다."""
        from crawler.api_key_extractor import extract_api_credentials

        with patch("crawler.api_key_extractor.CACHE_FILE", tmp_cache_file), \
             patch("crawler.api_key_extractor._extract_via_httpx") as mock_httpx:
            mock_httpx.return_value = {
                "api_key": "saved_key_12345678901234567890ab",
                "hashes": {"StaysSearch": "a" * 64},
            }
            await extract_api_credentials(force_refresh=True)

        assert tmp_cache_file.exists()
        saved = json.loads(tmp_cache_file.read_text())
        assert saved["api_key"] == "saved_key_12345678901234567890ab"


# ─── _extract_via_playwright() ──────────────────────────────────────

class TestExtractViaPlaywright:
    """Tests for _extract_via_playwright (mocked browser)."""

    def _make_pw_mocks(self, evaluate_rv="", goto_side_effect=None):
        """Playwright mock chain helper."""
        from unittest.mock import AsyncMock as AM

        mock_page = MagicMock()
        mock_page.goto = AM(side_effect=goto_side_effect)
        mock_page.wait_for_timeout = AM()
        if isinstance(evaluate_rv, list):
            mock_page.evaluate = AM(side_effect=evaluate_rv)
        else:
            mock_page.evaluate = AM(return_value=evaluate_rv)
        mock_page.on = MagicMock()

        mock_context = MagicMock()
        mock_context.add_init_script = AM()
        mock_context.new_page = AM(return_value=mock_page)

        mock_browser = MagicMock()
        mock_browser.new_context = AM(return_value=mock_context)
        mock_browser.close = AM()

        mock_chromium = MagicMock()
        mock_chromium.launch = AM(return_value=mock_browser)

        mock_pw = MagicMock()
        mock_pw.chromium = mock_chromium

        # async_playwright() returns context manager
        mock_ap = MagicMock()
        mock_ap.__aenter__ = AM(return_value=mock_pw)
        mock_ap.__aexit__ = AM(return_value=False)

        return mock_ap, mock_page

    async def test_playwright_basic_flow(self):
        """Playwright 기본 흐름 테스트 (API 키 없는 경우)."""
        from crawler.api_key_extractor import _extract_via_playwright

        mock_ap, mock_page = self._make_pw_mocks(evaluate_rv="")

        with patch("playwright.async_api.async_playwright", return_value=mock_ap), \
             patch("pathlib.Path.home", return_value=MagicMock(
                 __truediv__=lambda s, o: MagicMock(
                     exists=MagicMock(return_value=False),
                     __truediv__=lambda s2, o2: MagicMock(exists=MagicMock(return_value=False))
                 )
             )):
            result = await _extract_via_playwright(headless=True)

        assert isinstance(result, dict)
        assert "api_key" in result
        assert "hashes" in result

    async def test_playwright_captures_api_key_from_js(self):
        """JS context에서 API 키를 추출한다."""
        from crawler.api_key_extractor import _extract_via_playwright

        mock_ap, mock_page = self._make_pw_mocks(
            evaluate_rv="d306zoyjsyarp7ifhu67rjxn52tv0t20"
        )

        with patch("playwright.async_api.async_playwright", return_value=mock_ap), \
             patch("pathlib.Path.home", return_value=MagicMock(
                 __truediv__=lambda s, o: MagicMock(
                     exists=MagicMock(return_value=False),
                     __truediv__=lambda s2, o2: MagicMock(exists=MagicMock(return_value=False))
                 )
             )):
            result = await _extract_via_playwright(headless=True)

        assert result["api_key"] == "d306zoyjsyarp7ifhu67rjxn52tv0t20"

    async def test_playwright_error_handling(self):
        """goto 에러 시 빈 결과를 반환한다."""
        from crawler.api_key_extractor import _extract_via_playwright

        mock_ap, _ = self._make_pw_mocks(
            goto_side_effect=Exception("Navigation failed")
        )

        with patch("playwright.async_api.async_playwright", return_value=mock_ap), \
             patch("pathlib.Path.home", return_value=MagicMock(
                 __truediv__=lambda s, o: MagicMock(
                     exists=MagicMock(return_value=False),
                     __truediv__=lambda s2, o2: MagicMock(exists=MagicMock(return_value=False))
                 )
             )):
            result = await _extract_via_playwright(headless=True)

        assert result["api_key"] == ""

    async def test_playwright_scrolls_and_visits_listing(self):
        """스크롤 + 숙소 페이지 방문 경로를 테스트한다."""
        from crawler.api_key_extractor import _extract_via_playwright

        mock_ap, mock_page = self._make_pw_mocks(evaluate_rv=[
            "",                     # API key from JS
            None, None, None,       # 3 scroll calls
            "/rooms/12345678",      # listing link
        ])

        with patch("playwright.async_api.async_playwright", return_value=mock_ap), \
             patch("pathlib.Path.home", return_value=MagicMock(
                 __truediv__=lambda s, o: MagicMock(
                     exists=MagicMock(return_value=False),
                     __truediv__=lambda s2, o2: MagicMock(exists=MagicMock(return_value=False))
                 )
             )):
            result = await _extract_via_playwright(headless=False)

        assert isinstance(result, dict)
        # goto should be called twice: search page + listing page
        assert mock_page.goto.call_count == 2

    async def test_playwright_with_chromium_path(self):
        """chromium 경로가 존재할 때 executable_path를 설정한다."""
        from unittest.mock import AsyncMock as AM
        from crawler.api_key_extractor import _extract_via_playwright
        from pathlib import Path

        mock_ap, mock_page = self._make_pw_mocks(evaluate_rv="key12345678901234567890123456789x")

        mock_chrome = MagicMock(spec=Path)
        mock_chrome.__str__ = MagicMock(return_value="/home/.cache/ms-playwright/chromium-1/chrome-linux/chrome")
        mock_pw_cache = MagicMock()
        mock_pw_cache.exists.return_value = True
        mock_pw_cache.glob.return_value = [mock_chrome]

        mock_home = MagicMock()
        mock_cache = MagicMock()
        mock_cache.__truediv__ = MagicMock(return_value=mock_pw_cache)
        mock_home.__truediv__ = MagicMock(return_value=mock_cache)

        with patch("playwright.async_api.async_playwright", return_value=mock_ap), \
             patch("pathlib.Path.home", return_value=mock_home):
            result = await _extract_via_playwright(headless=True)

        assert result["api_key"] == "key12345678901234567890123456789x"

    async def test_playwright_on_request_handler(self):
        """on_request 핸들러가 API 키와 해시를 캡처한다 (lines 387-407)."""
        import json as json_mod
        from crawler.api_key_extractor import _extract_via_playwright

        mock_ap, mock_page = self._make_pw_mocks(evaluate_rv="")
        captured_handler = None

        def capture_on(event, handler):
            nonlocal captured_handler
            if event == "request":
                captured_handler = handler

        mock_page.on = MagicMock(side_effect=capture_on)

        # After goto, simulate the on_request callback
        original_goto = mock_page.goto

        async def goto_then_fire(*args, **kwargs):
            await original_goto(*args, **kwargs)
            if captured_handler:
                # Fire with a mock request that has API headers + query params
                extensions = json_mod.dumps({
                    "persistedQuery": {
                        "sha256Hash": "c" * 64,
                    }
                })
                mock_request = MagicMock()
                mock_request.url = (
                    "https://www.airbnb.co.kr/api/v3/StaysSearch"
                    f"?operationName=StaysSearch&extensions={extensions}"
                )
                mock_request.headers = {
                    "x-airbnb-api-key": "d306zoyjsyarp7ifhu67rjxn52tv0t20",
                }
                await captured_handler(mock_request)

                # Fire another request without API key (non-API URL)
                mock_request2 = MagicMock()
                mock_request2.url = "https://www.airbnb.co.kr/static/bundle.js"
                mock_request2.headers = {}
                await captured_handler(mock_request2)

                # Fire a bad request that triggers exception in parsing
                mock_request3 = MagicMock()
                mock_request3.url = "https://www.airbnb.co.kr/api/v3/bad?extensions=invalid_json"
                mock_request3.headers = {"x-airbnb-api-key": "d306zoyjsyarp7ifhu67rjxn52tv0t20"}
                await captured_handler(mock_request3)

        from unittest.mock import AsyncMock as AM
        mock_page.goto = AM(side_effect=goto_then_fire)

        with patch("playwright.async_api.async_playwright", return_value=mock_ap), \
             patch("pathlib.Path.home", return_value=MagicMock(
                 __truediv__=lambda s, o: MagicMock(
                     exists=MagicMock(return_value=False),
                     __truediv__=lambda s2, o2: MagicMock(exists=MagicMock(return_value=False))
                 )
             )):
            result = await _extract_via_playwright(headless=True)

        assert result["api_key"] == "d306zoyjsyarp7ifhu67rjxn52tv0t20"
        assert result["hashes"].get("StaysSearch") == "c" * 64


# ─── _extract_via_httpx 인라인 해시 경로 (line 149) ──────────────────

class TestExtractViaHttpxInlineHashes:
    """_extract_via_httpx에서 인라인 HTML 해시 발견 경로."""

    async def test_httpx_finds_inline_hashes(self):
        """HTML 인라인에서 해시를 찾으면 logger.info 호출 (line 149)."""
        from unittest.mock import AsyncMock as AM
        from crawler.api_key_extractor import _extract_via_httpx

        hash_val = "a" * 64
        # All 3 required ops in HTML so _scan_js_bundles early-exits
        html = (
            '"key":"d306zoyjsyarp7ifhu67rjxn52tv0t20"'
            f" name:'StaysSearch' operationId:'{hash_val}'}}"
            f" name:'PdpAvailabilityCalendar' operationId:'{hash_val}'}}"
            f" name:'StaysPdpSections' operationId:'{hash_val}'}}"
        )
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = html

        mock_client = MagicMock()
        mock_client.get = AM(return_value=mock_response)
        mock_client.__aenter__ = AM(return_value=mock_client)
        mock_client.__aexit__ = AM(return_value=False)

        with patch("httpx.AsyncClient", return_value=mock_client):
            result = await _extract_via_httpx()

        assert result["api_key"] == "d306zoyjsyarp7ifhu67rjxn52tv0t20"
        assert "StaysSearch" in result["hashes"]


# ─── _scan_listing_page base64 decode 예외 (lines 226-227) ───────────

class TestScanListingPageDecodeError:
    """_scan_listing_page에서 base64 디코딩 실패 경로."""

    async def test_base64_decode_exception(self):
        """잘못된 base64 문자열은 예외를 잡아 건너뛴다 (lines 226-227)."""
        from unittest.mock import AsyncMock as AM
        from crawler.api_key_extractor import _scan_listing_page

        # /////w== decodes to bytes 0xFFFFFF which fails UTF-8 decode
        search_html = 'RGVtYW5kU3RheUxpc3Rpbmc6/////w=='
        credentials = {"api_key": "key", "hashes": {}}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "nothing_useful"

        mock_client = MagicMock()
        mock_client.get = AM(return_value=mock_response)

        # Should not raise - catches the base64 decode exception
        await _scan_listing_page(mock_client, search_html, credentials)


# ─── _scan_lazy_bundles asyncRequire 에러 + http path (lines 308, 313-314) ──

class TestScanLazyBundlesAsyncRequire:
    """_scan_lazy_bundles asyncRequire 번들 경로 커버리지."""

    async def test_async_require_error(self):
        """asyncRequire 번들 요청 실패 시 예외를 잡는다 (lines 313-314)."""
        from unittest.mock import AsyncMock as AM
        from crawler.api_key_extractor import _scan_lazy_bundles

        # asyncRequire URL 포함
        html = 'src="https://a0.muscache.com/asyncRequire.bundle.js"'
        credentials = {"api_key": "key", "hashes": {}}

        mock_client = MagicMock()
        mock_client.get = AM(side_effect=Exception("Network error"))

        await _scan_lazy_bundles(mock_client, html, credentials)

    async def test_async_require_full_http_path(self):
        """asyncRequire 번들에서 full http URL 경로 (line 308)."""
        from unittest.mock import AsyncMock as AM
        from crawler.api_key_extractor import _scan_lazy_bundles

        html = 'src="https://a0.muscache.com/asyncRequire.bundle.js"'
        credentials = {"api_key": "key", "hashes": {}}

        # asyncRequire 응답에 full http URL이 포함된 JS 파일 참조
        ar_text = '"https://a0.muscache.com/full/RoomCalendar.bundle.js"'
        mock_ar_resp = MagicMock()
        mock_ar_resp.status_code = 200
        mock_ar_resp.text = ar_text

        hash_val = "f" * 64
        mock_lazy_resp = MagicMock()
        mock_lazy_resp.status_code = 200
        mock_lazy_resp.text = f"name:'PdpAvailabilityCalendar' operationId:'{hash_val}'}}"

        async def mock_get(url):
            if "asyncRequire" in url:
                return mock_ar_resp
            return mock_lazy_resp

        mock_client = MagicMock()
        mock_client.get = AM(side_effect=mock_get)

        await _scan_lazy_bundles(mock_client, html, credentials)
        assert credentials["hashes"].get("PdpAvailabilityCalendar") == hash_val

    async def test_lazy_bundle_non_200_status(self):
        """lazy 번들 응답이 200이 아니면 continue한다 (line 323)."""
        from unittest.mock import AsyncMock as AM
        from crawler.api_key_extractor import _scan_lazy_bundles

        html = '<script src="https://a0.muscache.com/RoomCalendar.abc.js"></script>'
        credentials = {"api_key": "key", "hashes": {}}

        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.text = "Not Found"

        mock_client = MagicMock()
        mock_client.get = AM(return_value=mock_response)

        await _scan_lazy_bundles(mock_client, html, credentials)
        assert len(credentials["hashes"]) == 0


# ─── _scan_listing_page → _scan_lazy_bundles 경로 (line 272) ────────

class TestScanListingPageLazyPath:
    """_scan_listing_page에서 lazy bundle 스캔 경로."""

    async def test_scan_listing_triggers_lazy_scan(self):
        """리스팅 페이지에서 missing hashes가 있으면 lazy scan 호출 (line 272)."""
        from unittest.mock import AsyncMock as AM
        from crawler.api_key_extractor import _scan_listing_page

        search_html = '"propertyId":"12345678"'
        credentials = {"api_key": "key", "hashes": {"StaysSearch": "a" * 64}}

        hash_val = "b" * 64
        # listing_html has a lazy-loaded RoomCalendar URL but no src= JS bundle
        listing_html = (
            '<html>https://a0.muscache.com/RoomCalendar.xyz.js</html>'
        )
        lazy_js = f"name:'PdpAvailabilityCalendar' operationId:'{hash_val}'}}"

        async def mock_get(url, **kwargs):
            resp = MagicMock()
            resp.status_code = 200
            if "/rooms/" in url:
                resp.text = listing_html
            else:
                resp.text = lazy_js
            return resp

        mock_client = MagicMock()
        mock_client.get = AM(side_effect=mock_get)

        await _scan_listing_page(mock_client, search_html, credentials)
        assert "PdpAvailabilityCalendar" in credentials["hashes"]


# ─── CLI __main__ block (lines 548-570) ──────────────────────────────

class TestApiKeyExtractorCLI:
    """api_key_extractor.py __main__ block (lines 548-570)."""

    def test_cli_execution(self, capsys):
        """CLI 블록이 runpy로 실행된다."""
        import runpy

        mock_creds = {
            "api_key": "test_key_12345678901234567890abcd",
            "hashes": {"StaysSearch": "a" * 64},
        }

        with patch("asyncio.run", return_value=mock_creds) as mock_run, \
             patch("sys.argv", ["api_key_extractor.py"]):
            try:
                runpy.run_module(
                    "crawler.api_key_extractor",
                    run_name="__main__",
                    alter_sys=True,
                )
            except SystemExit:
                pass

        mock_run.assert_called_once()
        captured = capsys.readouterr()
        assert "API Key: test_key_12345678901234567890abcd" in captured.out

    def test_cli_with_visible_flag(self, capsys):
        """--visible, --force 플래그 파싱."""
        import runpy

        mock_creds = {
            "api_key": "test_key_12345678901234567890abcd",
            "hashes": {},
        }

        with patch("asyncio.run", return_value=mock_creds) as mock_run, \
             patch("sys.argv", ["api_key_extractor.py", "--visible", "--force"]):
            try:
                runpy.run_module(
                    "crawler.api_key_extractor",
                    run_name="__main__",
                    alter_sys=True,
                )
            except SystemExit:
                pass

        mock_run.assert_called_once()
        captured = capsys.readouterr()
        assert "headless: False" in captured.out
        assert "force refresh: True" in captured.out
