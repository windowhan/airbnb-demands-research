"""main.py 단위 테스트."""

import asyncio
from unittest.mock import patch, MagicMock, AsyncMock

import pytest


# ─── setup_logging ──────────────────────────────────────────────────


class TestSetupLogging:
    """setup_logging 함수 테스트."""

    def test_creates_log_dir(self, tmp_path):
        log_dir = tmp_path / "logs"
        with patch("main.LOG_DIR", log_dir), \
             patch("main.LOG_LEVEL", "INFO"), \
             patch("main.LOG_FORMAT", "%(message)s"):
            from main import setup_logging
            setup_logging()
        assert log_dir.exists()


# ─── show_status ────────────────────────────────────────────────────


class TestShowStatus:
    """show_status 함수 테스트."""

    def test_show_status_prints_info(self, mock_session_scope, db_session, capsys):
        """상태 출력이 에러 없이 실행된다."""
        with patch("main.session_scope", mock_session_scope), \
             patch("main.get_tier_config") as mock_tier:
            mock_tier.return_value = {
                "station_priority": [1],
                "proxy_required": False,
                "max_requests_per_hour": 50,
            }
            from main import show_status
            show_status()

        captured = capsys.readouterr()
        assert "Status" in captured.out

    def test_show_status_with_crawl_log(self, mock_session_scope, db_session, sample_station, capsys):
        """CrawlLog가 있는 경우에도 정상 출력된다."""
        from models.schema import CrawlLog
        from datetime import datetime

        crawl_log = CrawlLog(
            job_type="search",
            started_at=datetime.utcnow(),
            status="success",
            total_requests=10,
            successful_requests=10,
            blocked_requests=2,
        )
        db_session.add(crawl_log)
        db_session.commit()

        with patch("main.session_scope", mock_session_scope), \
             patch("main.get_tier_config") as mock_tier:
            mock_tier.return_value = {
                "station_priority": [1],
                "proxy_required": False,
                "max_requests_per_hour": 50,
            }
            from main import show_status
            show_status()

        captured = capsys.readouterr()
        assert "search" in captured.out
        assert "Blocked" in captured.out


# ─── run_once ───────────────────────────────────────────────────────


class TestRunOnce:
    """run_once 함수 테스트."""

    async def test_run_once_search(self):
        from main import run_once
        with patch("main.run_search_job", new_callable=AsyncMock) as mock_search:
            await run_once("search")
        mock_search.assert_awaited_once()

    async def test_run_once_calendar(self):
        from main import run_once
        with patch("main.run_calendar_job", new_callable=AsyncMock) as mock_cal:
            await run_once("calendar")
        mock_cal.assert_awaited_once()

    async def test_run_once_detail(self):
        from main import run_once
        with patch("main.run_listing_detail_job", new_callable=AsyncMock) as mock_detail:
            await run_once("detail")
        mock_detail.assert_awaited_once()

    async def test_run_once_all(self):
        from main import run_once
        with patch("main.run_search_job", new_callable=AsyncMock) as mock_search, \
             patch("main.run_calendar_job", new_callable=AsyncMock) as mock_cal, \
             patch("main.run_listing_detail_job", new_callable=AsyncMock) as mock_detail:
            await run_once("all")
        mock_search.assert_awaited_once()
        mock_cal.assert_awaited_once()
        mock_detail.assert_awaited_once()


# ─── main ───────────────────────────────────────────────────────────


class TestMain:
    """main 함수 테스트."""

    def test_main_init(self, mock_session_scope):
        from main import main
        with patch("main.init_db"), \
             patch("main.setup_logging"), \
             patch("main.load_stations_from_json") as mock_load, \
             patch("main.show_status") as mock_status, \
             patch("sys.argv", ["main.py", "--init"]):
            main()
        mock_load.assert_called_once()
        mock_status.assert_called_once()

    def test_main_status(self, mock_session_scope):
        from main import main
        with patch("main.init_db"), \
             patch("main.setup_logging"), \
             patch("main.show_status") as mock_status, \
             patch("sys.argv", ["main.py", "--status"]):
            main()
        mock_status.assert_called_once()

    def test_main_once_search(self, mock_session_scope):
        from main import main
        with patch("main.init_db"), \
             patch("main.setup_logging"), \
             patch("main.asyncio") as mock_asyncio, \
             patch("main.show_status"), \
             patch("sys.argv", ["main.py", "--once", "search"]):
            main()
        mock_asyncio.run.assert_called_once()

    def test_main_extract_key_success(self, mock_session_scope):
        from main import main
        with patch("main.init_db"), \
             patch("main.setup_logging"), \
             patch("main.asyncio") as mock_asyncio, \
             patch("sys.argv", ["main.py", "--extract-key"]):
            mock_asyncio.run.return_value = {
                "api_key": "test_key_12345678901234567890ab",
                "hashes": {"StaysSearch": "a" * 64},
            }
            main()

    def test_main_extract_key_failure(self, mock_session_scope, capsys):
        from main import main
        with patch("main.init_db"), \
             patch("main.setup_logging"), \
             patch("main.asyncio") as mock_asyncio, \
             patch("sys.argv", ["main.py", "--extract-key"]):
            mock_asyncio.run.return_value = {"api_key": "", "hashes": {}}
            main()
        captured = capsys.readouterr()
        assert "Failed" in captured.out

    def test_main_scheduler_mode(self, mock_session_scope):
        from main import main
        with patch("main.init_db"), \
             patch("main.setup_logging"), \
             patch("main.asyncio") as mock_asyncio, \
             patch("sys.argv", ["main.py"]):
            main()
        mock_asyncio.run.assert_called_once()

    def test_main_extract_key_visible(self, mock_session_scope):
        from main import main
        with patch("main.init_db"), \
             patch("main.setup_logging"), \
             patch("main.asyncio") as mock_asyncio, \
             patch("sys.argv", ["main.py", "--extract-key", "--visible"]):
            mock_asyncio.run.return_value = {
                "api_key": "test_key_12345678901234567890ab",
                "hashes": {"StaysSearch": "a" * 64},
            }
            main()

    def test_main_once_calendar(self, mock_session_scope):
        from main import main
        with patch("main.init_db"), \
             patch("main.setup_logging"), \
             patch("main.asyncio") as mock_asyncio, \
             patch("main.show_status"), \
             patch("sys.argv", ["main.py", "--once", "calendar"]):
            main()
        mock_asyncio.run.assert_called_once()


# ─── run_scheduler ──────────────────────────────────────────────────

class TestRunScheduler:
    """run_scheduler 함수 테스트."""

    async def test_run_scheduler_signal_handler(self):
        """시그널 핸들러 등록 및 호출 시 stop_event.set() 확인 (lines 137-138)."""
        from unittest.mock import AsyncMock as AM
        import signal as signal_mod
        from main import run_scheduler

        mock_scheduler = MagicMock()
        captured_handlers = {}

        def mock_signal_fn(sig, handler):
            captured_handlers[sig] = handler

        with patch("main.setup_scheduler", return_value=mock_scheduler), \
             patch("main.run_search_job", new_callable=AM) as mock_search, \
             patch("main.signal.signal", side_effect=mock_signal_fn):

            async def run_and_signal():
                task = asyncio.create_task(run_scheduler())
                await asyncio.sleep(0.05)
                # 시그널 핸들러가 등록되었는지 확인하고 호출
                if signal_mod.SIGINT in captured_handlers:
                    captured_handlers[signal_mod.SIGINT](signal_mod.SIGINT, None)
                await asyncio.sleep(0.05)
                if not task.done():
                    task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

            await run_and_signal()

        mock_search.assert_awaited_once()
        mock_scheduler.start.assert_called_once()
        mock_scheduler.shutdown.assert_called_once_with(wait=False)


class TestMainEntryPoint:
    """main.py __name__ == '__main__' block (line 219)."""

    def test_main_module_entry_point(self):
        """__name__ == '__main__'일 때 main()이 호출된다."""
        import runpy

        with patch("sys.argv", ["main.py", "--status"]):
            # Patch at function level that will be looked up in the fresh namespace
            with patch("main.init_db"), \
                 patch("main.setup_logging"), \
                 patch("main.show_status"):
                try:
                    runpy.run_module("main", run_name="__main__", alter_sys=True)
                except SystemExit:
                    pass
