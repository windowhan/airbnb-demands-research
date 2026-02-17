"""
프록시 관리자 - 옵션 A(프록시 없음) ~ 옵션 C(10+개 프록시 로테이션) 지원

기능:
- 프록시 풀 관리 (파일/환경변수에서 로드)
- IP당 요청 카운터 기반 자동 로테이션
- 차단된 프록시 쿨다운 관리
- 프록시 없이도 동작 (옵션 A)
"""

import logging
import time
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class ProxyState:
    """개별 프록시의 상태를 추적합니다."""
    url: str
    request_count: int = 0
    total_requests: int = 0
    blocked_count: int = 0
    last_used: float = 0.0
    cooldown_until: float = 0.0     # 차단 후 쿨다운 종료 시각
    is_healthy: bool = True

    def reset_count(self):
        self.request_count = 0

    def mark_blocked(self, cooldown_seconds: int = 300):
        """프록시가 차단되었음을 기록합니다."""
        self.blocked_count += 1
        self.cooldown_until = time.time() + cooldown_seconds
        self.is_healthy = False
        logger.warning(
            "Proxy blocked: %s (total blocks: %d, cooldown: %ds)",
            self.url[:30] + "...", self.blocked_count, cooldown_seconds,
        )

    def is_available(self) -> bool:
        """프록시가 현재 사용 가능한지 확인합니다."""
        if time.time() > self.cooldown_until:
            self.is_healthy = True
        return self.is_healthy


class ProxyManager:
    """
    프록시 로테이션 관리자.

    옵션 A: 프록시 없이 직접 연결 (proxy_required=False)
    옵션 B/C: 프록시 풀에서 라운드 로빈 + 차단 감지 시 자동 교체
    """

    def __init__(self, proxy_urls: list[str] | None = None,
                 requests_per_rotate: int = 30,
                 block_cooldown: int = 300):
        self._proxies: list[ProxyState] = []
        self._current_index: int = 0
        self._requests_per_rotate = requests_per_rotate
        self._block_cooldown = block_cooldown

        if proxy_urls:
            for url in proxy_urls:
                url = url.strip()
                if url:
                    self._proxies.append(ProxyState(url=url))
            logger.info("ProxyManager initialized with %d proxies", len(self._proxies))

    @classmethod
    def from_config(cls) -> "ProxyManager":
        """설정 파일에서 프록시 매니저를 생성합니다."""
        from config.settings import (
            PROXY_LIST_ENV,
            PROXY_LIST_FILE,
            get_tier_config,
        )

        tier = get_tier_config()
        proxy_urls = []

        # 환경변수에서 로드
        if PROXY_LIST_ENV:
            proxy_urls.extend(PROXY_LIST_ENV.split(","))

        # 파일에서 로드
        proxy_file = Path(PROXY_LIST_FILE)
        if proxy_file.exists():
            with open(proxy_file) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        proxy_urls.append(line)

        if tier["proxy_required"] and not proxy_urls:
            logger.warning(
                "Tier %s requires proxies but none configured! "
                "Set PROXY_LIST env var or create config/proxies.txt",
                tier,
            )

        return cls(
            proxy_urls=proxy_urls,
            requests_per_rotate=tier["requests_per_ip_before_rotate"],
        )

    @property
    def has_proxies(self) -> bool:
        return len(self._proxies) > 0

    @property
    def available_count(self) -> int:
        return sum(1 for p in self._proxies if p.is_available())

    def get_proxy(self) -> str | None:
        """
        다음 사용할 프록시 URL을 반환합니다.
        프록시가 없으면 None (직접 연결).
        """
        if not self._proxies:
            return None

        # 사용 가능한 프록시 찾기
        attempts = 0
        while attempts < len(self._proxies):
            proxy = self._proxies[self._current_index]

            if proxy.is_available():
                # 로테이션 카운터 체크
                if proxy.request_count >= self._requests_per_rotate:
                    proxy.reset_count()
                    self._rotate()
                    continue

                proxy.request_count += 1
                proxy.total_requests += 1
                proxy.last_used = time.time()
                return proxy.url

            # 이 프록시 사용 불가 → 다음으로
            self._rotate()
            attempts += 1

        logger.error("No available proxies! All %d proxies are in cooldown.", len(self._proxies))
        return None

    def report_success(self):
        """현재 프록시의 요청 성공을 기록합니다."""
        if self._proxies:
            self._proxies[self._current_index].is_healthy = True

    def report_blocked(self):
        """현재 프록시가 차단되었음을 보고하고 다음 프록시로 전환합니다."""
        if self._proxies:
            self._proxies[self._current_index].mark_blocked(self._block_cooldown)
            self._rotate()

    def get_stats(self) -> dict:
        """프록시 풀 상태 요약을 반환합니다."""
        return {
            "total": len(self._proxies),
            "available": self.available_count,
            "proxies": [
                {
                    "index": i,
                    "requests": p.total_requests,
                    "blocked": p.blocked_count,
                    "healthy": p.is_healthy,
                }
                for i, p in enumerate(self._proxies)
            ],
        }

    def _rotate(self):
        """다음 프록시로 인덱스를 이동합니다."""
        if self._proxies:
            self._current_index = (self._current_index + 1) % len(self._proxies)
