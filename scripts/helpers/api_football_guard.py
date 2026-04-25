from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

API_BASE = "https://v3.football.api-sports.io"


def header_value(headers: Dict[str, str], name: str) -> Optional[str]:
    target = name.lower()
    for k, v in headers.items():
        if str(k).lower() == target:
            return str(v)
    return None


def header_int(headers: Dict[str, str], names: Tuple[str, ...]) -> Optional[int]:
    for name in names:
        raw = header_value(headers, name)
        if raw is None:
            continue
        raw = raw.strip()
        if not raw:
            continue
        try:
            return int(float(raw))
        except ValueError:
            continue
    return None


def retry_after_seconds(headers: Dict[str, str], default_seconds: int = 60) -> int:
    retry_after = header_int(headers, ("retry-after",))
    if retry_after is not None and retry_after > 0:
        return retry_after

    reset_epoch = header_int(headers, ("x-ratelimit-requests-reset", "x-ratelimit-reset"))
    if reset_epoch is not None:
        now_epoch = int(time.time())
        return max(1, reset_epoch - now_epoch)

    return max(1, int(default_seconds))


@dataclass
class ApiGuard:
    user_agent: str
    min_interval_seconds: float = 0.35
    max_retry_after_seconds: int = 3600
    remaining_reserve: int = 10
    log_enabled: bool = True
    _last_call_at: float = 0.0

    @classmethod
    def from_env(cls, user_agent: str) -> "ApiGuard":
        return cls(
            user_agent=user_agent,
            min_interval_seconds=float(os.getenv("APIFOOTBALL_MIN_INTERVAL_SECONDS", "0.35")),
            max_retry_after_seconds=int(os.getenv("APIFOOTBALL_MAX_RETRY_AFTER_SECONDS", "3600")),
            remaining_reserve=int(os.getenv("APIFOOTBALL_REMAINING_RESERVE", "10")),
            log_enabled=os.getenv("APIFOOTBALL_GUARD_LOG", "1").strip().lower() not in {"0", "false", "no", "off"},
        )

    def _log(self, line: str) -> None:
        if self.log_enabled:
            print(line)

    def log_call(self, path: str, code: int, headers: Dict[str, str], action: str) -> None:
        remaining = header_int(headers, ("x-ratelimit-requests-remaining",))
        limit = header_int(headers, ("x-ratelimit-requests-limit", "x-ratelimit-limit", "x-ratelimit-requests"))
        reset_epoch = header_int(headers, ("x-ratelimit-requests-reset", "x-ratelimit-reset"))
        self._log(
            "[API-GUARD] "
            f"path={path} code={code} remaining={remaining} limit={limit} reset={reset_epoch} action={action}"
        )

    def wait_before_call(self) -> None:
        now = time.time()
        elapsed = now - self._last_call_at
        wait_for = max(0.0, self.min_interval_seconds - elapsed)
        if wait_for > 0:
            time.sleep(wait_for)

    def note_call(self) -> None:
        self._last_call_at = time.time()

    def should_stop_from_remaining(self, headers: Dict[str, str]) -> bool:
        remaining = header_int(headers, ("x-ratelimit-requests-remaining",))
        if remaining is None:
            return False
        return remaining <= self.remaining_reserve

    def sleep_retry_window(self, headers: Dict[str, str]) -> int:
        seconds = min(self.max_retry_after_seconds, retry_after_seconds(headers, default_seconds=60))
        time.sleep(max(1, seconds))
        return seconds


def api_get_json(
    guard: ApiGuard,
    api_key: str,
    path: str,
    params: Dict[str, str],
    timeout_seconds: int = 30,
) -> Tuple[int, Dict, Dict[str, str]]:
    guard.wait_before_call()
    query = urllib.parse.urlencode(params)
    url = f"{API_BASE}{path}?{query}"
    req = urllib.request.Request(
        url,
        headers={
            "x-apisports-key": api_key,
            "Accept": "application/json",
            "User-Agent": guard.user_agent,
        },
        method="GET",
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
            headers = dict(resp.headers)
            guard.note_call()
            guard.log_call(path=path, code=resp.getcode(), headers=headers, action="continue")
            return resp.getcode(), payload, headers
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        headers = dict(exc.headers)
        guard.note_call()
        guard.log_call(path=path, code=exc.code, headers=headers, action=("backoff" if exc.code == 429 else "error"))
        return exc.code, {"error": body}, headers
