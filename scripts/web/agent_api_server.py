#!/usr/bin/env python3
"""Minimal HTTP bridge for external AI agents and website frontends.

Endpoints:
- GET /health
- POST /v1/question

This server wraps the existing dynamic helper pipeline and can optionally
run a small API freshness sync before answering.
"""

from __future__ import annotations

import argparse
import json
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Optional, Tuple

from scripts.helpers.dynamic_helper_manager import (
    answer_question_with_helpers,
    fetch_and_insert_missing_data_from_api,
    infer_league_code,
)
from scripts.helpers.league_records import DBConfig


class AgentApiHandler(BaseHTTPRequestHandler):
    db: DBConfig
    allowed_origin: str
    required_token: str
    rate_limit_rpm: int
    _rate_limit_lock = threading.Lock()
    _rate_limit_state: Dict[str, Tuple[float, int]] = {}

    def _send_json(
        self,
        status: int,
        payload: Dict[str, Any],
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> None:
        body = json.dumps(payload, ensure_ascii=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", self.allowed_origin)
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        if extra_headers:
            for key, value in extra_headers.items():
                self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)

    def _extract_bearer_token(self) -> Optional[str]:
        auth = self.headers.get("Authorization", "")
        if not auth.lower().startswith("bearer "):
            return None
        token = auth[7:].strip()
        return token if token else None

    def _authorize_request(self) -> Tuple[bool, Optional[str]]:
        if not self.required_token:
            return True, "anonymous"

        token = self._extract_bearer_token()
        if not token or token != self.required_token:
            self._send_json(401, {"ok": False, "error": "unauthorized"})
            return False, None
        return True, "token"

    def _check_rate_limit(self, identity: str) -> Tuple[bool, int]:
        rpm = int(self.rate_limit_rpm)
        if rpm <= 0:
            return True, 0

        now = time.time()
        window_start = now - (now % 60)
        with self._rate_limit_lock:
            state = self._rate_limit_state.get(identity)
            if not state or state[0] != window_start:
                self._rate_limit_state[identity] = (window_start, 1)
                return True, 0

            count = state[1]
            if count >= rpm:
                retry_after = max(1, int(60 - (now - window_start)))
                return False, retry_after

            self._rate_limit_state[identity] = (window_start, count + 1)
            return True, 0

    def _read_json(self) -> Dict[str, Any]:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = 0
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        if not raw:
            return {}
        parsed = json.loads(raw.decode("utf-8"))
        if not isinstance(parsed, dict):
            raise ValueError("JSON payload must be an object")
        return parsed

    def _run_optional_freshness_sync(
        self,
        question: str,
        local_only: bool,
        freshness_days_back: int,
    ) -> Dict[str, Any]:
        if local_only:
            return {
                "mode": "local_only",
                "performed": False,
                "reason": "explicit_local_only",
            }

        league_code = infer_league_code(question)
        if not league_code:
            return {
                "mode": "api_check",
                "performed": False,
                "reason": "league_not_inferred_from_question",
            }

        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=max(1, int(freshness_days_back)))

        fetch_and_insert_missing_data_from_api(
            db=self.db,
            league_code=league_code,
            from_date=start_date.isoformat(),
            to_date=end_date.isoformat(),
        )
        return {
            "mode": "api_check",
            "performed": True,
            "league_code": league_code,
            "from_date": start_date.isoformat(),
            "to_date": end_date.isoformat(),
        }

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", self.allowed_origin)
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/health":
            self._send_json(
                200,
                {
                    "ok": True,
                    "service": "agent_api_server",
                    "time_utc": datetime.now(timezone.utc).isoformat(),
                },
            )
            return
        self._send_json(404, {"ok": False, "error": "not_found"})

    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/v1/question":
            self._send_json(404, {"ok": False, "error": "not_found"})
            return

        authorized, identity = self._authorize_request()
        if not authorized:
            return

        rate_key = f"{identity}:{self.client_address[0]}"
        allowed, retry_after = self._check_rate_limit(rate_key)
        if not allowed:
            self._send_json(
                429,
                {"ok": False, "error": "rate_limited", "retry_after_seconds": retry_after},
                extra_headers={"Retry-After": str(retry_after)},
            )
            return

        try:
            payload = self._read_json()
            question = str(payload.get("question", "")).strip()
            if not question:
                self._send_json(400, {"ok": False, "error": "question_is_required"})
                return

            local_only = bool(payload.get("local_only", False))
            no_cache = bool(payload.get("no_cache", False))
            cache_ttl_seconds_raw: Optional[Any] = payload.get("cache_ttl_seconds")
            freshness_days_back = int(payload.get("freshness_days_back", 3))

            cache_ttl_seconds: Optional[int]
            if cache_ttl_seconds_raw is None:
                cache_ttl_seconds = None
            else:
                cache_ttl_seconds = int(cache_ttl_seconds_raw)

            freshness = self._run_optional_freshness_sync(
                question=question,
                local_only=local_only,
                freshness_days_back=freshness_days_back,
            )

            answer = answer_question_with_helpers(
                question=question,
                db=self.db,
                use_cache=(False if no_cache else None),
                cache_ttl_seconds=cache_ttl_seconds,
            )

            self._send_json(
                200,
                {
                    "ok": True,
                    "freshness": freshness,
                    "answer": answer,
                },
            )
        except Exception as exc:  # pragma: no cover
            self._send_json(
                500,
                {
                    "ok": False,
                    "error": "internal_error",
                    "detail": str(exc),
                },
            )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="HTTP wrapper for dynamic helper pipeline")
    p.add_argument("--bind", default=os.getenv("AGENT_API_BIND", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.getenv("AGENT_API_PORT", "8081")))
    p.add_argument("--allowed-origin", default=os.getenv("AGENT_API_ALLOWED_ORIGIN", "*"))
    p.add_argument("--api-token-env", default="AGENT_API_TOKEN")
    p.add_argument("--rate-limit-rpm", type=int, default=int(os.getenv("AGENT_API_RATE_LIMIT_RPM", "60")))

    p.add_argument("--host", default=os.getenv("MYSQL_HOST", "127.0.0.1"))
    p.add_argument("--db-port", type=int, default=int(os.getenv("MYSQL_PORT", "3306")))
    p.add_argument("--user", default=os.getenv("MYSQL_USER", "football_admin"))
    p.add_argument("--database", default=os.getenv("MYSQL_DATABASE", "historic_football_data"))
    p.add_argument("--mysql-password-env", default="MYSQL_PASSWORD")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    db = DBConfig(
        host=args.host,
        port=args.db_port,
        user=args.user,
        password=os.getenv(args.mysql_password_env, ""),
        database=args.database,
    )

    AgentApiHandler.db = db
    AgentApiHandler.allowed_origin = args.allowed_origin
    AgentApiHandler.required_token = os.getenv(args.api_token_env, "")
    AgentApiHandler.rate_limit_rpm = args.rate_limit_rpm

    server = ThreadingHTTPServer((args.bind, args.port), AgentApiHandler)
    print(f"Serving agent API on http://{args.bind}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
