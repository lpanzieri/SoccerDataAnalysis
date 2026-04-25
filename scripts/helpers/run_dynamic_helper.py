#!/usr/bin/env python3
"""Run dynamic helper resolution/execution from a natural language question."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.helpers.dynamic_helper_manager import answer_question_with_helpers
from scripts.helpers.league_records import DBConfig


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Resolve/create helper for a question and execute it")
    p.add_argument("--question", required=True)
    p.add_argument("--host", default=os.getenv("MYSQL_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.getenv("MYSQL_PORT", "3306")))
    p.add_argument("--user", default=os.getenv("MYSQL_USER", "football_admin"))
    p.add_argument("--database", default=os.getenv("MYSQL_DATABASE", "historic_football_data"))
    p.add_argument("--mysql-password-env", default="MYSQL_PASSWORD")
    p.add_argument("--cache-ttl-seconds", type=int, default=None)
    p.add_argument("--no-cache", action="store_true", help="Disable MySQL response cache for this run")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    db = DBConfig(
        host=args.host,
        port=args.port,
        user=args.user,
        password=os.getenv(args.mysql_password_env, ""),
        database=args.database,
    )

    use_cache = None if not args.no_cache else False

    result = answer_question_with_helpers(
        question=args.question,
        db=db,
        use_cache=use_cache,
        cache_ttl_seconds=args.cache_ttl_seconds,
    )
    print(json.dumps(result, indent=2, ensure_ascii=True))


if __name__ == "__main__":
    main()
