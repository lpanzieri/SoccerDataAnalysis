#!/usr/bin/env python3
"""CLI example for the league records helpers.

Examples:
  python scripts/helpers/run_league_records_example.py --record longest_streak --league-code E0
  python scripts/helpers/run_league_records_example.py --record most_goals --league-code I1
  python scripts/helpers/run_league_records_example.py --record most_points --league-code I1
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

# Allow running the file directly: python scripts/helpers/run_league_records_example.py
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.helpers.league_records import (
    DBConfig,
    get_longest_title_streak,
    get_most_goals_in_season,
    get_most_points_in_season,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run reusable league records queries")
    p.add_argument(
        "--record",
        required=True,
        choices=["longest_streak", "most_goals", "most_points"],
        help="Which helper query to run",
    )
    p.add_argument("--league-code", default=None, help="League code like E0, I1 (optional)")
    p.add_argument("--host", default=os.getenv("MYSQL_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.getenv("MYSQL_PORT", "3306")))
    p.add_argument("--user", default=os.getenv("MYSQL_USER", "football_admin"))
    p.add_argument("--database", default=os.getenv("MYSQL_DATABASE", "historic_football_data"))
    p.add_argument("--mysql-password-env", default="MYSQL_PASSWORD")
    p.add_argument("--points-for-win", type=int, default=3)
    p.add_argument("--points-for-draw", type=int, default=1)
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

    rows: List[Dict[str, Any]]
    if args.record == "longest_streak":
        if not args.league_code:
            raise SystemExit("--league-code is required for --record longest_streak")
        rows = get_longest_title_streak(
            db=db,
            league_code=args.league_code,
            points_for_win=args.points_for_win,
            points_for_draw=args.points_for_draw,
        )
    elif args.record == "most_goals":
        rows = get_most_goals_in_season(db=db, league_code=args.league_code)
    else:
        rows = get_most_points_in_season(
            db=db,
            league_code=args.league_code,
            points_for_win=args.points_for_win,
            points_for_draw=args.points_for_draw,
        )

    print(json.dumps(rows, indent=2, ensure_ascii=True))


if __name__ == "__main__":
    main()
