#!/usr/bin/env python3
"""Build a targeted retry schedule for major-5 league-season gaps.

This tool is read-only against source coverage tables and writes a CSV that can be
loaded into backfill_progress_tracker. It is intended for post-pass reconciliation
where only league-season slices with remaining gaps are retried.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import mysql.connector


MAJOR5 = [
    {"league_code": "E0", "league_name": "Premier League", "api_league_id": 39, "priority": 1},
    {"league_code": "SP1", "league_name": "La Liga", "api_league_id": 140, "priority": 2},
    {"league_code": "I1", "league_name": "Serie A", "api_league_id": 135, "priority": 3},
    {"league_code": "D1", "league_name": "Bundesliga", "api_league_id": 78, "priority": 4},
    {"league_code": "F1", "league_name": "Ligue 1", "api_league_id": 61, "priority": 5},
]

FINISHED_STATUS = ("FT", "AET", "PEN", "FT_PEN", "AWD", "WO")


@dataclass
class RetryItem:
    league_code: str
    league_name: str
    api_league_id: int
    start_year: int
    missing_timeline: int
    missing_player_stats: int


def guard_unsafe_secret_flags() -> None:
    for arg in sys.argv[1:]:
        if arg == "--password" or arg.startswith("--password="):
            raise SystemExit("Unsafe secret flag detected. Use environment variable MYSQL_PASSWORD.")


def parse_args() -> argparse.Namespace:
    guard_unsafe_secret_flags()
    p = argparse.ArgumentParser(description="Build major-5 retry schedule from current DB gaps")
    p.add_argument("--host", default=os.getenv("MYSQL_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.getenv("MYSQL_PORT", "3306")))
    p.add_argument("--user", default=os.getenv("MYSQL_USER", "football_admin"))
    p.add_argument("--database", default=os.getenv("MYSQL_DATABASE", "historic_football_data"))
    p.add_argument("--mysql-password-env", default="MYSQL_PASSWORD")

    p.add_argument("--min-start-year", type=int, default=2016)
    p.add_argument(
        "--max-start-year",
        type=int,
        default=0,
        help="Upper bound year. Use 0 to auto-resolve latest season year in event_fixture for major-5.",
    )
    p.add_argument("--daily-limit", type=int, default=75000)
    p.add_argument("--max-batch-calls", type=int, default=1000)
    p.add_argument(
        "--retry-mode",
        choices=["both", "events", "players"],
        default="both",
        help="Which gaps qualify a slice for retry scheduling.",
    )
    p.add_argument("--csv-out", default="plans/major5_retry_schedule.csv")
    return p.parse_args()


def connect_db(args: argparse.Namespace):
    mysql_password = os.getenv(args.mysql_password_env, "")
    return mysql.connector.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=mysql_password,
        database=args.database,
        charset="utf8mb4",
        use_unicode=True,
        use_pure=True,
        autocommit=True,
    )


def resolve_max_year(conn, min_year: int, max_year: int) -> int:
    if max_year > 0:
        return max_year
    cur = conn.cursor()
    try:
        cur.execute("SELECT MAX(season_year) FROM event_fixture WHERE league_id IN (39, 140, 135, 78, 61)")
        row = cur.fetchone()
        if not row or row[0] is None:
            return min_year
        return max(min_year, int(row[0]))
    finally:
        cur.close()


def fetch_retry_items(conn, min_year: int, max_year: int, retry_mode: str) -> List[RetryItem]:
    cur = conn.cursor()
    placeholders = ",".join(["%s"] * len(FINISHED_STATUS))
    by_api = {int(x["api_league_id"]): x for x in MAJOR5}

    sql = f"""
        SELECT
            ef.league_id,
            ef.season_year,
            SUM(CASE WHEN ef.status_short IN ({placeholders}) THEN 1 ELSE 0 END) AS finished_fixtures,
            COUNT(DISTINCT CASE WHEN et.provider_fixture_id IS NOT NULL THEN ef.provider_fixture_id END) AS fixtures_with_timeline,
            COUNT(DISTINCT CASE WHEN pms.provider_fixture_id IS NOT NULL THEN ef.provider_fixture_id END) AS fixtures_with_player_stats
        FROM event_fixture ef
        LEFT JOIN event_timeline et ON et.provider_fixture_id = ef.provider_fixture_id
        LEFT JOIN player_match_stats pms ON pms.provider_fixture_id = ef.provider_fixture_id
        WHERE ef.league_id IN (39, 140, 135, 78, 61)
          AND ef.season_year BETWEEN %s AND %s
        GROUP BY ef.league_id, ef.season_year
        ORDER BY ef.season_year DESC, ef.league_id ASC
    """

    cur.execute(sql, tuple(list(FINISHED_STATUS) + [min_year, max_year]))
    rows = cur.fetchall()
    cur.close()

    out: List[RetryItem] = []
    for league_id, start_year, finished, with_timeline, with_player_stats in rows:
        league_id = int(league_id)
        meta = by_api.get(league_id)
        if not meta:
            continue

        finished_i = int(finished or 0)
        with_timeline_i = int(with_timeline or 0)
        with_player_i = int(with_player_stats or 0)
        missing_timeline = max(0, finished_i - with_timeline_i)
        missing_player = max(0, finished_i - with_player_i)

        include = False
        if retry_mode == "both":
            include = (missing_timeline > 0) or (missing_player > 0)
        elif retry_mode == "events":
            include = missing_timeline > 0
        else:
            include = missing_player > 0

        if not include:
            continue

        out.append(
            RetryItem(
                league_code=str(meta["league_code"]),
                league_name=str(meta["league_name"]),
                api_league_id=league_id,
                start_year=int(start_year),
                missing_timeline=missing_timeline,
                missing_player_stats=missing_player,
            )
        )

    out.sort(key=lambda r: (-r.start_year, next(x["priority"] for x in MAJOR5 if x["league_code"] == r.league_code)))
    return out


def split_item_calls(item_calls: int, max_batch_calls: int) -> List[int]:
    remaining = max(1, int(item_calls))
    chunks: List[int] = []
    while remaining > 0:
        take = min(remaining, max_batch_calls)
        chunks.append(take)
        remaining -= take
    return chunks


def schedule_rows(
    items: List[RetryItem],
    daily_limit: int,
    max_batch_calls: int,
    retry_mode: str,
) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    day = 1
    day_calls = 0
    seq = 1

    for item in items:
        suggested_calls = 1 + max(item.missing_timeline, item.missing_player_stats)
        chunks = split_item_calls(suggested_calls, max_batch_calls)

        for idx, chunk_calls in enumerate(chunks, start=1):
            if day_calls + chunk_calls > daily_limit:
                day += 1
                day_calls = 0

            item_type = "league_season_retry"
            if len(chunks) > 1:
                item_type = f"league_season_retry_part_{idx}"

            notes = (
                f"retry_mode={retry_mode} missing_timeline={item.missing_timeline} "
                f"missing_player_stats={item.missing_player_stats}"
            )
            if len(chunks) > 1:
                notes += f" | chunk={idx}"

            rows.append(
                {
                    "day": day,
                    "task_seq": seq,
                    "item_type": item_type,
                    "league_code": item.league_code,
                    "league_name": item.league_name,
                    "api_league_id": item.api_league_id,
                    "start_year": item.start_year,
                    "estimated_calls": chunk_calls,
                    "notes": notes,
                }
            )
            day_calls += chunk_calls
            seq += 1

    return rows


def write_csv(path: str, rows: List[Dict[str, object]]) -> Path:
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "day",
                "task_seq",
                "item_type",
                "league_code",
                "league_name",
                "api_league_id",
                "start_year",
                "estimated_calls",
                "notes",
            ],
        )
        w.writeheader()
        for row in rows:
            w.writerow(row)
    return out_path


def main() -> None:
    args = parse_args()
    if args.daily_limit <= 0:
        raise SystemExit("--daily-limit must be > 0")
    if args.max_batch_calls <= 0:
        raise SystemExit("--max-batch-calls must be > 0")
    if args.max_batch_calls > args.daily_limit:
        raise SystemExit("--max-batch-calls must be <= --daily-limit")

    conn = connect_db(args)
    try:
        max_year = resolve_max_year(conn, int(args.min_start_year), int(args.max_start_year))
        items = fetch_retry_items(conn, int(args.min_start_year), max_year, args.retry_mode)
        rows = schedule_rows(items, int(args.daily_limit), int(args.max_batch_calls), str(args.retry_mode))
        out_path = write_csv(args.csv_out, rows)

        day_totals: Dict[int, int] = {}
        for row in rows:
            d = int(row["day"])
            day_totals[d] = day_totals.get(d, 0) + int(row["estimated_calls"])

        print("Major-5 retry schedule created (no API calls were made).")
        print(f"CSV output: {out_path.resolve()}")
        print(f"Slices scheduled: {len(items)}")
        print(f"Rows scheduled: {len(rows)}")
        print(f"Year span: {args.min_start_year}-{max_year}")
        print(f"Retry mode: {args.retry_mode}")
        print("Calls per day:")
        for d in sorted(day_totals):
            print(f"  Day {d}: {day_totals[d]:,}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
