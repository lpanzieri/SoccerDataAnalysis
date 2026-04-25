#!/usr/bin/env python3
"""Capture a major-5 (E0, SP1, I1, D1, F1) backfill baseline snapshot.

This script is read-only. It does not call external APIs and does not mutate DB data.
It writes a JSON + CSV report with per league-season coverage metrics to support
2016..current historical/API gap-fill planning.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import mysql.connector


MAJOR5 = [
    {"league_code": "E0", "league_name": "Premier League", "api_league_id": 39},
    {"league_code": "SP1", "league_name": "La Liga", "api_league_id": 140},
    {"league_code": "I1", "league_name": "Serie A", "api_league_id": 135},
    {"league_code": "D1", "league_name": "Bundesliga", "api_league_id": 78},
    {"league_code": "F1", "league_name": "Ligue 1", "api_league_id": 61},
]

FINISHED_STATUS = ("FT", "AET", "PEN", "FT_PEN", "AWD", "WO")


def guard_unsafe_secret_flags() -> None:
    for arg in sys.argv[1:]:
        if arg == "--password" or arg.startswith("--password="):
            raise SystemExit("Unsafe secret flag detected. Use environment variable MYSQL_PASSWORD.")


def parse_args() -> argparse.Namespace:
    guard_unsafe_secret_flags()
    p = argparse.ArgumentParser(description="Snapshot major-5 baseline coverage for backfill planning")
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
        help="Upper year bound. Use 0 to auto-detect from DB for major-5 scope.",
    )
    p.add_argument(
        "--out-prefix",
        default="plans/major5_backfill_baseline",
        help="Output path prefix; .json and .csv are appended.",
    )
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


def map_code_to_name() -> Dict[str, str]:
    return {x["league_code"]: x["league_name"] for x in MAJOR5}


def map_api_to_code() -> Dict[int, str]:
    return {int(x["api_league_id"]): x["league_code"] for x in MAJOR5}


def resolve_max_start_year(conn, min_year: int) -> int:
    cur = conn.cursor()
    codes = [x["league_code"] for x in MAJOR5]
    placeholders = ",".join(["%s"] * len(codes))
    try:
        cur.execute(
            f"""
            SELECT MAX(s.start_year)
            FROM match_game mg
            JOIN league l ON l.league_id = mg.league_id
            JOIN season s ON s.season_id = mg.season_id
            WHERE l.league_code IN ({placeholders})
            """,
            tuple(codes),
        )
        row = cur.fetchone()
        max_hist = int(row[0]) if row and row[0] is not None else min_year

        cur.execute(
            "SELECT MAX(season_year) FROM event_fixture WHERE league_id IN (39, 140, 135, 78, 61)"
        )
        row = cur.fetchone()
        max_event = int(row[0]) if row and row[0] is not None else min_year

        return max(min_year, max(max_hist, max_event))
    finally:
        cur.close()


def query_historical_matches(conn, min_year: int, max_year: int) -> Dict[Tuple[str, int], int]:
    out: Dict[Tuple[str, int], int] = {}
    cur = conn.cursor()
    codes = [x["league_code"] for x in MAJOR5]
    placeholders = ",".join(["%s"] * len(codes))
    try:
        cur.execute(
            f"""
            SELECT l.league_code, s.start_year, COUNT(*) AS matches
            FROM match_game mg
            JOIN league l ON l.league_id = mg.league_id
            JOIN season s ON s.season_id = mg.season_id
            WHERE l.league_code IN ({placeholders})
              AND s.start_year BETWEEN %s AND %s
            GROUP BY l.league_code, s.start_year
            """,
            tuple(codes + [min_year, max_year]),
        )
        for league_code, start_year, matches in cur.fetchall():
            out[(str(league_code), int(start_year))] = int(matches)
        return out
    finally:
        cur.close()


def query_event_fixtures(conn, min_year: int, max_year: int) -> Dict[Tuple[str, int], Dict[str, int]]:
    out: Dict[Tuple[str, int], Dict[str, int]] = {}
    cur = conn.cursor()
    api_to_code = map_api_to_code()
    placeholders = ",".join(["%s"] * len(FINISHED_STATUS))
    try:
        cur.execute(
            f"""
            SELECT
                league_id,
                season_year,
                COUNT(*) AS fixtures_total,
                SUM(CASE WHEN status_short IN ({placeholders}) THEN 1 ELSE 0 END) AS fixtures_finished
            FROM event_fixture
            WHERE league_id IN (39, 140, 135, 78, 61)
              AND season_year BETWEEN %s AND %s
            GROUP BY league_id, season_year
            """,
            tuple(list(FINISHED_STATUS) + [min_year, max_year]),
        )
        for league_id, season_year, fixtures_total, fixtures_finished in cur.fetchall():
            code = api_to_code.get(int(league_id))
            if not code:
                continue
            out[(code, int(season_year))] = {
                "event_fixtures_total": int(fixtures_total or 0),
                "event_fixtures_finished": int(fixtures_finished or 0),
            }
        return out
    finally:
        cur.close()


def query_timeline_coverage(conn, min_year: int, max_year: int) -> Dict[Tuple[str, int], Dict[str, int]]:
    out: Dict[Tuple[str, int], Dict[str, int]] = {}
    cur = conn.cursor()
    api_to_code = map_api_to_code()
    try:
        cur.execute(
            """
            SELECT
                ef.league_id,
                ef.season_year,
                COUNT(*) AS timeline_rows,
                COUNT(DISTINCT et.provider_fixture_id) AS fixtures_with_timeline
            FROM event_timeline et
            JOIN event_fixture ef ON ef.provider_fixture_id = et.provider_fixture_id
            WHERE ef.league_id IN (39, 140, 135, 78, 61)
              AND ef.season_year BETWEEN %s AND %s
            GROUP BY ef.league_id, ef.season_year
            """,
            (min_year, max_year),
        )
        for league_id, season_year, rows, fixtures in cur.fetchall():
            code = api_to_code.get(int(league_id))
            if not code:
                continue
            out[(code, int(season_year))] = {
                "timeline_rows": int(rows or 0),
                "fixtures_with_timeline": int(fixtures or 0),
            }
        return out
    finally:
        cur.close()


def query_goal_coverage(conn, min_year: int, max_year: int) -> Dict[Tuple[str, int], Dict[str, int]]:
    out: Dict[Tuple[str, int], Dict[str, int]] = {}
    cur = conn.cursor()
    api_to_code = map_api_to_code()
    try:
        cur.execute(
            """
            SELECT
                ef.league_id,
                ef.season_year,
                COUNT(*) AS goal_rows,
                COUNT(DISTINCT eg.provider_fixture_id) AS fixtures_with_goals
            FROM event_goal eg
            JOIN event_fixture ef ON ef.provider_fixture_id = eg.provider_fixture_id
            WHERE ef.league_id IN (39, 140, 135, 78, 61)
              AND ef.season_year BETWEEN %s AND %s
            GROUP BY ef.league_id, ef.season_year
            """,
            (min_year, max_year),
        )
        for league_id, season_year, rows, fixtures in cur.fetchall():
            code = api_to_code.get(int(league_id))
            if not code:
                continue
            out[(code, int(season_year))] = {
                "goal_rows": int(rows or 0),
                "fixtures_with_goals": int(fixtures or 0),
            }
        return out
    finally:
        cur.close()


def query_player_stats_coverage(conn, min_year: int, max_year: int) -> Dict[Tuple[str, int], Dict[str, int]]:
    out: Dict[Tuple[str, int], Dict[str, int]] = {}
    cur = conn.cursor()
    api_to_code = map_api_to_code()
    try:
        cur.execute(
            """
            SELECT
                ef.league_id,
                ef.season_year,
                COUNT(*) AS player_stats_rows,
                COUNT(DISTINCT pms.provider_fixture_id) AS fixtures_with_player_stats,
                COUNT(DISTINCT pms.provider_player_id) AS distinct_players_in_stats
            FROM player_match_stats pms
            JOIN event_fixture ef ON ef.provider_fixture_id = pms.provider_fixture_id
            WHERE ef.league_id IN (39, 140, 135, 78, 61)
              AND ef.season_year BETWEEN %s AND %s
            GROUP BY ef.league_id, ef.season_year
            """,
            (min_year, max_year),
        )
        for league_id, season_year, rows, fixtures, players in cur.fetchall():
            code = api_to_code.get(int(league_id))
            if not code:
                continue
            out[(code, int(season_year))] = {
                "player_stats_rows": int(rows or 0),
                "fixtures_with_player_stats": int(fixtures or 0),
                "distinct_players_in_stats": int(players or 0),
            }
        return out
    finally:
        cur.close()


def query_mapping_coverage(conn, min_year: int, max_year: int) -> Dict[Tuple[str, int], int]:
    out: Dict[Tuple[str, int], int] = {}
    cur = conn.cursor()
    api_to_code = map_api_to_code()
    try:
        cur.execute(
            """
            SELECT
                ef.league_id,
                ef.season_year,
                COUNT(*) AS mapped_fixtures
            FROM event_fixture_match_map m
            JOIN event_fixture ef ON ef.provider_fixture_id = m.provider_fixture_id
            WHERE ef.league_id IN (39, 140, 135, 78, 61)
              AND ef.season_year BETWEEN %s AND %s
            GROUP BY ef.league_id, ef.season_year
            """,
            (min_year, max_year),
        )
        for league_id, season_year, count_rows in cur.fetchall():
            code = api_to_code.get(int(league_id))
            if not code:
                continue
            out[(code, int(season_year))] = int(count_rows or 0)
        return out
    finally:
        cur.close()


def query_player_dimensions(conn) -> Dict[str, int]:
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM player_dim")
        player_dim_rows = int(cur.fetchone()[0] or 0)

        cur.execute("SELECT COUNT(*) FROM player_team_history")
        player_team_history_rows = int(cur.fetchone()[0] or 0)

        cur.execute("SELECT COUNT(*) FROM player_name_alias")
        player_alias_rows = int(cur.fetchone()[0] or 0)

        return {
            "player_dim_rows": player_dim_rows,
            "player_team_history_rows": player_team_history_rows,
            "player_name_alias_rows": player_alias_rows,
        }
    finally:
        cur.close()


def build_rows(min_year: int, max_year: int) -> List[Tuple[str, int]]:
    rows: List[Tuple[str, int]] = []
    for entry in MAJOR5:
        for year in range(min_year, max_year + 1):
            rows.append((entry["league_code"], year))
    return rows


def write_outputs(
    out_prefix: str,
    payload: Dict[str, object],
    matrix_rows: List[Dict[str, object]],
) -> Tuple[Path, Path]:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    prefix = Path(f"{out_prefix}_{stamp}")
    prefix.parent.mkdir(parents=True, exist_ok=True)

    json_path = Path(f"{prefix}.json")
    csv_path = Path(f"{prefix}.csv")

    with json_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=True)

    fieldnames = [
        "league_code",
        "league_name",
        "start_year",
        "historical_matches",
        "event_fixtures_total",
        "event_fixtures_finished",
        "mapped_fixtures",
        "timeline_rows",
        "fixtures_with_timeline",
        "goal_rows",
        "fixtures_with_goals",
        "player_stats_rows",
        "fixtures_with_player_stats",
        "distinct_players_in_stats",
        "gap_historical_minus_event",
        "gap_finished_minus_timeline",
        "gap_finished_minus_player_stats",
    ]

    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in matrix_rows:
            writer.writerow({k: row.get(k, 0) for k in fieldnames})

    return json_path, csv_path


def main() -> None:
    args = parse_args()
    if args.min_start_year < 1900:
        raise SystemExit("--min-start-year must be realistic (>= 1900)")

    conn = connect_db(args)
    try:
        max_year = int(args.max_start_year)
        if max_year == 0:
            max_year = resolve_max_start_year(conn, args.min_start_year)
        if max_year < args.min_start_year:
            raise SystemExit("Resolved max year is below min year")

        hist = query_historical_matches(conn, args.min_start_year, max_year)
        fixtures = query_event_fixtures(conn, args.min_start_year, max_year)
        timeline = query_timeline_coverage(conn, args.min_start_year, max_year)
        goals = query_goal_coverage(conn, args.min_start_year, max_year)
        player_stats = query_player_stats_coverage(conn, args.min_start_year, max_year)
        mapped = query_mapping_coverage(conn, args.min_start_year, max_year)
        player_dims = query_player_dimensions(conn)

        league_name = map_code_to_name()
        matrix_rows: List[Dict[str, object]] = []

        totals = {
            "historical_matches": 0,
            "event_fixtures_total": 0,
            "event_fixtures_finished": 0,
            "mapped_fixtures": 0,
            "timeline_rows": 0,
            "fixtures_with_timeline": 0,
            "goal_rows": 0,
            "fixtures_with_goals": 0,
            "player_stats_rows": 0,
            "fixtures_with_player_stats": 0,
            "distinct_players_in_stats": 0,
        }

        for key in build_rows(args.min_start_year, max_year):
            code, year = key
            row = {
                "league_code": code,
                "league_name": league_name.get(code, code),
                "start_year": year,
                "historical_matches": hist.get(key, 0),
                "event_fixtures_total": fixtures.get(key, {}).get("event_fixtures_total", 0),
                "event_fixtures_finished": fixtures.get(key, {}).get("event_fixtures_finished", 0),
                "mapped_fixtures": mapped.get(key, 0),
                "timeline_rows": timeline.get(key, {}).get("timeline_rows", 0),
                "fixtures_with_timeline": timeline.get(key, {}).get("fixtures_with_timeline", 0),
                "goal_rows": goals.get(key, {}).get("goal_rows", 0),
                "fixtures_with_goals": goals.get(key, {}).get("fixtures_with_goals", 0),
                "player_stats_rows": player_stats.get(key, {}).get("player_stats_rows", 0),
                "fixtures_with_player_stats": player_stats.get(key, {}).get("fixtures_with_player_stats", 0),
                "distinct_players_in_stats": player_stats.get(key, {}).get("distinct_players_in_stats", 0),
            }

            row["gap_historical_minus_event"] = max(0, row["historical_matches"] - row["event_fixtures_total"])
            row["gap_finished_minus_timeline"] = max(0, row["event_fixtures_finished"] - row["fixtures_with_timeline"])
            row["gap_finished_minus_player_stats"] = max(
                0,
                row["event_fixtures_finished"] - row["fixtures_with_player_stats"],
            )

            for metric in totals:
                totals[metric] += int(row[metric])

            matrix_rows.append(row)

        payload = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "scope": {
                "league_codes": [x["league_code"] for x in MAJOR5],
                "api_league_ids": [x["api_league_id"] for x in MAJOR5],
            },
            "year_range": {"min_start_year": args.min_start_year, "max_start_year": max_year},
            "totals": totals,
            "player_dimension_totals": player_dims,
            "matrix": matrix_rows,
        }

        json_path, csv_path = write_outputs(args.out_prefix, payload, matrix_rows)

        print("Major-5 baseline snapshot complete (read-only).")
        print(f"Year range: {args.min_start_year}-{max_year}")
        print(f"JSON: {json_path}")
        print(f"CSV: {csv_path}")
        print(
            "Totals: "
            f"historical_matches={totals['historical_matches']}, "
            f"event_fixtures_total={totals['event_fixtures_total']}, "
            f"fixtures_with_timeline={totals['fixtures_with_timeline']}, "
            f"fixtures_with_player_stats={totals['fixtures_with_player_stats']}"
        )
        print(
            "Player dimensions: "
            f"player_dim={player_dims['player_dim_rows']}, "
            f"player_team_history={player_dims['player_team_history_rows']}, "
            f"player_name_alias={player_dims['player_name_alias_rows']}"
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
