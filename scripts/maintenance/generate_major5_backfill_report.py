#!/usr/bin/env python3
"""Generate final major-5 backfill validation artifacts.

Outputs:
1. league-season completeness matrix (CSV)
2. replay-ready fixture gap list (CSV)
3. summary JSON linking both files and totals

This script is read-only and does not call external APIs.
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
    {"league_code": "E0", "league_name": "Premier League", "api_league_id": 39, "priority": 1},
    {"league_code": "SP1", "league_name": "La Liga", "api_league_id": 140, "priority": 2},
    {"league_code": "I1", "league_name": "Serie A", "api_league_id": 135, "priority": 3},
    {"league_code": "D1", "league_name": "Bundesliga", "api_league_id": 78, "priority": 4},
    {"league_code": "F1", "league_name": "Ligue 1", "api_league_id": 61, "priority": 5},
]

FINISHED_STATUS = ("FT", "AET", "PEN", "FT_PEN", "AWD", "WO")


def guard_unsafe_secret_flags() -> None:
    for arg in sys.argv[1:]:
        if arg == "--password" or arg.startswith("--password="):
            raise SystemExit("Unsafe secret flag detected. Use environment variable MYSQL_PASSWORD.")


def parse_args() -> argparse.Namespace:
    guard_unsafe_secret_flags()
    p = argparse.ArgumentParser(description="Generate major-5 backfill validation report")
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
        help="Upper year bound. Use 0 to auto-resolve from event_fixture for major-5.",
    )
    p.add_argument(
        "--max-gap-rows",
        type=int,
        default=0,
        help="Cap gap CSV rows (0 = all).",
    )
    p.add_argument(
        "--out-prefix",
        default="plans/reports/major5_backfill_report",
        help="Output prefix; _<timestamp>_summary.json and CSVs are appended.",
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


def pct(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator * 100.0) / denominator, 2)


def fetch_matrix(conn, min_year: int, max_year: int) -> List[Dict[str, object]]:
    cur = conn.cursor()
    placeholders = ",".join(["%s"] * len(FINISHED_STATUS))

    sql = f"""
        SELECT
            ef.league_id,
            ef.season_year,
            COUNT(*) AS fixtures_total,
            SUM(CASE WHEN ef.status_short IN ({placeholders}) THEN 1 ELSE 0 END) AS fixtures_finished,
            COUNT(DISTINCT CASE WHEN m.provider_fixture_id IS NOT NULL THEN ef.provider_fixture_id END) AS fixtures_mapped,
            COUNT(DISTINCT CASE WHEN et.provider_fixture_id IS NOT NULL THEN ef.provider_fixture_id END) AS fixtures_with_timeline,
            COUNT(DISTINCT CASE WHEN eg.provider_fixture_id IS NOT NULL THEN ef.provider_fixture_id END) AS fixtures_with_goals,
            COUNT(DISTINCT CASE WHEN pms.provider_fixture_id IS NOT NULL THEN ef.provider_fixture_id END) AS fixtures_with_player_stats,
            COUNT(et.event_hash) AS timeline_rows,
            COUNT(eg.goal_id) AS goal_rows,
            COUNT(pms.provider_player_id) AS player_stats_rows
        FROM event_fixture ef
        LEFT JOIN event_fixture_match_map m ON m.provider_fixture_id = ef.provider_fixture_id
        LEFT JOIN event_timeline et ON et.provider_fixture_id = ef.provider_fixture_id
        LEFT JOIN event_goal eg ON eg.provider_fixture_id = ef.provider_fixture_id
        LEFT JOIN player_match_stats pms ON pms.provider_fixture_id = ef.provider_fixture_id
        WHERE ef.league_id IN (39, 140, 135, 78, 61)
          AND ef.season_year BETWEEN %s AND %s
        GROUP BY ef.league_id, ef.season_year
        ORDER BY ef.season_year DESC, ef.league_id ASC
    """

    params = tuple(list(FINISHED_STATUS) + [min_year, max_year])
    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close()

    by_api = {int(x["api_league_id"]): x for x in MAJOR5}
    out: List[Dict[str, object]] = []
    for row in rows:
        (
            league_id,
            season_year,
            fixtures_total,
            fixtures_finished,
            fixtures_mapped,
            fixtures_with_timeline,
            fixtures_with_goals,
            fixtures_with_player_stats,
            timeline_rows,
            goal_rows,
            player_stats_rows,
        ) = row

        meta = by_api.get(int(league_id))
        if not meta:
            continue

        finished = int(fixtures_finished or 0)
        mapped = int(fixtures_mapped or 0)
        with_timeline = int(fixtures_with_timeline or 0)
        with_goals = int(fixtures_with_goals or 0)
        with_players = int(fixtures_with_player_stats or 0)

        out.append(
            {
                "league_id": int(league_id),
                "league_code": str(meta["league_code"]),
                "league_name": str(meta["league_name"]),
                "season_year": int(season_year),
                "fixtures_total": int(fixtures_total or 0),
                "fixtures_finished": finished,
                "fixtures_mapped": mapped,
                "fixtures_with_timeline": with_timeline,
                "fixtures_with_goals": with_goals,
                "fixtures_with_player_stats": with_players,
                "timeline_rows": int(timeline_rows or 0),
                "goal_rows": int(goal_rows or 0),
                "player_stats_rows": int(player_stats_rows or 0),
                "mapping_pct_of_finished": pct(mapped, finished),
                "timeline_pct_of_finished": pct(with_timeline, finished),
                "goals_pct_of_finished": pct(with_goals, finished),
                "player_stats_pct_of_finished": pct(with_players, finished),
                "gap_missing_mapped": max(0, finished - mapped),
                "gap_missing_timeline": max(0, finished - with_timeline),
                "gap_missing_goals": max(0, finished - with_goals),
                "gap_missing_player_stats": max(0, finished - with_players),
            }
        )

    out.sort(key=lambda r: (-int(r["season_year"]), next(x["priority"] for x in MAJOR5 if x["league_code"] == r["league_code"])))
    return out


def fetch_gaps(conn, min_year: int, max_year: int, max_gap_rows: int) -> List[Dict[str, object]]:
    cur = conn.cursor()
    placeholders = ",".join(["%s"] * len(FINISHED_STATUS))

    limit_clause = ""
    params: List[object] = list(FINISHED_STATUS) + [min_year, max_year]
    if max_gap_rows > 0:
        limit_clause = "LIMIT %s"
        params.append(max_gap_rows)

    sql = f"""
        SELECT
            ef.provider_fixture_id,
            ef.league_id,
            ef.season_year,
            ef.fixture_date_utc,
            ef.status_short,
            ef.home_team_name,
            ef.away_team_name,
            CASE WHEN m.provider_fixture_id IS NULL THEN 1 ELSE 0 END AS missing_mapping,
            CASE WHEN et.provider_fixture_id IS NULL THEN 1 ELSE 0 END AS missing_timeline,
            CASE WHEN eg.provider_fixture_id IS NULL THEN 1 ELSE 0 END AS missing_goal,
            CASE WHEN pms.provider_fixture_id IS NULL THEN 1 ELSE 0 END AS missing_player_stats
        FROM event_fixture ef
        LEFT JOIN event_fixture_match_map m ON m.provider_fixture_id = ef.provider_fixture_id
        LEFT JOIN (
            SELECT DISTINCT provider_fixture_id FROM event_timeline
        ) et ON et.provider_fixture_id = ef.provider_fixture_id
        LEFT JOIN (
            SELECT DISTINCT provider_fixture_id FROM event_goal
        ) eg ON eg.provider_fixture_id = ef.provider_fixture_id
        LEFT JOIN (
            SELECT DISTINCT provider_fixture_id FROM player_match_stats
        ) pms ON pms.provider_fixture_id = ef.provider_fixture_id
        WHERE ef.league_id IN (39, 140, 135, 78, 61)
          AND ef.season_year BETWEEN %s AND %s
          AND ef.status_short IN ({placeholders})
          AND (
                m.provider_fixture_id IS NULL
                OR et.provider_fixture_id IS NULL
                OR pms.provider_fixture_id IS NULL
              )
        ORDER BY ef.season_year DESC, ef.league_id ASC, ef.fixture_date_utc ASC, ef.provider_fixture_id ASC
        {limit_clause}
    """

    cur.execute(sql, tuple([min_year, max_year] + list(FINISHED_STATUS) + ([] if max_gap_rows <= 0 else [max_gap_rows])))
    rows = cur.fetchall()
    cur.close()

    code_by_api = {int(x["api_league_id"]): str(x["league_code"]) for x in MAJOR5}
    out: List[Dict[str, object]] = []

    for row in rows:
        (
            provider_fixture_id,
            league_id,
            season_year,
            fixture_date_utc,
            status_short,
            home_team_name,
            away_team_name,
            missing_mapping,
            missing_timeline,
            missing_goal,
            missing_player_stats,
        ) = row

        out.append(
            {
                "provider_fixture_id": int(provider_fixture_id),
                "league_id": int(league_id),
                "league_code": code_by_api.get(int(league_id), "UNKNOWN"),
                "season_year": int(season_year),
                "fixture_date_utc": str(fixture_date_utc) if fixture_date_utc is not None else "",
                "status_short": str(status_short or ""),
                "home_team_name": str(home_team_name or ""),
                "away_team_name": str(away_team_name or ""),
                "missing_mapping": int(missing_mapping or 0),
                "missing_timeline": int(missing_timeline or 0),
                "missing_goal": int(missing_goal or 0),
                "missing_player_stats": int(missing_player_stats or 0),
                "recommended_retry_type": "league_season_retry",
                "recommended_command": (
                    "python sync_api_football_events.py "
                    f"--league-id {int(league_id)} --season-year {int(season_year)} "
                    "--skip-fixture-refresh --max-full-event-backfill-calls 200"
                ),
            }
        )

    return out


def write_csv(path: Path, rows: List[Dict[str, object]], fieldnames: List[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in fieldnames})


def main() -> None:
    args = parse_args()

    conn = connect_db(args)
    try:
        max_year = resolve_max_year(conn, int(args.min_start_year), int(args.max_start_year))
        matrix = fetch_matrix(conn, int(args.min_start_year), max_year)
        gaps = fetch_gaps(conn, int(args.min_start_year), max_year, int(args.max_gap_rows))

        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        base_prefix = Path(f"{args.out_prefix}_{ts}")
        base_prefix.parent.mkdir(parents=True, exist_ok=True)

        matrix_csv = Path(f"{base_prefix}_matrix.csv")
        gaps_csv = Path(f"{base_prefix}_gaps.csv")
        summary_json = Path(f"{base_prefix}_summary.json")

        matrix_fields = [
            "league_id",
            "league_code",
            "league_name",
            "season_year",
            "fixtures_total",
            "fixtures_finished",
            "fixtures_mapped",
            "fixtures_with_timeline",
            "fixtures_with_goals",
            "fixtures_with_player_stats",
            "timeline_rows",
            "goal_rows",
            "player_stats_rows",
            "mapping_pct_of_finished",
            "timeline_pct_of_finished",
            "goals_pct_of_finished",
            "player_stats_pct_of_finished",
            "gap_missing_mapped",
            "gap_missing_timeline",
            "gap_missing_goals",
            "gap_missing_player_stats",
        ]
        write_csv(matrix_csv, matrix, matrix_fields)

        gap_fields = [
            "provider_fixture_id",
            "league_id",
            "league_code",
            "season_year",
            "fixture_date_utc",
            "status_short",
            "home_team_name",
            "away_team_name",
            "missing_mapping",
            "missing_timeline",
            "missing_goal",
            "missing_player_stats",
            "recommended_retry_type",
            "recommended_command",
        ]
        write_csv(gaps_csv, gaps, gap_fields)

        totals = {
            "matrix_rows": len(matrix),
            "gap_rows": len(gaps),
            "fixtures_finished_total": int(sum(int(r["fixtures_finished"]) for r in matrix)),
            "fixtures_with_timeline_total": int(sum(int(r["fixtures_with_timeline"]) for r in matrix)),
            "fixtures_with_player_stats_total": int(sum(int(r["fixtures_with_player_stats"]) for r in matrix)),
            "gap_missing_timeline_total": int(sum(int(r["gap_missing_timeline"]) for r in matrix)),
            "gap_missing_player_stats_total": int(sum(int(r["gap_missing_player_stats"]) for r in matrix)),
            "gap_missing_mapping_total": int(sum(int(r["gap_missing_mapped"]) for r in matrix)),
        }

        summary = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "year_range": {"min_start_year": int(args.min_start_year), "max_start_year": int(max_year)},
            "scope": {
                "league_codes": [x["league_code"] for x in MAJOR5],
                "api_league_ids": [x["api_league_id"] for x in MAJOR5],
            },
            "totals": totals,
            "artifacts": {
                "matrix_csv": str(matrix_csv),
                "gaps_csv": str(gaps_csv),
                "summary_json": str(summary_json),
            },
        }

        with summary_json.open("w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=True)

        print("Major-5 backfill report generated.")
        print(f"Matrix CSV: {matrix_csv}")
        print(f"Gap CSV: {gaps_csv}")
        print(f"Summary JSON: {summary_json}")
        print(
            "Totals: "
            f"finished={totals['fixtures_finished_total']}, "
            f"with_timeline={totals['fixtures_with_timeline_total']}, "
            f"with_player_stats={totals['fixtures_with_player_stats_total']}, "
            f"gap_rows={totals['gap_rows']}"
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
