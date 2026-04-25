#!/usr/bin/env python3
"""Build a day-by-day pull schedule under strict small-batch limits (no API calls).

Supports splitting large league-season tasks into chunked batches.
"""

import argparse
import csv
import math
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import mysql.connector


TOP5 = [
    {"league_code": "E0", "league_name": "Premier League", "api_league_id": 39, "priority": 1},
    {"league_code": "SP1", "league_name": "La Liga", "api_league_id": 140, "priority": 2},
    {"league_code": "I1", "league_name": "Serie A", "api_league_id": 135, "priority": 3},
    {"league_code": "D1", "league_name": "Bundesliga", "api_league_id": 78, "priority": 4},
    {"league_code": "F1", "league_name": "Ligue 1", "api_league_id": 61, "priority": 5},
]


TOP5_CODES = [x["league_code"] for x in TOP5]


@dataclass
class PullItem:
    item_type: str
    league_code: str
    league_name: str
    api_league_id: int
    start_year: int
    calls: int
    notes: str


def guard_unsafe_secret_flags() -> None:
    for arg in sys.argv[1:]:
        if arg == "--password" or arg.startswith("--password="):
            raise SystemExit("Unsafe secret flag detected. Use environment variable MYSQL_PASSWORD.")


def parse_args() -> argparse.Namespace:
    guard_unsafe_secret_flags()
    p = argparse.ArgumentParser(description="Build daily pull schedule with small-batch support")
    p.add_argument("--host", default=os.getenv("MYSQL_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.getenv("MYSQL_PORT", "3306")))
    p.add_argument("--user", default=os.getenv("MYSQL_USER", "football_admin"))
    p.add_argument("--database", default=os.getenv("MYSQL_DATABASE", "historic_football_data"))
    p.add_argument("--mysql-password-env", default="MYSQL_PASSWORD")

    p.add_argument("--min-start-year", type=int, default=2016)
    p.add_argument("--max-start-year", type=int, default=2025)
    p.add_argument(
        "--scope",
        choices=["top5", "all"],
        default="top5",
        help="League scope: top5 only or all leagues present in match_game",
    )
    p.add_argument(
        "--auto-span",
        action="store_true",
        help="Override min/max years with widest available span in selected scope",
    )
    p.add_argument("--daily-limit", type=int, default=75000)
    p.add_argument(
        "--max-batch-calls",
        type=int,
        default=1000,
        help="Max calls for a single scheduled task row; lower values reduce burst load and per-second pressure",
    )
    p.add_argument("--include-rosters", action="store_true")
    p.add_argument("--include-player-stats", action="store_true")
    p.add_argument("--players-pages-per-team-season", type=float, default=2.0)
    p.add_argument("--csv-out", default="plans/top5_last10_daily_schedule.csv")
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


def load_league_meta(conn, scope: str) -> List[Dict[str, object]]:
    if scope == "top5":
        return [dict(x) for x in TOP5]

    sql = """
        SELECT DISTINCT l.league_code, l.league_name
        FROM match_game mg
        JOIN league l ON l.league_id = mg.league_id
        WHERE l.league_code IS NOT NULL
        ORDER BY l.league_code
    """
    cur = conn.cursor()
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        out: List[Dict[str, object]] = []
        for idx, (league_code, league_name) in enumerate(rows, start=1):
            out.append(
                {
                    "league_code": str(league_code),
                    "league_name": str(league_name),
                    "api_league_id": 0,
                    "priority": idx,
                }
            )
        return out
    finally:
        cur.close()


def resolve_year_span(conn, args: argparse.Namespace, league_meta: List[Dict[str, object]]) -> Tuple[int, int]:
    if not args.auto_span:
        return int(args.min_start_year), int(args.max_start_year)

    codes = [str(x["league_code"]) for x in league_meta]
    if not codes:
        raise SystemExit("No leagues found for selected scope")

    placeholders = ",".join(["%s"] * len(codes))
    sql = f"""
        SELECT MIN(s.start_year), MAX(s.start_year)
        FROM match_game mg
        JOIN league l ON l.league_id = mg.league_id
        JOIN season s ON s.season_id = mg.season_id
        WHERE l.league_code IN ({placeholders})
    """

    cur = conn.cursor()
    try:
        cur.execute(sql, codes)
        row = cur.fetchone()
        if not row or row[0] is None or row[1] is None:
            raise SystemExit("Unable to determine year span for selected scope")
        return int(row[0]), int(row[1])
    finally:
        cur.close()


def fetch_league_season_items(
    conn,
    args: argparse.Namespace,
    league_meta: List[Dict[str, object]],
    min_year: int,
    max_year: int,
) -> List[PullItem]:
    meta = {str(x["league_code"]): x for x in league_meta}
    placeholders = ",".join(["%s"] * len(meta))

    sql = f"""
        SELECT
            l.league_code,
            s.start_year,
            COUNT(*) AS matches,
            COUNT(DISTINCT t.team_id) AS teams_in_league_season
        FROM match_game mg
        JOIN league l ON l.league_id = mg.league_id
        JOIN season s ON s.season_id = mg.season_id
        JOIN (
            SELECT match_id, home_team_id AS team_id FROM match_game
            UNION ALL
            SELECT match_id, away_team_id AS team_id FROM match_game
        ) t ON t.match_id = mg.match_id
        WHERE l.league_code IN ({placeholders})
          AND s.start_year BETWEEN %s AND %s
        GROUP BY l.league_code, s.start_year
    """

    cur = conn.cursor()
    try:
        cur.execute(sql, [*meta.keys(), min_year, max_year])
        items: List[PullItem] = []
        for league_code, start_year, matches, teams_in_league_season in cur.fetchall():
            info = meta[league_code]
            fixtures_calls = 1
            event_calls = int(matches)
            player_calls = 0
            if args.include_player_stats:
                player_calls = int(math.ceil(float(teams_in_league_season) * args.players_pages_per_team_season))

            total_calls = fixtures_calls + event_calls + player_calls
            notes = f"fixtures={fixtures_calls} events={event_calls} players={player_calls}"
            items.append(
                PullItem(
                    item_type="league_season",
                    league_code=league_code,
                    league_name=info["league_name"],
                    api_league_id=info["api_league_id"],
                    start_year=int(start_year),
                    calls=total_calls,
                    notes=notes,
                )
            )

        items.sort(key=lambda r: (-r.start_year, meta[r.league_code]["priority"]))
        return items
    finally:
        cur.close()


def fetch_roster_calls(conn, args: argparse.Namespace) -> int:
    if not args.include_rosters:
        return 0
    if args.scope == "top5":
        codes = TOP5_CODES
    else:
        cur_codes = conn.cursor()
        try:
            cur_codes.execute(
                """
                SELECT DISTINCT l.league_code
                FROM match_game mg
                JOIN league l ON l.league_id = mg.league_id
                WHERE l.league_code IS NOT NULL
                """
            )
            codes = [str(r[0]) for r in cur_codes.fetchall() if r[0]]
        finally:
            cur_codes.close()

    placeholders = ",".join(["%s"] * len(codes))
    sql = f"""
        SELECT COUNT(DISTINCT t.team_id)
        FROM (
            SELECT mg.home_team_id AS team_id, mg.league_id, mg.season_id FROM match_game mg
            UNION
            SELECT mg.away_team_id AS team_id, mg.league_id, mg.season_id FROM match_game mg
        ) t
        JOIN league l ON l.league_id = t.league_id
        JOIN season s ON s.season_id = t.season_id
        WHERE l.league_code IN ({placeholders})
          AND s.start_year BETWEEN %s AND %s
    """
    cur = conn.cursor()
    try:
        min_year = int(args.min_start_year)
        max_year = int(args.max_start_year)
        if args.auto_span:
            span_cur = conn.cursor()
            try:
                span_cur.execute(
                    f"""
                    SELECT MIN(s.start_year), MAX(s.start_year)
                    FROM match_game mg
                    JOIN league l ON l.league_id = mg.league_id
                    JOIN season s ON s.season_id = mg.season_id
                    WHERE l.league_code IN ({placeholders})
                    """,
                    codes,
                )
                row = span_cur.fetchone()
                if row and row[0] is not None and row[1] is not None:
                    min_year = int(row[0])
                    max_year = int(row[1])
            finally:
                span_cur.close()

        cur.execute(sql, [*codes, min_year, max_year])
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    finally:
        cur.close()


def chunk_items(items: List[PullItem], max_batch_calls: int) -> List[PullItem]:
    chunked: List[PullItem] = []
    for item in items:
        remaining = item.calls
        chunk_idx = 1
        while remaining > 0:
            take = min(remaining, max_batch_calls)
            note = item.notes
            out_item_type = item.item_type
            if item.calls > max_batch_calls:
                note += f" | chunk={chunk_idx}"
                out_item_type = f"{item.item_type}_part_{chunk_idx}"
            chunked.append(
                PullItem(
                    item_type=out_item_type,
                    league_code=item.league_code,
                    league_name=item.league_name,
                    api_league_id=item.api_league_id,
                    start_year=item.start_year,
                    calls=take,
                    notes=note,
                )
            )
            remaining -= take
            chunk_idx += 1
    return chunked


def schedule_items(items: List[PullItem], daily_limit: int) -> List[Dict[str, object]]:
    scheduled: List[Dict[str, object]] = []
    day = 1
    day_calls = 0
    task_seq = 1

    for item in items:
        if day_calls + item.calls > daily_limit:
            day += 1
            day_calls = 0

        day_calls += item.calls
        scheduled.append(
            {
                "day": day,
                "task_seq": task_seq,
                "item_type": item.item_type,
                "league_code": item.league_code,
                "league_name": item.league_name,
                "api_league_id": item.api_league_id,
                "start_year": item.start_year,
                "estimated_calls": item.calls,
                "notes": item.notes,
            }
        )
        task_seq += 1

    return scheduled


def main() -> None:
    args = parse_args()
    if args.daily_limit <= 0:
        raise SystemExit("--daily-limit must be > 0")
    if args.max_batch_calls <= 0:
        raise SystemExit("--max-batch-calls must be > 0")
    if args.max_batch_calls > args.daily_limit:
        # Allow bigger batches than daily-limit? No, this would break packing.
        raise SystemExit("--max-batch-calls must be <= --daily-limit")

    conn = connect_db(args)
    try:
        league_meta = load_league_meta(conn, args.scope)
        if not league_meta:
            raise SystemExit("No leagues found in selected scope")

        min_year, max_year = resolve_year_span(conn, args, league_meta)
        args.min_start_year = min_year
        args.max_start_year = max_year

        items = fetch_league_season_items(conn, args, league_meta, min_year, max_year)
        if args.include_rosters:
            roster_calls = fetch_roster_calls(conn, args)
            if roster_calls > 0:
                items.insert(
                    0,
                    PullItem(
                        item_type="rosters",
                        league_code=("TOP5" if args.scope == "top5" else "ALL"),
                        league_name=("Top5 Rosters" if args.scope == "top5" else "All-Leagues Rosters"),
                        api_league_id=0,
                        start_year=max_year,
                        calls=roster_calls,
                        notes="players/squads per distinct team in scope",
                    ),
                )

        chunked = chunk_items(items, args.max_batch_calls)
        scheduled = schedule_items(chunked, args.daily_limit)

        out_path = Path(args.csv_out)
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
            for row in scheduled:
                w.writerow(row)

        day_totals: Dict[int, int] = {}
        for row in scheduled:
            d = int(row["day"])
            day_totals[d] = day_totals.get(d, 0) + int(row["estimated_calls"])

        print("Daily pull schedule created (no API calls were made).")
        print(f"CSV output: {out_path.resolve()}")
        print(f"Rows scheduled: {len(scheduled)}")
        print(f"Days planned: {len(day_totals)}")
        print(f"Scope: {args.scope}")
        print(f"Year span: {min_year}-{max_year}")
        print("Calls per day:")
        for d in sorted(day_totals):
            print(f"  Day {d}: {day_totals[d]:,}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
