#!/usr/bin/env python3
"""Sync API-Football fixture lineups and player injuries into database.

Fetches:
- Lineups from /fixtures/lineups for upcoming/recent fixtures
- Injuries from /injuries for league+season
"""

import argparse
import datetime as dt
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from typing import Dict, List, Optional, Tuple

import mysql.connector

API_BASE = "https://v3.football.api-sports.io"


def api_get(path: str, params: Dict[str, str], api_key: str) -> Tuple[int, Dict, Dict[str, str]]:
    """Make a GET request to the API-Football API."""
    qs = urllib.parse.urlencode(params)
    url = f"{API_BASE}{path}?{qs}"
    req = urllib.request.Request(
        url,
        headers={
            "x-apisports-key": api_key,
            "Accept": "application/json",
            "User-Agent": "data-analysis-sync/1.0",
        },
        method="GET",
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8")
            return resp.getcode(), json.loads(body), dict(resp.headers)
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        payload = {"error": body}
        return e.code, payload, dict(e.headers)


def _header_int(headers: Dict[str, str], names: List[str]) -> Optional[int]:
    """Extract integer value from response headers (for rate limit tracking)."""
    for name in names:
        wanted = name.lower()
        for k, v in headers.items():
            if str(k).lower() == wanted:
                token = str(v).strip()
                if token:
                    try:
                        return int(float(token))
                    except ValueError:
                        continue
    return None


def connect_db(host: str, port: int, user: str, password: str, database: str):
    """Connect to MySQL database."""
    return mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset="utf8mb4",
        use_unicode=True,
        use_pure=True,
        autocommit=False,
    )


def sync_lineups(
    conn,
    api_key: str,
    fixture_ids: List[int],
    calls_left: int,
    sleep_seconds: float = 1.5,
) -> int:
    """Fetch and store lineups for fixtures."""
    cur = conn.cursor(dictionary=True)
    try:
        for fid in fixture_ids:
            if calls_left <= 0:
                break

            params = {"fixture": str(fid)}
            code, payload, headers = api_get("/fixtures/lineups", params, api_key)
            calls_left -= 1

            if code != 200:
                print(f"WARN: lineups failed for fixture={fid}, code={code}")
                continue

            response_lineups = payload.get("response", [])
            for lineup in response_lineups:
                team = lineup.get("team", {})
                team_id = team.get("id")
                team_name = team.get("name")
                formation = lineup.get("formation")
                coach = lineup.get("coach", {})
                coach_id = coach.get("id")
                coach_name = coach.get("name")

                cur.execute(
                    """
                    INSERT INTO fixture_lineup
                    (provider_fixture_id, team_id, team_name, formation, coach_id, coach_name, raw_json)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        formation = VALUES(formation),
                        coach_id = VALUES(coach_id),
                        coach_name = VALUES(coach_name),
                        raw_json = VALUES(raw_json),
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (fid, team_id, team_name, formation, coach_id, coach_name, json.dumps(lineup, ensure_ascii=False)),
                )
                cur.execute(
                    """
                    SELECT lineup_id
                    FROM fixture_lineup
                    WHERE provider_fixture_id = %s AND team_id = %s
                    LIMIT 1
                    """,
                    (fid, team_id),
                )
                lineup_row = cur.fetchone() or {}
                lineup_id = lineup_row.get("lineup_id")
                if not lineup_id:
                    continue

                players = lineup.get("startXI", []) + lineup.get("substitutes", [])
                for i, player_entry in enumerate(players):
                    player = player_entry.get("player", {})
                    player_id = player.get("id")
                    player_name = player.get("name")
                    player_number = player.get("number")
                    player_pos = player.get("pos")
                    is_starter = i < len(lineup.get("startXI", []))

                    if player_id is not None:
                        cur.execute(
                            """
                            INSERT INTO player_dim (provider_player_id, player_name)
                            VALUES (%s, %s)
                            ON DUPLICATE KEY UPDATE
                                player_name = COALESCE(player_name, VALUES(player_name))
                            """,
                            (player_id, player_name),
                        )

                    cur.execute(
                        """
                        INSERT INTO fixture_lineup_player
                        (lineup_id, provider_fixture_id, player_id, player_name, player_number, player_pos, is_starter, raw_json)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            player_name = VALUES(player_name),
                            player_number = VALUES(player_number),
                            player_pos = VALUES(player_pos),
                            is_starter = VALUES(is_starter),
                            raw_json = VALUES(raw_json)
                        """,
                        (
                            lineup_id,
                            fid,
                            player_id,
                            player_name,
                            player_number,
                            player_pos,
                            is_starter,
                            json.dumps(player_entry, ensure_ascii=False),
                        ),
                    )

            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

    finally:
        cur.close()

    return calls_left


def sync_injuries(
    conn,
    api_key: str,
    league_id: int,
    season_year: int,
    calls_left: int,
    sleep_seconds: float = 1.5,
) -> int:
    """Fetch and store player injuries."""
    cur = conn.cursor(dictionary=True)
    try:
        if calls_left <= 0:
            return calls_left

        params = {"league": str(league_id), "season": str(season_year)}
        code, payload, headers = api_get("/injuries", params, api_key)
        calls_left -= 1

        if code != 200:
            print(f"WARN: injuries failed for league={league_id} season={season_year}, code={code}")
            return calls_left

        response_injuries = payload.get("response", [])
        for injury in response_injuries:
            player = injury.get("player", {})
            player_id = player.get("id")
            player_name = player.get("name")
            team = injury.get("team", {})
            team_id = team.get("id")
            team_name = team.get("name")
            fixture = injury.get("fixture", {})
            fixture_id = fixture.get("id")
            injury_type = injury.get("type")
            injury_reason = injury.get("reason")
            injury_date = injury.get("start")
            return_date = injury.get("end")
            games_missed = injury.get("games", {}).get("number")

            cur.execute(
                """
                INSERT INTO player_injury
                (provider_player_id, player_name, provider_team_id, team_name, fixture_id,
                 league_id, season_year, injury_type, injury_reason, injury_date, return_date,
                 games_missed, raw_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    injury_reason = VALUES(injury_reason),
                    return_date = VALUES(return_date),
                    games_missed = VALUES(games_missed),
                    raw_json = VALUES(raw_json),
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    player_id,
                    player_name,
                    team_id,
                    team_name,
                    fixture_id,
                    league_id,
                    season_year,
                    injury_type,
                    injury_reason,
                    injury_date,
                    return_date,
                    games_missed,
                    json.dumps(injury, ensure_ascii=False),
                ),
            )

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    finally:
        cur.close()

    return calls_left


def parse_args():
    p = argparse.ArgumentParser(description="Sync API-Football lineups and injuries")
    p.add_argument("--host", default=os.getenv("MYSQL_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.getenv("MYSQL_PORT", "3306")))
    p.add_argument("--user", default=os.getenv("MYSQL_USER", "football_admin"))
    p.add_argument("--database", default=os.getenv("MYSQL_DATABASE", "historic_football_data"))
    p.add_argument("--mysql-password-env", default="MYSQL_PASSWORD")
    p.add_argument("--api-key-env", default="APIFOOTBALL_KEY")
    p.add_argument("--league-id", type=int, default=135, help="League ID (135=Serie A)")
    p.add_argument("--season-year", type=int, required=True, help="Season year")
    p.add_argument("--sync-lineups", action="store_true", help="Sync lineups from API")
    p.add_argument("--sync-injuries", action="store_true", help="Sync injuries from API")
    p.add_argument("--fixture-days-back", type=int, default=7, help="Fetch lineups for fixtures N days back")
    p.add_argument("--sleep-seconds", type=float, default=1.5)
    p.add_argument("--daily-limit", type=int, default=100)
    p.add_argument("--reserve", type=int, default=15)
    return p.parse_args()


def main():
    args = parse_args()
    api_key = os.getenv(args.api_key_env, "")
    if not api_key:
        raise ValueError(f"API key not found in {args.api_key_env}")

    mysql_password = os.getenv(args.mysql_password_env, "")
    conn = connect_db(args.host, args.port, args.user, mysql_password, args.database)

    try:
        calls_budget = args.daily_limit - args.reserve
        calls_left = calls_budget

        if args.sync_lineups:
            cur = conn.cursor(dictionary=True)
            try:
                cutoff = dt.datetime.now(dt.UTC) - dt.timedelta(days=args.fixture_days_back)
                cur.execute(
                    """
                    SELECT provider_fixture_id FROM event_fixture
                    WHERE league_id = %s AND season_year = %s
                    AND status_short IN ('FT','AET','PEN','FT_PEN','AWD','WO','NS','SUSP','PST','CANC')
                    AND fixture_date_utc >= %s
                    LIMIT %s
                    """,
                    (args.league_id, args.season_year, cutoff.isoformat(), 50),
                )
                fixture_ids = [row["provider_fixture_id"] for row in cur.fetchall()]
            finally:
                cur.close()

            print(f"Fixtures for lineup sync: {len(fixture_ids)}")
            calls_left = sync_lineups(conn, api_key, fixture_ids, calls_left, args.sleep_seconds)
            conn.commit()

        if args.sync_injuries:
            print(f"Syncing injuries for league={args.league_id} season={args.season_year}")
            calls_left = sync_injuries(conn, api_key, args.league_id, args.season_year, calls_left, args.sleep_seconds)
            conn.commit()

        print(f"Calls left in budget: {calls_left}")
        print("Sync completed successfully.")

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
