#!/usr/bin/env python3
"""Sync API-Football fixtures and events into event_* tables.

Design goals:
- Uses existing DB (no schema switch), only event_* tables.
- Never mutates existing match/odds/stats tables.
- Idempotent via upserts and unique event hash.
- Daily call budget control for free plan.
"""

import argparse
import datetime as dt
import hashlib
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from typing import Dict, List, Optional, Tuple

import mysql.connector

API_BASE = "https://v3.football.api-sports.io"
COMPLETED_STATUSES = {"FT", "AET", "PEN"}


def guard_unsafe_secret_flags():
    blocked = ("--password", "--api-key")
    for arg in sys.argv[1:]:
        if any(arg == flag or arg.startswith(flag + "=") for flag in blocked):
            raise SystemExit(
                "Unsafe secret flags detected. Use environment variables MYSQL_PASSWORD and APIFOOTBALL_KEY."
            )


def parse_args():
    guard_unsafe_secret_flags()
    p = argparse.ArgumentParser(description="Sync API-Football fixture events")
    p.add_argument("--host", default=os.getenv("MYSQL_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.getenv("MYSQL_PORT", "3306")))
    p.add_argument("--user", default=os.getenv("MYSQL_USER", "football_admin"))
    p.add_argument("--database", default=os.getenv("MYSQL_DATABASE", "historic_football_data"))
    p.add_argument("--mysql-password-env", default="MYSQL_PASSWORD")
    p.add_argument("--api-key-env", default="APIFOOTBALL_KEY")
    p.add_argument("--league-id", type=int, default=135, help="Serie A = 135")
    p.add_argument("--season-year", type=int, default=0, help="0 means auto-detect current")
    p.add_argument("--daily-limit", type=int, default=100)
    p.add_argument("--reserve", type=int, default=15)
    p.add_argument("--max-event-calls", type=int, default=75)
    p.add_argument(
        "--max-full-event-backfill-calls",
        type=int,
        default=10,
        help="Repoll already-polled fixtures that are missing rows in event_timeline",
    )
    p.add_argument("--sleep-seconds", type=float, default=1.5)
    p.add_argument(
        "--skip-fixture-refresh",
        action="store_true",
        help="Skip /fixtures call and work only on already-present event_fixture rows",
    )
    p.add_argument(
        "--skip-name-normalization",
        action="store_true",
        help="Skip refreshing team_name_alias/team_provider_dim at the end of sync",
    )
    p.add_argument("--log-retention-days", type=int, default=90)
    return p.parse_args()


def connect_db(args):
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
        autocommit=False,
    )


def ensure_runtime_schema_ready(conn):
    required_tables = {
        "event_ingest_state",
        "event_api_call_log",
        "event_fixture",
        "event_goal",
        "event_timeline",
        "event_fixture_match_map",
        "team_name_alias",
        "team_provider_dim",
        "player_dim",
        "player_team_history",
        "player_name_alias",
    }
    required_event_fixture_columns = {
        "events_polled_at",
        "last_events_http_code",
        "last_events_count",
        "last_events_attempt_at",
        "events_attempt_count",
        "next_retry_after",
    }

    cur = conn.cursor()
    try:
        cur.execute("SHOW TABLES")
        existing_tables = {r[0] for r in cur.fetchall()}
        missing_tables = sorted(required_tables - existing_tables)
        if missing_tables:
            raise RuntimeError(
                "Missing required tables: "
                + ", ".join(missing_tables)
                + ". Run setup_schema.py with a migration/admin DB user first."
            )

        cur.execute("SHOW COLUMNS FROM event_fixture")
        existing_columns = {r[0] for r in cur.fetchall()}
        missing_columns = sorted(required_event_fixture_columns - existing_columns)
        if missing_columns:
            raise RuntimeError(
                "Missing required columns in event_fixture: "
                + ", ".join(missing_columns)
                + ". Run setup_schema.py with a migration/admin DB user first."
            )
    finally:
        cur.close()


def db_upsert_state(conn, key: str, value: str):
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO event_ingest_state (state_key, state_value)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE state_value = VALUES(state_value)
            """,
            (key, value),
        )
    finally:
        cur.close()


def refresh_team_name_normalization(conn) -> Dict[str, int]:
    cur = conn.cursor()
    try:
        alias_stmts = [
            """
            INSERT INTO team_name_alias (provider_team_id, alias_name, source_table)
            SELECT ef.home_team_id, ef.home_team_name, 'event_fixture_home'
            FROM event_fixture ef
            WHERE ef.home_team_id IS NOT NULL
              AND ef.home_team_name IS NOT NULL
              AND ef.home_team_name <> ''
            ON DUPLICATE KEY UPDATE last_seen_at = CURRENT_TIMESTAMP
            """,
            """
            INSERT INTO team_name_alias (provider_team_id, alias_name, source_table)
            SELECT ef.away_team_id, ef.away_team_name, 'event_fixture_away'
            FROM event_fixture ef
            WHERE ef.away_team_id IS NOT NULL
              AND ef.away_team_name IS NOT NULL
              AND ef.away_team_name <> ''
            ON DUPLICATE KEY UPDATE last_seen_at = CURRENT_TIMESTAMP
            """,
            """
            INSERT INTO team_name_alias (provider_team_id, alias_name, source_table)
            SELECT et.team_id, et.team_name, 'event_timeline'
            FROM event_timeline et
            WHERE et.team_id IS NOT NULL
              AND et.team_name IS NOT NULL
              AND et.team_name <> ''
            ON DUPLICATE KEY UPDATE last_seen_at = CURRENT_TIMESTAMP
            """,
            """
            INSERT INTO team_name_alias (provider_team_id, alias_name, source_table)
            SELECT eg.team_id, eg.team_name, 'event_goal'
            FROM event_goal eg
            WHERE eg.team_id IS NOT NULL
              AND eg.team_name IS NOT NULL
              AND eg.team_name <> ''
            ON DUPLICATE KEY UPDATE last_seen_at = CURRENT_TIMESTAMP
            """,
            """
            INSERT INTO team_name_alias (provider_team_id, alias_name, source_table)
            SELECT tb.provider_team_id, tb.team_name, 'team_badge'
            FROM team_badge tb
            WHERE tb.provider_team_id IS NOT NULL
              AND tb.team_name IS NOT NULL
              AND tb.team_name <> ''
            ON DUPLICATE KEY UPDATE last_seen_at = CURRENT_TIMESTAMP
            """,
        ]
        for stmt in alias_stmts:
            cur.execute(stmt)

        cur.execute(
            """
            INSERT INTO team_provider_dim (provider_team_id, canonical_team_name, chosen_source)
            SELECT
                tb.provider_team_id,
                SUBSTRING_INDEX(
                    GROUP_CONCAT(tb.team_name ORDER BY tb.updated_at DESC SEPARATOR '\\n'),
                    '\\n',
                    1
                ) AS canonical_team_name,
                'team_badge'
            FROM team_badge tb
            GROUP BY tb.provider_team_id
            ON DUPLICATE KEY UPDATE
                canonical_team_name = VALUES(canonical_team_name),
                chosen_source = VALUES(chosen_source),
                updated_at = CURRENT_TIMESTAMP
            """
        )

        cur.execute(
            """
            INSERT INTO team_provider_dim (provider_team_id, canonical_team_name, chosen_source)
            SELECT
                x.provider_team_id,
                SUBSTRING_INDEX(
                    GROUP_CONCAT(x.alias_name ORDER BY x.last_seen_at DESC SEPARATOR '\\n'),
                    '\\n',
                    1
                ) AS canonical_team_name,
                'team_name_alias'
            FROM team_name_alias x
            LEFT JOIN team_provider_dim d ON d.provider_team_id = x.provider_team_id
            WHERE d.provider_team_id IS NULL
            GROUP BY x.provider_team_id
            """
        )

        cur.execute(
            """
            UPDATE team_provider_dim d
            LEFT JOIN team t ON t.team_name = d.canonical_team_name
            SET d.canonical_team_id = t.team_id
            """
        )

        cur.execute("SELECT COUNT(*) FROM team_name_alias")
        alias_count = int(cur.fetchone()[0])
        cur.execute("SELECT COUNT(*) FROM team_provider_dim")
        dim_count = int(cur.fetchone()[0])
        cur.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT provider_team_id
                FROM team_name_alias
                GROUP BY provider_team_id
                HAVING COUNT(DISTINCT alias_name) > 1
            ) x
            """
        )
        variant_count = int(cur.fetchone()[0])
        return {
            "alias_count": alias_count,
            "dim_count": dim_count,
            "variant_provider_count": variant_count,
        }
    finally:
        cur.close()


def get_fixture_date_utc(conn, fixture_id: int) -> Optional[dt.datetime]:
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT fixture_date_utc FROM event_fixture WHERE provider_fixture_id = %s",
            (fixture_id,),
        )
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        cur.close()


def upsert_player_dim(conn, player_id: int, player_name: str):
    canonical_name = resolve_canonical_player_name(conn, player_id, player_name)
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO player_dim (provider_player_id, player_name)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE
                player_name = VALUES(player_name)
            """,
            (player_id, canonical_name),
        )
    finally:
        cur.close()


def upsert_player_alias(conn, player_id: int, alias_name: str, source_table: str):
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO player_name_alias (provider_player_id, alias_name, source_table)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                last_seen_at = CURRENT_TIMESTAMP
            """,
            (player_id, alias_name, source_table),
        )
    finally:
        cur.close()


def resolve_canonical_player_name(conn, player_id: int, fallback_name: str) -> str:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT alias_name
            FROM player_name_alias
            WHERE provider_player_id = %s
            ORDER BY
                CASE WHEN alias_name LIKE '%%.%%' THEN 1 ELSE 0 END ASC,
                CASE WHEN alias_name LIKE '%% %%' THEN 0 ELSE 1 END ASC,
                CHAR_LENGTH(alias_name) DESC,
                last_seen_at DESC
            LIMIT 1
            """,
            (player_id,),
        )
        row = cur.fetchone()
        if row and row[0]:
            return str(row[0])
        return fallback_name
    finally:
        cur.close()


def upsert_player_team_history(
    conn,
    player_id: int,
    team_id: int,
    observed_at: Optional[dt.datetime],
):
    if observed_at is None:
        observed_at = dt.datetime.now(dt.UTC).replace(tzinfo=None)
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO player_team_history (
                provider_player_id,
                provider_team_id,
                first_seen_at,
                last_seen_at,
                observations
            ) VALUES (%s, %s, %s, %s, 1)
            ON DUPLICATE KEY UPDATE
                first_seen_at = LEAST(first_seen_at, VALUES(first_seen_at)),
                last_seen_at = GREATEST(last_seen_at, VALUES(last_seen_at)),
                observations = observations + 1
            """,
            (player_id, team_id, observed_at, observed_at),
        )
    finally:
        cur.close()


def maybe_capture_player_identity(
    conn,
    player_obj: Dict,
    team_id: Optional[int],
    observed_at: Optional[dt.datetime],
    source_table: str,
):
    player_id = player_obj.get("id")
    player_name = player_obj.get("name")
    if player_id is None or player_name is None or str(player_name).strip() == "":
        return

    pid = int(player_id)
    pname = str(player_name)
    upsert_player_alias(conn, pid, pname, source_table)
    upsert_player_dim(conn, pid, pname)

    if team_id is not None:
        upsert_player_team_history(conn, int(player_id), int(team_id), observed_at)


def capture_players_from_event(conn, fixture_id: int, event: Dict):
    team_id = (event.get("team") or {}).get("id")
    observed_at = get_fixture_date_utc(conn, fixture_id)

    maybe_capture_player_identity(
        conn,
        event.get("player") or {},
        int(team_id) if team_id is not None else None,
        observed_at,
        "event_player",
    )
    maybe_capture_player_identity(
        conn,
        event.get("assist") or {},
        int(team_id) if team_id is not None else None,
        observed_at,
        "event_assist",
    )


def purge_old_api_logs(conn, retention_days: int) -> int:
    if retention_days <= 0:
        return 0
    cutoff = (dt.datetime.now(dt.UTC) - dt.timedelta(days=retention_days)).replace(tzinfo=None)
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM event_api_call_log WHERE created_at < %s", (cutoff,))
        return cur.rowcount
    finally:
        cur.close()


def api_get(path: str, params: Dict[str, str], api_key: str) -> Tuple[int, Dict, Dict[str, str]]:
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


def payload_plan_error(payload: Dict) -> Optional[str]:
    errors = payload.get("errors")
    if isinstance(errors, dict):
        plan_msg = errors.get("plan")
        if plan_msg:
            return str(plan_msg)
    return None


def log_api_call(conn, endpoint: str, params: Dict[str, str], code: int, headers: Dict[str, str]):
    remaining = headers.get("x-ratelimit-requests-remaining")
    remaining_int = int(remaining) if remaining and str(remaining).isdigit() else None
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO event_api_call_log (endpoint, query_params, response_code, requests_remaining, calls_used)
            VALUES (%s, %s, %s, %s, 1)
            """,
            (endpoint, json.dumps(params, sort_keys=True), code, remaining_int),
        )
    finally:
        cur.close()


def get_current_season_year(api_key: str, league_id: int, conn, calls_left: int) -> Tuple[int, int]:
    if calls_left <= 0:
        raise RuntimeError("No call budget available for season discovery")

    code, payload, headers = api_get("/leagues", {"id": str(league_id), "current": "true"}, api_key)
    log_api_call(conn, "/leagues", {"id": str(league_id), "current": "true"}, code, headers)
    if code != 200:
        raise RuntimeError(f"/leagues failed ({code}): {payload}")

    plan_msg = payload_plan_error(payload)
    if plan_msg:
        raise RuntimeError(f"API plan restriction on /leagues: {plan_msg}")

    response = payload.get("response", [])
    if not response:
        raise RuntimeError("No league response found for current=true")

    seasons = response[0].get("seasons", [])
    current = [s for s in seasons if s.get("current")]
    if not current:
        raise RuntimeError("No current season found")
    return int(current[0]["year"]), calls_left - 1


def upsert_fixture(conn, fixture_obj: Dict):
    fixture = fixture_obj.get("fixture", {})
    league = fixture_obj.get("league", {})
    teams = fixture_obj.get("teams", {})
    goals = fixture_obj.get("goals", {})
    status = fixture.get("status", {})

    fid = fixture.get("id")
    if not fid:
        return

    date_str = fixture.get("date")
    date_utc = None
    if date_str:
        # Keep UTC naive DATETIME in DB after parsing ISO8601.
        date_utc = dt.datetime.fromisoformat(date_str.replace("Z", "+00:00")).replace(tzinfo=None)

    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO event_fixture (
                provider_fixture_id, league_id, league_name, season_year,
                fixture_date_utc, status_short, status_long,
                home_team_id, home_team_name, away_team_id, away_team_name,
                goals_home, goals_away, raw_json
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                league_id = VALUES(league_id),
                league_name = VALUES(league_name),
                season_year = VALUES(season_year),
                fixture_date_utc = VALUES(fixture_date_utc),
                status_short = VALUES(status_short),
                status_long = VALUES(status_long),
                home_team_id = VALUES(home_team_id),
                home_team_name = VALUES(home_team_name),
                away_team_id = VALUES(away_team_id),
                away_team_name = VALUES(away_team_name),
                goals_home = VALUES(goals_home),
                goals_away = VALUES(goals_away),
                raw_json = VALUES(raw_json)
            """,
            (
                fid,
                league.get("id"),
                league.get("name"),
                league.get("season"),
                date_utc,
                status.get("short"),
                status.get("long"),
                (teams.get("home") or {}).get("id"),
                (teams.get("home") or {}).get("name"),
                (teams.get("away") or {}).get("id"),
                (teams.get("away") or {}).get("name"),
                goals.get("home"),
                goals.get("away"),
                json.dumps(fixture_obj, ensure_ascii=False),
            ),
        )
    finally:
        cur.close()


def sync_fixtures(conn, api_key: str, league_id: int, season_year: int, calls_left: int, sleep_seconds: float) -> int:
    if calls_left <= 0:
        return calls_left

    # Some plans return all fixtures in one response and reject the page parameter.
    params = {"league": str(league_id), "season": str(season_year)}
    code, payload, headers = api_get("/fixtures", params, api_key)
    log_api_call(conn, "/fixtures", params, code, headers)
    calls_left -= 1

    if code == 429:
        print(
            "WARN: rate limited (429) on /fixtures. "
            "Proceeding without fixture refresh for this run."
        )
        return calls_left

    if code != 200:
        raise RuntimeError(f"/fixtures failed ({code}): {payload}")

    plan_msg = payload_plan_error(payload)
    if plan_msg:
        raise RuntimeError(
            f"API plan restriction on /fixtures for season={season_year}: {plan_msg}"
        )

    for row in payload.get("response", []):
        upsert_fixture(conn, row)

    if sleep_seconds:
        time.sleep(sleep_seconds)

    return calls_left


def get_fixtures_needing_events(conn, league_id: int, season_year: int, max_rows: int) -> List[int]:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT ef.provider_fixture_id
            FROM event_fixture ef
            WHERE ef.league_id = %s
              AND ef.season_year = %s
              AND ef.status_short IN ('FT', 'AET', 'PEN')
              AND ef.events_polled_at IS NULL
              AND (ef.last_events_http_code IS NULL OR ef.last_events_http_code <> 200)
              AND (ef.next_retry_after IS NULL OR ef.next_retry_after <= UTC_TIMESTAMP())
            ORDER BY ef.fixture_date_utc ASC
            LIMIT %s
            """,
            (league_id, season_year, max_rows),
        )
        return [r[0] for r in cur.fetchall()]
    finally:
        cur.close()


def get_polled_fixtures_missing_timeline(
    conn, league_id: int, season_year: int, max_rows: int
) -> List[int]:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT ef.provider_fixture_id
            FROM event_fixture ef
            WHERE ef.league_id = %s
              AND ef.season_year = %s
              AND ef.status_short IN ('FT', 'AET', 'PEN')
              AND (ef.events_polled_at IS NOT NULL OR ef.last_events_http_code = 200)
              AND NOT EXISTS (
                  SELECT 1
                  FROM event_timeline et
                  WHERE et.provider_fixture_id = ef.provider_fixture_id
              )
            ORDER BY ef.fixture_date_utc ASC
            LIMIT %s
            """,
            (league_id, season_year, max_rows),
        )
        return [r[0] for r in cur.fetchall()]
    finally:
        cur.close()


def fixture_already_polled(conn, fixture_id: int) -> bool:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT events_polled_at, last_events_http_code
            FROM event_fixture
            WHERE provider_fixture_id = %s
            """,
            (fixture_id,),
        )
        row = cur.fetchone()
        if row and (row[0] is not None or row[1] == 200):
            return True
        return False
    finally:
        cur.close()


def mark_fixture_polled(conn, fixture_id: int, http_code: int, events_count: int):
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE event_fixture
            SET events_polled_at = %s,
                last_events_http_code = %s,
                last_events_count = %s
            WHERE provider_fixture_id = %s
            """,
            (dt.datetime.now(dt.UTC).replace(tzinfo=None), http_code, events_count, fixture_id),
        )
    finally:
        cur.close()


def mark_fixture_attempt(conn, fixture_id: int):
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE event_fixture
            SET last_events_attempt_at = %s,
                events_attempt_count = COALESCE(events_attempt_count, 0) + 1
            WHERE provider_fixture_id = %s
            """,
            (dt.datetime.now(dt.UTC).replace(tzinfo=None), fixture_id),
        )
    finally:
        cur.close()


def compute_retry_after(attempt_count: int, http_code: int) -> dt.datetime:
    # Exponential-ish cooldown to preserve daily quota.
    if http_code == 429:
        minutes = 60
    elif attempt_count <= 1:
        minutes = 2
    elif attempt_count == 2:
        minutes = 10
    elif attempt_count == 3:
        minutes = 60
    elif attempt_count == 4:
        minutes = 360
    else:
        minutes = 1440
    return (dt.datetime.now(dt.UTC) + dt.timedelta(minutes=minutes)).replace(tzinfo=None)


def mark_fixture_failed(conn, fixture_id: int, http_code: int):
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT COALESCE(events_attempt_count, 0) FROM event_fixture WHERE provider_fixture_id = %s",
            (fixture_id,),
        )
        row = cur.fetchone()
        attempts = int(row[0]) if row else 0
        retry_after = compute_retry_after(attempts, http_code)

        cur.execute(
            """
            UPDATE event_fixture
            SET last_events_http_code = %s,
                next_retry_after = %s
            WHERE provider_fixture_id = %s
            """,
            (http_code, retry_after, fixture_id),
        )
    finally:
        cur.close()


def maybe_insert_goal_event(conn, fixture_id: int, event: Dict):
    if (event.get("type") or "").lower() != "goal":
        return

    time_obj = event.get("time") or {}
    elapsed = time_obj.get("elapsed")
    if elapsed is None:
        return

    # Stable idempotency key for providers that don't expose event ids.
    base = {
        "fixture": fixture_id,
        "elapsed": elapsed,
        "extra": time_obj.get("extra"),
        "team": (event.get("team") or {}).get("id"),
        "player": (event.get("player") or {}).get("id"),
        "assist": (event.get("assist") or {}).get("id"),
        "type": event.get("type"),
        "detail": event.get("detail"),
        "comments": event.get("comments"),
    }
    event_hash = hashlib.sha256(json.dumps(base, sort_keys=True).encode("utf-8")).hexdigest()

    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO event_goal (
                provider_fixture_id, event_hash, team_id, team_name,
                player_id, player_name, assist_id, assist_name,
                elapsed_minute, extra_minute, event_type, event_detail,
                comments, raw_json
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                team_id = VALUES(team_id),
                team_name = VALUES(team_name),
                player_id = VALUES(player_id),
                player_name = VALUES(player_name),
                assist_id = VALUES(assist_id),
                assist_name = VALUES(assist_name),
                elapsed_minute = VALUES(elapsed_minute),
                extra_minute = VALUES(extra_minute),
                event_type = VALUES(event_type),
                event_detail = VALUES(event_detail),
                comments = VALUES(comments),
                raw_json = VALUES(raw_json)
            """,
            (
                fixture_id,
                event_hash,
                (event.get("team") or {}).get("id"),
                (event.get("team") or {}).get("name"),
                (event.get("player") or {}).get("id"),
                (event.get("player") or {}).get("name"),
                (event.get("assist") or {}).get("id"),
                (event.get("assist") or {}).get("name"),
                int(elapsed),
                (int(time_obj.get("extra")) if time_obj.get("extra") is not None else None),
                event.get("type"),
                event.get("detail"),
                event.get("comments"),
                json.dumps(event, ensure_ascii=False),
            ),
        )
    finally:
        cur.close()


def maybe_insert_timeline_event(conn, fixture_id: int, event: Dict):
    time_obj = event.get("time") or {}
    elapsed = time_obj.get("elapsed")

    # Stable idempotency key across all event kinds.
    event_hash = hashlib.sha256(
        json.dumps(
            {
                "fixture": fixture_id,
                "event": event,
            },
            sort_keys=True,
            ensure_ascii=False,
        ).encode("utf-8")
    ).hexdigest()

    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO event_timeline (
                provider_fixture_id, event_hash, team_id, team_name,
                player_id, player_name, assist_id, assist_name,
                elapsed_minute, extra_minute, event_type, event_detail,
                comments, raw_json
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                team_id = VALUES(team_id),
                team_name = VALUES(team_name),
                player_id = VALUES(player_id),
                player_name = VALUES(player_name),
                assist_id = VALUES(assist_id),
                assist_name = VALUES(assist_name),
                elapsed_minute = VALUES(elapsed_minute),
                extra_minute = VALUES(extra_minute),
                event_type = VALUES(event_type),
                event_detail = VALUES(event_detail),
                comments = VALUES(comments),
                raw_json = VALUES(raw_json)
            """,
            (
                fixture_id,
                event_hash,
                (event.get("team") or {}).get("id"),
                (event.get("team") or {}).get("name"),
                (event.get("player") or {}).get("id"),
                (event.get("player") or {}).get("name"),
                (event.get("assist") or {}).get("id"),
                (event.get("assist") or {}).get("name"),
                (int(elapsed) if elapsed is not None else None),
                (int(time_obj.get("extra")) if time_obj.get("extra") is not None else None),
                event.get("type"),
                event.get("detail"),
                event.get("comments"),
                json.dumps(event, ensure_ascii=False),
            ),
        )
    finally:
        cur.close()


def sync_events(
    conn,
    api_key: str,
    fixture_ids: List[int],
    calls_left: int,
    sleep_seconds: float,
    force_repoll: bool = False,
) -> int:
    rate_limited = False
    for fid in fixture_ids:
        if calls_left <= 0:
            break

        # Double-check in DB before polling to avoid duplicate event calls.
        if not force_repoll and fixture_already_polled(conn, fid):
            continue

        mark_fixture_attempt(conn, fid)

        params = {"fixture": str(fid)}
        code, payload, headers = api_get("/fixtures/events", params, api_key)
        log_api_call(conn, "/fixtures/events", params, code, headers)
        calls_left -= 1

        if code == 429:
            mark_fixture_failed(conn, fid, code)
            print(
                "WARN: rate limited (429) on /fixtures/events. "
                "Stopping this run early to avoid wasting request budget."
            )
            rate_limited = True
            break

        if code != 200:
            mark_fixture_failed(conn, fid, code)
            print(f"WARN: events failed for fixture={fid}, code={code}")
            continue

        response_events = payload.get("response", [])
        for event in payload.get("response", []):
            capture_players_from_event(conn, fid, event)
            maybe_insert_timeline_event(conn, fid, event)
            maybe_insert_goal_event(conn, fid, event)

        mark_fixture_polled(conn, fid, code, len(response_events))
        # Success clears retry cooldown.
        cur = conn.cursor()
        try:
            cur.execute(
                "UPDATE event_fixture SET next_retry_after = NULL WHERE provider_fixture_id = %s",
                (fid,),
            )
        finally:
            cur.close()

        if sleep_seconds:
            time.sleep(sleep_seconds)

    if rate_limited:
        print("INFO: Resume later with the same command; ingestion is idempotent.")

    return calls_left


def main():
    args = parse_args()
    api_key = os.getenv(args.api_key_env, "")
    if not api_key:
        raise SystemExit("APIFOOTBALL_KEY missing. Set environment variable APIFOOTBALL_KEY.")

    usable_calls = max(0, args.daily_limit - args.reserve)
    if usable_calls <= 0:
        raise SystemExit("Invalid budget: reserve must be lower than daily-limit")

    conn = connect_db(args)
    try:
        ensure_runtime_schema_ready(conn)

        calls_left = usable_calls
        season_year = args.season_year
        if season_year <= 0:
            season_year, calls_left = get_current_season_year(api_key, args.league_id, conn, calls_left)
            print(f"Detected current season year: {season_year}")

        print(f"Daily budget: total={args.daily_limit}, reserve={args.reserve}, usable={usable_calls}")
        print(f"Sync fixtures for league={args.league_id}, season={season_year}")
        if args.skip_fixture_refresh:
            print("Skipping fixture refresh (--skip-fixture-refresh enabled).")
        else:
            calls_left = sync_fixtures(
                conn,
                api_key,
                args.league_id,
                season_year,
                calls_left,
                args.sleep_seconds,
            )
            conn.commit()

        max_event_calls = min(args.max_event_calls, calls_left)
        fixture_ids = get_fixtures_needing_events(conn, args.league_id, season_year, max_event_calls)
        print(f"Fixtures missing events (processing now): {len(fixture_ids)}")

        calls_left = sync_events(conn, api_key, fixture_ids, calls_left, args.sleep_seconds)
        conn.commit()

        backfill_calls = min(args.max_full_event_backfill_calls, calls_left)
        if backfill_calls > 0:
            repoll_ids = get_polled_fixtures_missing_timeline(
                conn, args.league_id, season_year, backfill_calls
            )
            print(
                "Already-polled fixtures missing full timeline "
                f"(repoll now): {len(repoll_ids)}"
            )
            calls_left = sync_events(
                conn,
                api_key,
                repoll_ids,
                calls_left,
                args.sleep_seconds,
                force_repoll=True,
            )
            conn.commit()

        db_upsert_state(conn, "last_sync_league", str(args.league_id))
        db_upsert_state(conn, "last_sync_season_year", str(season_year))
        db_upsert_state(conn, "last_sync_utc", dt.datetime.now(dt.UTC).isoformat())
        name_stats = None
        if args.skip_name_normalization:
            print("Skipping team name normalization (--skip-name-normalization enabled).")
        else:
            name_stats = refresh_team_name_normalization(conn)
        purged = purge_old_api_logs(conn, args.log_retention_days)
        conn.commit()

        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT COUNT(*) FROM event_fixture WHERE league_id=%s AND season_year=%s",
                (args.league_id, season_year),
            )
            fixtures_count = cur.fetchone()[0]
            cur.execute(
                """
                SELECT COUNT(*)
                FROM event_goal eg
                JOIN event_fixture ef ON ef.provider_fixture_id = eg.provider_fixture_id
                WHERE ef.league_id=%s AND ef.season_year=%s
                """,
                (args.league_id, season_year),
            )
            goals_count = cur.fetchone()[0]
            cur.execute(
                """
                SELECT COUNT(*)
                FROM event_timeline et
                JOIN event_fixture ef ON ef.provider_fixture_id = et.provider_fixture_id
                WHERE ef.league_id=%s AND ef.season_year=%s
                """,
                (args.league_id, season_year),
            )
            timeline_count = cur.fetchone()[0]
        finally:
            cur.close()

        print(f"event_fixture rows (league/season): {fixtures_count}")
        print(f"event_goal rows (league/season): {goals_count}")
        print(f"event_timeline rows (league/season): {timeline_count}")
        if name_stats is not None:
            print(f"team_name_alias rows: {name_stats['alias_count']}")
            print(f"team_provider_dim rows: {name_stats['dim_count']}")
            print(
                "providers with >1 alias: "
                f"{name_stats['variant_provider_count']}"
            )
        print(f"event_api_call_log purged rows: {purged}")
        print(f"Calls left in run budget: {calls_left}")
        print("Sync completed successfully.")

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
