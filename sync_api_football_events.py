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
from typing import Any, Dict, List, Optional, Tuple

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
    p.add_argument(
        "--season-count",
        type=int,
        default=3,
        help="When --season-year=0, sync current season and this many recent seasons total.",
    )
    p.add_argument("--daily-limit", type=int, default=100)
    p.add_argument("--reserve", type=int, default=15)
    p.add_argument("--max-event-calls", type=int, default=75)
    p.add_argument("--max-stats-calls", type=int, default=75)
    p.add_argument("--max-lineup-calls", type=int, default=75)
    p.add_argument(
        "--max-full-event-backfill-calls",
        type=int,
        default=10,
        help="Repoll already-polled fixtures that are missing rows in event_timeline",
    )
    p.add_argument("--sleep-seconds", type=float, default=1.5)
    p.add_argument(
        "--disable-adaptive-throttle",
        action="store_true",
        help="Disable header-aware throttling and use fixed --sleep-seconds pacing only.",
    )
    p.add_argument(
        "--adaptive-throttle-max-seconds",
        type=float,
        default=8.0,
        help="Upper bound for adaptive inter-request delay.",
    )
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
    p.add_argument(
        "--skip-stats-sync",
        action="store_true",
        help="Skip /fixtures/statistics ingestion.",
    )
    p.add_argument(
        "--skip-lineups-sync",
        action="store_true",
        help="Skip /fixtures/lineups ingestion for player enrichment.",
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
        "event_fixture_enrichment_state",
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


def _header_value(headers: Dict[str, str], name: str) -> Optional[str]:
    wanted = name.lower()
    for k, v in headers.items():
        if str(k).lower() == wanted:
            return str(v)
    return None


def _header_int(headers: Dict[str, str], names: List[str]) -> Optional[int]:
    for name in names:
        raw = _header_value(headers, name)
        if raw is None:
            continue
        token = raw.strip()
        if not token:
            continue
        try:
            return int(float(token))
        except ValueError:
            continue
    return None


def compute_adaptive_throttle_seconds(
    headers: Dict[str, str],
    calls_left: int,
    sleep_seconds: float,
    max_sleep_seconds: float,
) -> float:
    base = max(0.0, sleep_seconds)
    cap = max(0.0, max_sleep_seconds)

    remaining = _header_int(headers, ["x-ratelimit-requests-remaining"])
    limit = _header_int(
        headers,
        ["x-ratelimit-requests-limit", "x-ratelimit-limit", "x-ratelimit-requests"],
    )
    reset_epoch = _header_int(headers, ["x-ratelimit-requests-reset", "x-ratelimit-reset"])

    if remaining is None:
        return min(base, cap) if cap > 0 else base

    if remaining <= 1:
        emergency = max(base, 5.0)
        return min(emergency, cap) if cap > 0 else emergency

    delay = base

    if reset_epoch is not None:
        now_epoch = int(time.time())
        seconds_until_reset = max(1, reset_epoch - now_epoch)
        delay = seconds_until_reset / max(1, remaining)
    elif limit is not None and limit > 0:
        ratio = remaining / float(limit)
        if ratio >= 0.5:
            delay = 0.0
        elif ratio >= 0.25:
            delay = base * 0.5
        elif ratio >= 0.1:
            delay = base
        else:
            delay = max(base * 2.0, 1.0)
    elif calls_left > 0:
        pressure = calls_left / float(max(1, remaining))
        delay = base * min(3.0, max(0.5, pressure))

    if cap > 0:
        delay = min(delay, cap)
    return max(0.0, delay)


def maybe_sleep_between_requests(
    headers: Dict[str, str],
    calls_left: int,
    sleep_seconds: float,
    adaptive_throttle: bool,
    adaptive_throttle_max_seconds: float,
):
    if adaptive_throttle:
        delay = compute_adaptive_throttle_seconds(
            headers=headers,
            calls_left=calls_left,
            sleep_seconds=sleep_seconds,
            max_sleep_seconds=adaptive_throttle_max_seconds,
        )
    else:
        delay = max(0.0, sleep_seconds)

    if delay > 0:
        time.sleep(delay)


def log_api_call(conn, endpoint: str, params: Dict[str, str], code: int, headers: Dict[str, str]):
    remaining_int = _header_int(headers, ["x-ratelimit-requests-remaining"])
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


def get_recent_season_years(
    api_key: str,
    league_id: int,
    conn,
    calls_left: int,
    season_count: int,
) -> Tuple[List[int], int]:
    if season_count <= 0:
        raise RuntimeError("season_count must be positive")

    # Anchor rolling window to today's year so season_count always means
    # "now back N seasons" even if older data was previously synced.
    current_year = dt.datetime.now(dt.UTC).year
    api_current_year: Optional[int] = None
    if calls_left > 0:
        params = {"id": str(league_id)}
        code, payload, headers = api_get("/leagues", params, api_key)
        log_api_call(conn, "/leagues", params, code, headers)
        calls_left -= 1

        if code == 200:
            response = payload.get("response", [])
            if response:
                seasons = response[0].get("seasons", [])
                years = [int(s.get("year")) for s in seasons if s.get("year") is not None]
                if years:
                    api_current_year = max(years)
                for s in seasons:
                    if s.get("current") and s.get("year") is not None:
                        api_current_year = int(s.get("year"))
                        break

    # Prefer whichever anchor is newer between API current-season metadata and today's year.
    if api_current_year is not None:
        current_year = max(current_year, api_current_year)

    target = [current_year - i for i in range(season_count)]
    return target, calls_left


def _ensure_fixture_enrichment_row(conn, fixture_id: int):
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO event_fixture_enrichment_state (provider_fixture_id)
            VALUES (%s)
            ON DUPLICATE KEY UPDATE provider_fixture_id = VALUES(provider_fixture_id)
            """,
            (fixture_id,),
        )
    finally:
        cur.close()


def mark_fixture_stats_polled(conn, fixture_id: int, http_code: int):
    _ensure_fixture_enrichment_row(conn, fixture_id)
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE event_fixture_enrichment_state
            SET stats_polled_at = %s,
                last_stats_http_code = %s
            WHERE provider_fixture_id = %s
            """,
            (dt.datetime.now(dt.UTC).replace(tzinfo=None), http_code, fixture_id),
        )
    finally:
        cur.close()


def mark_fixture_lineups_polled(conn, fixture_id: int, http_code: int):
    _ensure_fixture_enrichment_row(conn, fixture_id)
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE event_fixture_enrichment_state
            SET lineups_polled_at = %s,
                last_lineups_http_code = %s
            WHERE provider_fixture_id = %s
            """,
            (dt.datetime.now(dt.UTC).replace(tzinfo=None), http_code, fixture_id),
        )
    finally:
        cur.close()


def get_fixtures_needing_stats(conn, league_id: int, season_year: int, max_rows: int) -> List[int]:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT ef.provider_fixture_id
            FROM event_fixture ef
            JOIN event_fixture_match_map mm
              ON mm.provider_fixture_id = ef.provider_fixture_id
             AND mm.match_id IS NOT NULL
            LEFT JOIN event_fixture_enrichment_state es
              ON es.provider_fixture_id = ef.provider_fixture_id
            WHERE ef.league_id = %s
              AND ef.season_year = %s
              AND ef.status_short IN ('FT', 'AET', 'PEN')
              AND (es.stats_polled_at IS NULL OR es.last_stats_http_code <> 200)
            ORDER BY ef.fixture_date_utc ASC
            LIMIT %s
            """,
            (league_id, season_year, max_rows),
        )
        return [int(r[0]) for r in cur.fetchall()]
    finally:
        cur.close()


def get_fixtures_needing_lineups(conn, league_id: int, season_year: int, max_rows: int) -> List[int]:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT ef.provider_fixture_id
            FROM event_fixture ef
            LEFT JOIN event_fixture_enrichment_state es
              ON es.provider_fixture_id = ef.provider_fixture_id
            WHERE ef.league_id = %s
              AND ef.season_year = %s
              AND ef.status_short IN ('FT', 'AET', 'PEN')
              AND (es.lineups_polled_at IS NULL OR es.last_lineups_http_code <> 200)
            ORDER BY ef.fixture_date_utc DESC
            LIMIT %s
            """,
            (league_id, season_year, max_rows),
        )
        return [int(r[0]) for r in cur.fetchall()]
    finally:
        cur.close()


def _parse_stat_value(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)

    text = str(value).strip()
    if not text or text.upper() in {"N/A", "NULL", "NONE"}:
        return None
    if text.endswith("%"):
        text = text[:-1].strip()
    try:
        return int(float(text))
    except ValueError:
        return None


def _get_fixture_match_context(conn, fixture_id: int) -> Optional[Tuple[int, Optional[int], Optional[int]]]:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT mm.match_id, ef.home_team_id, ef.away_team_id
            FROM event_fixture ef
            LEFT JOIN event_fixture_match_map mm ON mm.provider_fixture_id = ef.provider_fixture_id
            WHERE ef.provider_fixture_id = %s
            """,
            (fixture_id,),
        )
        row = cur.fetchone()
        if not row or row[0] is None:
            return None
        return int(row[0]), (int(row[1]) if row[1] is not None else None), (int(row[2]) if row[2] is not None else None)
    finally:
        cur.close()


def upsert_match_stats_from_api(conn, fixture_id: int, response_rows: List[Dict[str, Any]]):
    ctx = _get_fixture_match_context(conn, fixture_id)
    if ctx is None:
        return

    match_id, home_team_id, away_team_id = ctx

    stat_aliases = {
        "shots on goal": "shots_on_target",
        "total shots": "shots",
        "fouls": "fouls",
        "corner kicks": "corners",
        "yellow cards": "yellow_cards",
        "red cards": "red_cards",
        "offsides": "offsides",
        "hit woodwork": "hit_woodwork",
        "free kicks": "free_kicks_conceded",
    }

    by_side: Dict[str, Dict[str, Optional[int]]] = {
        "home": {},
        "away": {},
    }

    for team_block in response_rows:
        team_id = (team_block.get("team") or {}).get("id")
        side = None
        if home_team_id is not None and team_id == home_team_id:
            side = "home"
        elif away_team_id is not None and team_id == away_team_id:
            side = "away"
        elif side is None:
            if not by_side["home"]:
                side = "home"
            elif not by_side["away"]:
                side = "away"
        if side is None:
            continue

        for stat in team_block.get("statistics", []):
            name = str(stat.get("type", "")).strip().lower()
            metric = stat_aliases.get(name)
            if metric is None:
                continue
            by_side[side][metric] = _parse_stat_value(stat.get("value"))

    hy = by_side["home"].get("yellow_cards")
    hr = by_side["home"].get("red_cards")
    ay = by_side["away"].get("yellow_cards")
    ar = by_side["away"].get("red_cards")
    home_booking_points = (0 if hy is None else hy * 10) + (0 if hr is None else hr * 25)
    away_booking_points = (0 if ay is None else ay * 10) + (0 if ar is None else ar * 25)

    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO match_stats (
                match_id,
                home_shots,
                away_shots,
                home_shots_on_target,
                away_shots_on_target,
                home_fouls,
                away_fouls,
                home_corners,
                away_corners,
                home_yellow_cards,
                away_yellow_cards,
                home_red_cards,
                away_red_cards,
                home_offsides,
                away_offsides,
                home_hit_woodwork,
                away_hit_woodwork,
                home_booking_points,
                away_booking_points,
                home_free_kicks_conceded,
                away_free_kicks_conceded
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                home_shots = VALUES(home_shots),
                away_shots = VALUES(away_shots),
                home_shots_on_target = VALUES(home_shots_on_target),
                away_shots_on_target = VALUES(away_shots_on_target),
                home_fouls = VALUES(home_fouls),
                away_fouls = VALUES(away_fouls),
                home_corners = VALUES(home_corners),
                away_corners = VALUES(away_corners),
                home_yellow_cards = VALUES(home_yellow_cards),
                away_yellow_cards = VALUES(away_yellow_cards),
                home_red_cards = VALUES(home_red_cards),
                away_red_cards = VALUES(away_red_cards),
                home_offsides = VALUES(home_offsides),
                away_offsides = VALUES(away_offsides),
                home_hit_woodwork = VALUES(home_hit_woodwork),
                away_hit_woodwork = VALUES(away_hit_woodwork),
                home_booking_points = VALUES(home_booking_points),
                away_booking_points = VALUES(away_booking_points),
                home_free_kicks_conceded = VALUES(home_free_kicks_conceded),
                away_free_kicks_conceded = VALUES(away_free_kicks_conceded)
            """,
            (
                match_id,
                by_side["home"].get("shots"),
                by_side["away"].get("shots"),
                by_side["home"].get("shots_on_target"),
                by_side["away"].get("shots_on_target"),
                by_side["home"].get("fouls"),
                by_side["away"].get("fouls"),
                by_side["home"].get("corners"),
                by_side["away"].get("corners"),
                by_side["home"].get("yellow_cards"),
                by_side["away"].get("yellow_cards"),
                by_side["home"].get("red_cards"),
                by_side["away"].get("red_cards"),
                by_side["home"].get("offsides"),
                by_side["away"].get("offsides"),
                by_side["home"].get("hit_woodwork"),
                by_side["away"].get("hit_woodwork"),
                home_booking_points,
                away_booking_points,
                by_side["home"].get("free_kicks_conceded"),
                by_side["away"].get("free_kicks_conceded"),
            ),
        )
    finally:
        cur.close()


def capture_players_from_lineups(conn, fixture_id: int, response_rows: List[Dict[str, Any]]):
    observed_at = get_fixture_date_utc(conn, fixture_id)
    for team_block in response_rows:
        team_id = (team_block.get("team") or {}).get("id")
        if team_id is None:
            continue
        team_id_int = int(team_id)

        for item in team_block.get("startXI", []):
            player = item.get("player") or {}
            maybe_capture_player_identity(
                conn,
                player,
                team_id_int,
                observed_at,
                "lineup_startxi",
            )

        for item in team_block.get("substitutes", []):
            player = item.get("player") or {}
            maybe_capture_player_identity(
                conn,
                player,
                team_id_int,
                observed_at,
                "lineup_substitute",
            )


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


def sync_fixtures(
    conn,
    api_key: str,
    league_id: int,
    season_year: int,
    calls_left: int,
    sleep_seconds: float,
    adaptive_throttle: bool = True,
    adaptive_throttle_max_seconds: float = 8.0,
) -> int:
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

    maybe_sleep_between_requests(
        headers=headers,
        calls_left=calls_left,
        sleep_seconds=sleep_seconds,
        adaptive_throttle=adaptive_throttle,
        adaptive_throttle_max_seconds=adaptive_throttle_max_seconds,
    )

    return calls_left


def sync_fixture_stats(
    conn,
    api_key: str,
    fixture_ids: List[int],
    calls_left: int,
    sleep_seconds: float,
    adaptive_throttle: bool,
    adaptive_throttle_max_seconds: float,
) -> int:
    rate_limited = False
    for fid in fixture_ids:
        if calls_left <= 0:
            break

        params = {"fixture": str(fid)}
        code, payload, headers = api_get("/fixtures/statistics", params, api_key)
        log_api_call(conn, "/fixtures/statistics", params, code, headers)
        calls_left -= 1

        if code == 429:
            mark_fixture_stats_polled(conn, fid, code)
            print("WARN: rate limited (429) on /fixtures/statistics. Stopping stats pass.")
            rate_limited = True
            break

        if code != 200:
            mark_fixture_stats_polled(conn, fid, code)
            print(f"WARN: statistics failed for fixture={fid}, code={code}")
            continue

        upsert_match_stats_from_api(conn, fid, payload.get("response", []))
        mark_fixture_stats_polled(conn, fid, code)

        maybe_sleep_between_requests(
            headers=headers,
            calls_left=calls_left,
            sleep_seconds=sleep_seconds,
            adaptive_throttle=adaptive_throttle,
            adaptive_throttle_max_seconds=adaptive_throttle_max_seconds,
        )

    if rate_limited:
        print("INFO: Resume later with the same command; stats sync is idempotent.")
    return calls_left


def sync_fixture_lineups(
    conn,
    api_key: str,
    fixture_ids: List[int],
    calls_left: int,
    sleep_seconds: float,
    adaptive_throttle: bool,
    adaptive_throttle_max_seconds: float,
) -> int:
    rate_limited = False
    for fid in fixture_ids:
        if calls_left <= 0:
            break

        params = {"fixture": str(fid)}
        code, payload, headers = api_get("/fixtures/lineups", params, api_key)
        log_api_call(conn, "/fixtures/lineups", params, code, headers)
        calls_left -= 1

        if code == 429:
            mark_fixture_lineups_polled(conn, fid, code)
            print("WARN: rate limited (429) on /fixtures/lineups. Stopping lineup pass.")
            rate_limited = True
            break

        if code != 200:
            mark_fixture_lineups_polled(conn, fid, code)
            print(f"WARN: lineups failed for fixture={fid}, code={code}")
            continue

        capture_players_from_lineups(conn, fid, payload.get("response", []))
        mark_fixture_lineups_polled(conn, fid, code)

        maybe_sleep_between_requests(
            headers=headers,
            calls_left=calls_left,
            sleep_seconds=sleep_seconds,
            adaptive_throttle=adaptive_throttle,
            adaptive_throttle_max_seconds=adaptive_throttle_max_seconds,
        )

    if rate_limited:
        print("INFO: Resume later with the same command; lineup sync is idempotent.")
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
                last_events_count = %s,
                next_retry_after = NULL
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


def _build_goal_event_row(fixture_id: int, event: Dict) -> Optional[Tuple]:
    if (event.get("type") or "").lower() != "goal":
        return None

    time_obj = event.get("time") or {}
    elapsed = time_obj.get("elapsed")
    if elapsed is None:
        return None

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
    return (
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
    )


def _build_timeline_event_row(fixture_id: int, event: Dict) -> Tuple:
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

    return (
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
    )


def upsert_timeline_events_batch(conn, rows: List[Tuple]):
    if not rows:
        return

    cur = conn.cursor()
    try:
        cur.executemany(
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
            rows,
        )
    finally:
        cur.close()


def upsert_goal_events_batch(conn, rows: List[Tuple]):
    if not rows:
        return

    cur = conn.cursor()
    try:
        cur.executemany(
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
            rows,
        )
    finally:
        cur.close()


def sync_events(
    conn,
    api_key: str,
    fixture_ids: List[int],
    calls_left: int,
    sleep_seconds: float,
    adaptive_throttle: bool,
    adaptive_throttle_max_seconds: float,
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
        timeline_rows: List[Tuple] = []
        goal_rows: List[Tuple] = []
        for event in response_events:
            capture_players_from_event(conn, fid, event)
            timeline_rows.append(_build_timeline_event_row(fid, event))
            goal_row = _build_goal_event_row(fid, event)
            if goal_row is not None:
                goal_rows.append(goal_row)

        upsert_timeline_events_batch(conn, timeline_rows)
        upsert_goal_events_batch(conn, goal_rows)

        mark_fixture_polled(conn, fid, code, len(response_events))

        maybe_sleep_between_requests(
            headers=headers,
            calls_left=calls_left,
            sleep_seconds=sleep_seconds,
            adaptive_throttle=adaptive_throttle,
            adaptive_throttle_max_seconds=adaptive_throttle_max_seconds,
        )

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
        if args.season_year > 0:
            target_seasons = [args.season_year]
        else:
            target_seasons, calls_left = get_recent_season_years(
                api_key,
                args.league_id,
                conn,
                calls_left,
                args.season_count,
            )

        if not target_seasons:
            raise RuntimeError("No target seasons resolved for sync")

        print(f"Daily budget: total={args.daily_limit}, reserve={args.reserve}, usable={usable_calls}")
        print(f"Sync fixtures for league={args.league_id}, seasons={target_seasons}")
        adaptive_throttle_enabled = not args.disable_adaptive_throttle
        if adaptive_throttle_enabled:
            print(
                "Adaptive throttle: enabled "
                f"(base_sleep={args.sleep_seconds}, cap={args.adaptive_throttle_max_seconds})"
            )
        else:
            print(f"Adaptive throttle: disabled (fixed sleep={args.sleep_seconds})")

        for season_year in target_seasons:
            print(f"--- Season {season_year} ---")
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
                    adaptive_throttle_enabled,
                    args.adaptive_throttle_max_seconds,
                )
                conn.commit()

            max_event_calls = min(args.max_event_calls, calls_left)
            fixture_ids = get_fixtures_needing_events(conn, args.league_id, season_year, max_event_calls)
            print(f"Fixtures missing events (processing now): {len(fixture_ids)}")

            calls_left = sync_events(
                conn,
                api_key,
                fixture_ids,
                calls_left,
                args.sleep_seconds,
                adaptive_throttle_enabled,
                args.adaptive_throttle_max_seconds,
            )
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
                    adaptive_throttle_enabled,
                    args.adaptive_throttle_max_seconds,
                    force_repoll=True,
                )
                conn.commit()

            if args.skip_stats_sync:
                print("Skipping fixture statistics sync (--skip-stats-sync enabled).")
            else:
                max_stats_calls = min(args.max_stats_calls, calls_left)
                stats_fixture_ids = get_fixtures_needing_stats(
                    conn,
                    args.league_id,
                    season_year,
                    max_stats_calls,
                )
                print(f"Fixtures needing statistics sync: {len(stats_fixture_ids)}")
                calls_left = sync_fixture_stats(
                    conn,
                    api_key,
                    stats_fixture_ids,
                    calls_left,
                    args.sleep_seconds,
                    adaptive_throttle_enabled,
                    args.adaptive_throttle_max_seconds,
                )
                conn.commit()

            if args.skip_lineups_sync:
                print("Skipping lineup/player sync (--skip-lineups-sync enabled).")
            else:
                max_lineup_calls = min(args.max_lineup_calls, calls_left)
                lineup_fixture_ids = get_fixtures_needing_lineups(
                    conn,
                    args.league_id,
                    season_year,
                    max_lineup_calls,
                )
                print(f"Fixtures needing lineup sync: {len(lineup_fixture_ids)}")
                calls_left = sync_fixture_lineups(
                    conn,
                    api_key,
                    lineup_fixture_ids,
                    calls_left,
                    args.sleep_seconds,
                    adaptive_throttle_enabled,
                    args.adaptive_throttle_max_seconds,
                )
                conn.commit()

        db_upsert_state(conn, "last_sync_league", str(args.league_id))
        db_upsert_state(conn, "last_sync_season_year", str(target_seasons[0]))
        db_upsert_state(conn, "last_sync_season_years", ",".join(str(y) for y in target_seasons))
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
                (args.league_id, target_seasons[0]),
            )
            fixtures_count = cur.fetchone()[0]
            cur.execute(
                """
                SELECT COUNT(*)
                FROM event_goal eg
                JOIN event_fixture ef ON ef.provider_fixture_id = eg.provider_fixture_id
                WHERE ef.league_id=%s AND ef.season_year=%s
                """,
                (args.league_id, target_seasons[0]),
            )
            goals_count = cur.fetchone()[0]
            cur.execute(
                """
                SELECT COUNT(*)
                FROM event_timeline et
                JOIN event_fixture ef ON ef.provider_fixture_id = et.provider_fixture_id
                WHERE ef.league_id=%s AND ef.season_year=%s
                """,
                (args.league_id, target_seasons[0]),
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
