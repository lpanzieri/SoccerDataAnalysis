#!/usr/bin/env python3
"""One-time schema setup/migration for all project tables.

Use an admin/migration DB user for this script.
Runtime sync/downloader scripts should run with reduced privileges and do not execute DDL.
"""

import argparse
import hashlib
import os
import sys
from pathlib import Path

import mysql.connector


REQUIRED_EVENT_FIXTURE_COLUMNS = {
    "events_polled_at": "ALTER TABLE event_fixture ADD COLUMN events_polled_at DATETIME NULL",
    "last_events_http_code": "ALTER TABLE event_fixture ADD COLUMN last_events_http_code INT NULL",
    "last_events_count": "ALTER TABLE event_fixture ADD COLUMN last_events_count INT NULL",
    "last_events_attempt_at": "ALTER TABLE event_fixture ADD COLUMN last_events_attempt_at DATETIME NULL",
    "events_attempt_count": "ALTER TABLE event_fixture ADD COLUMN events_attempt_count INT NOT NULL DEFAULT 0",
    "next_retry_after": "ALTER TABLE event_fixture ADD COLUMN next_retry_after DATETIME NULL",
}


def guard_unsafe_secret_flags():
    for arg in sys.argv[1:]:
        if arg == "--password" or arg.startswith("--password="):
            raise SystemExit("Unsafe secret flag detected. Use environment variable MYSQL_PASSWORD.")


def parse_args():
    guard_unsafe_secret_flags()
    p = argparse.ArgumentParser(description="Setup consolidated database schema")
    p.add_argument("--host", default=os.getenv("MYSQL_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.getenv("MYSQL_PORT", "3306")))
    p.add_argument("--user", default=os.getenv("MYSQL_USER", "football_admin"))
    p.add_argument("--database", default=os.getenv("MYSQL_DATABASE", "historic_football_data"))
    p.add_argument("--mysql-password-env", default="MYSQL_PASSWORD")
    p.add_argument("--schema-file", default="schema.sql")
    p.add_argument(
        "--schema-sha256",
        default="",
        help="Optional expected SHA-256 for schema file integrity check",
    )
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


def run_schema_sql(conn, schema_path: Path):
    sql = schema_path.read_text(encoding="utf-8")
    cur = conn.cursor()
    try:
        for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
            cur.execute(stmt)
    finally:
        cur.close()


def verify_schema_hash(schema_path: Path, expected_hash: str):
    if not expected_hash:
        return
    digest = hashlib.sha256(schema_path.read_bytes()).hexdigest()
    if digest.lower() != expected_hash.lower():
        raise SystemExit(
            f"Schema hash mismatch for {schema_path}. expected={expected_hash} actual={digest}"
        )


def run_migrations(conn):
    cur = conn.cursor()
    try:
        cur.execute("SHOW COLUMNS FROM event_fixture")
        existing = {r[0] for r in cur.fetchall()}
        for col, ddl in REQUIRED_EVENT_FIXTURE_COLUMNS.items():
            if col not in existing:
                cur.execute(ddl)
    finally:
        cur.close()


def _has_foreign_key_constraint(conn, table_name: str, constraint_name: str) -> bool:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.TABLE_CONSTRAINTS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = %s
              AND CONSTRAINT_NAME = %s
              AND CONSTRAINT_TYPE = 'FOREIGN KEY'
            LIMIT 1
            """,
            (table_name, constraint_name),
        )
        return cur.fetchone() is not None
    finally:
        cur.close()


def _count_player_orphans(conn, table_name: str, player_col: str) -> int:
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM {table_name} t
            LEFT JOIN player_dim pd ON pd.provider_player_id = t.{player_col}
            WHERE pd.provider_player_id IS NULL
            """
        )
        row = cur.fetchone()
        return int(row[0] if row else 0)
    finally:
        cur.close()


def enforce_player_integrity(conn):
    """Backfill missing player_dim rows and enforce foreign keys for player references."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO player_dim (provider_player_id, player_name)
            SELECT
                pi.provider_player_id,
                COALESCE(NULLIF(MAX(pi.player_name), ''), CONCAT('player_', pi.provider_player_id))
            FROM player_injury pi
            LEFT JOIN player_dim pd ON pd.provider_player_id = pi.provider_player_id
            WHERE pd.provider_player_id IS NULL
            GROUP BY pi.provider_player_id
            """
        )

        cur.execute(
            """
            INSERT INTO player_dim (provider_player_id, player_name)
            SELECT
                flp.player_id,
                COALESCE(NULLIF(MAX(flp.player_name), ''), CONCAT('player_', flp.player_id))
            FROM fixture_lineup_player flp
            LEFT JOIN player_dim pd ON pd.provider_player_id = flp.player_id
            WHERE pd.provider_player_id IS NULL
            GROUP BY flp.player_id
            """
        )
    finally:
        cur.close()

    orphan_injury = _count_player_orphans(conn, "player_injury", "provider_player_id")
    orphan_lineup = _count_player_orphans(conn, "fixture_lineup_player", "player_id")
    if orphan_injury > 0 or orphan_lineup > 0:
        raise RuntimeError(
            "Player integrity migration blocked: unresolved orphans remain "
            f"(player_injury={orphan_injury}, fixture_lineup_player={orphan_lineup})"
        )

    cur = conn.cursor()
    try:
        if not _has_foreign_key_constraint(conn, "player_injury", "fk_player_injury_player"):
            cur.execute(
                """
                ALTER TABLE player_injury
                ADD CONSTRAINT fk_player_injury_player
                FOREIGN KEY (provider_player_id)
                REFERENCES player_dim(provider_player_id)
                ON UPDATE CASCADE
                ON DELETE RESTRICT
                """
            )

        if not _has_foreign_key_constraint(conn, "fixture_lineup_player", "fk_lineup_player_player"):
            cur.execute(
                """
                ALTER TABLE fixture_lineup_player
                ADD CONSTRAINT fk_lineup_player_player
                FOREIGN KEY (player_id)
                REFERENCES player_dim(provider_player_id)
                ON UPDATE CASCADE
                ON DELETE RESTRICT
                """
            )
    finally:
        cur.close()


def validate_required_tables(conn):
    required_tables = {
        "league",
        "season",
        "team",
        "player_dim",
        "player_team_history",
        "player_name_alias",
        "ingest_batch",
        "raw_match_row",
        "match_game",
        "match_stats",
        "odds_quote",
        "ingest_error",
        "event_ingest_state",
        "event_api_call_log",
        "event_fixture",
        "event_goal",
        "event_timeline",
        "event_fixture_match_map",
        "team_badge",
        "team_provider_dim",
        "team_name_alias",
        "backfill_task",
        "backfill_day_log",
        "fixture_lineup",
        "fixture_lineup_player",
        "player_injury",
    }
    cur = conn.cursor()
    try:
        cur.execute("SHOW TABLES")
        existing = {r[0] for r in cur.fetchall()}
        missing = sorted(required_tables - existing)
        if missing:
            raise RuntimeError("Missing required tables after setup: " + ", ".join(missing))
    finally:
        cur.close()


def main():
    args = parse_args()
    schema_path = Path(__file__).resolve().parent / args.schema_file
    if not schema_path.exists():
        raise SystemExit(f"Schema file not found: {schema_path}")
    verify_schema_hash(schema_path, args.schema_sha256)

    conn = connect_db(args)
    try:
        run_schema_sql(conn, schema_path)
        run_migrations(conn)
        enforce_player_integrity(conn)
        validate_required_tables(conn)
        conn.commit()
        print("Schema setup/migration completed successfully.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
