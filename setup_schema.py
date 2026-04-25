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
