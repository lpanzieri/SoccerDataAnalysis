#!/usr/bin/env python3
"""Backfill and refresh canonical team naming across provider/event tables."""

import argparse
import os
import sys

import mysql.connector


def guard_unsafe_secret_flags():
    blocked = ("--password",)
    for arg in sys.argv[1:]:
        if any(arg == flag or arg.startswith(flag + "=") for flag in blocked):
            raise SystemExit(
                "Unsafe secret flags detected. Use environment variable MYSQL_PASSWORD."
            )


def parse_args():
    guard_unsafe_secret_flags()
    p = argparse.ArgumentParser(description="Refresh canonical team naming tables")
    p.add_argument("--host", default=os.getenv("MYSQL_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.getenv("MYSQL_PORT", "3306")))
    p.add_argument("--user", default=os.getenv("MYSQL_USER", "football_admin"))
    p.add_argument("--database", default=os.getenv("MYSQL_DATABASE", "historic_football_data"))
    p.add_argument("--mysql-password-env", default="MYSQL_PASSWORD")
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


def ensure_tables(conn):
    required = {"team_name_alias", "team_provider_dim", "event_fixture", "event_timeline"}
    cur = conn.cursor()
    try:
        cur.execute("SHOW TABLES")
        existing = {r[0] for r in cur.fetchall()}
        missing = sorted(required - existing)
        if missing:
            raise RuntimeError(
                "Missing required tables: "
                + ", ".join(missing)
                + ". Run setup_schema.py first."
            )
    finally:
        cur.close()


def insert_aliases(conn):
    stmts = [
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

    cur = conn.cursor()
    try:
        for stmt in stmts:
            cur.execute(stmt)
    finally:
        cur.close()


def upsert_canonical_names(conn):
    cur = conn.cursor()
    try:
        # Prefer badge name first when available.
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

        # Fill the rest from most recently seen alias.
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

        # Map canonical provider names back to legacy normalized team IDs when exact match exists.
        cur.execute(
            """
            UPDATE team_provider_dim d
            LEFT JOIN team t ON t.team_name = d.canonical_team_name
            SET d.canonical_team_id = t.team_id
            """
        )
    finally:
        cur.close()


def print_stats(conn):
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM team_name_alias")
        alias_count = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM team_provider_dim")
        dim_count = cur.fetchone()[0]

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
        providers_with_variants = cur.fetchone()[0]

        cur.execute(
            """
            SELECT COUNT(*)
            FROM event_timeline et
            LEFT JOIN team_provider_dim d ON d.provider_team_id = et.team_id
            WHERE et.team_id IS NOT NULL
              AND d.provider_team_id IS NULL
            """
        )
        timeline_unmapped = cur.fetchone()[0]

        print(f"team_name_alias rows: {alias_count}")
        print(f"team_provider_dim rows: {dim_count}")
        print(f"providers with >1 alias: {providers_with_variants}")
        print(f"event_timeline rows with team_id but no canonical map: {timeline_unmapped}")

        cur.execute(
            """
            SELECT provider_team_id, COUNT(DISTINCT alias_name) AS variants,
                   GROUP_CONCAT(DISTINCT alias_name ORDER BY alias_name SEPARATOR ' | ') AS aliases
            FROM team_name_alias
            GROUP BY provider_team_id
            HAVING COUNT(DISTINCT alias_name) > 1
            ORDER BY variants DESC, provider_team_id
            LIMIT 15
            """
        )
        rows = cur.fetchall()
        if rows:
            print("Top provider IDs with alias variants:")
            for provider_team_id, variants, aliases in rows:
                print(f"  {provider_team_id}: variants={variants} aliases={aliases}")
    finally:
        cur.close()


def main():
    args = parse_args()
    conn = connect_db(args)
    try:
        ensure_tables(conn)
        insert_aliases(conn)
        upsert_canonical_names(conn)
        conn.commit()
        print("Team name normalization refresh completed successfully.")
        print_stats(conn)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
