#!/usr/bin/env python3
"""Backfill player_dim and player_team_history from existing event_timeline rows."""

import argparse
import os
import sys

import mysql.connector


def guard_unsafe_secret_flags():
    for arg in sys.argv[1:]:
        if arg == "--password" or arg.startswith("--password="):
            raise SystemExit("Unsafe secret flag detected. Use environment variable MYSQL_PASSWORD.")


def parse_args():
    guard_unsafe_secret_flags()
    p = argparse.ArgumentParser(description="Backfill player data from event_timeline")
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
    required = {
        "event_timeline",
        "event_fixture",
        "player_dim",
        "player_team_history",
        "player_name_alias",
    }
    cur = conn.cursor()
    try:
        cur.execute("SHOW TABLES")
        existing = {r[0] for r in cur.fetchall()}
        missing = sorted(required - existing)
        if missing:
            raise RuntimeError("Missing required tables: " + ", ".join(missing))
    finally:
        cur.close()


def run_backfill(conn):
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO player_name_alias (provider_player_id, alias_name, source_table)
            SELECT et.player_id, et.player_name, 'event_player'
            FROM event_timeline et
            WHERE et.player_id IS NOT NULL
              AND et.player_name IS NOT NULL
              AND et.player_name <> ''
            ON DUPLICATE KEY UPDATE last_seen_at = CURRENT_TIMESTAMP
            """
        )

        cur.execute(
            """
            INSERT INTO player_name_alias (provider_player_id, alias_name, source_table)
            SELECT et.assist_id, et.assist_name, 'event_assist'
            FROM event_timeline et
            WHERE et.assist_id IS NOT NULL
              AND et.assist_name IS NOT NULL
              AND et.assist_name <> ''
            ON DUPLICATE KEY UPDATE last_seen_at = CURRENT_TIMESTAMP
            """
        )

        cur.execute(
            """
            INSERT INTO player_dim (provider_player_id, player_name)
            SELECT
                a.provider_player_id,
                SUBSTRING_INDEX(
                    GROUP_CONCAT(a.alias_name ORDER BY
                        CASE WHEN a.alias_name LIKE '%%.%%' THEN 1 ELSE 0 END ASC,
                        CASE WHEN a.alias_name LIKE '%% %%' THEN 0 ELSE 1 END ASC,
                        CHAR_LENGTH(a.alias_name) DESC,
                        a.last_seen_at DESC
                        SEPARATOR '\\n'
                    ),
                    '\\n',
                    1
                ) AS canonical_name
            FROM player_name_alias a
            GROUP BY a.provider_player_id
            ON DUPLICATE KEY UPDATE
                player_name = VALUES(player_name)
            """
        )

        cur.execute(
            """
            INSERT INTO player_team_history (
                provider_player_id,
                provider_team_id,
                first_seen_at,
                last_seen_at,
                observations
            )
            SELECT
                x.player_id,
                x.team_id,
                MIN(x.fixture_date_utc) AS first_seen_at,
                MAX(x.fixture_date_utc) AS last_seen_at,
                COUNT(*) AS observations
            FROM (
                SELECT et.player_id AS player_id, et.team_id AS team_id, ef.fixture_date_utc
                FROM event_timeline et
                JOIN event_fixture ef ON ef.provider_fixture_id = et.provider_fixture_id
                WHERE et.player_id IS NOT NULL
                  AND et.team_id IS NOT NULL
                  AND ef.fixture_date_utc IS NOT NULL
                UNION ALL
                SELECT et.assist_id AS player_id, et.team_id AS team_id, ef.fixture_date_utc
                FROM event_timeline et
                JOIN event_fixture ef ON ef.provider_fixture_id = et.provider_fixture_id
                WHERE et.assist_id IS NOT NULL
                  AND et.team_id IS NOT NULL
                  AND ef.fixture_date_utc IS NOT NULL
            ) x
            GROUP BY x.player_id, x.team_id
            ON DUPLICATE KEY UPDATE
                first_seen_at = LEAST(first_seen_at, VALUES(first_seen_at)),
                last_seen_at = GREATEST(last_seen_at, VALUES(last_seen_at)),
                observations = GREATEST(observations, VALUES(observations))
            """
        )
    finally:
        cur.close()


def print_stats(conn):
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM player_dim")
        players = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM player_team_history")
        memberships = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM player_name_alias")
        aliases = cur.fetchone()[0]

        cur.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT provider_player_id
                FROM player_team_history
                GROUP BY provider_player_id
                HAVING COUNT(DISTINCT provider_team_id) > 1
            ) x
            """
        )
        multi_team_players = cur.fetchone()[0]

        print(f"player_dim rows: {players}")
        print(f"player_team_history rows: {memberships}")
        print(f"player_name_alias rows: {aliases}")
        print(f"players with >1 teams observed: {multi_team_players}")

        cur.execute(
            """
            SELECT provider_player_id, COUNT(DISTINCT alias_name) AS variants,
                   GROUP_CONCAT(DISTINCT alias_name ORDER BY alias_name SEPARATOR ' | ') AS aliases
            FROM player_name_alias
            GROUP BY provider_player_id
            HAVING COUNT(DISTINCT alias_name) > 1
            ORDER BY variants DESC, provider_player_id
            LIMIT 10
            """
        )
        rows = cur.fetchall()
        if rows:
            print("Top players with alias variants:")
            for provider_player_id, variants, alias_names in rows:
                print(f"  {provider_player_id}: variants={variants} aliases={alias_names}")
    finally:
        cur.close()


def main():
    args = parse_args()
    conn = connect_db(args)
    try:
        ensure_tables(conn)
        run_backfill(conn)
        conn.commit()
        print("Player data backfill completed successfully.")
        print_stats(conn)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
