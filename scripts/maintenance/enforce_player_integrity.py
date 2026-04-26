#!/usr/bin/env python3
"""Backfill missing player_dim rows and enforce player foreign keys.

This migration is idempotent and safe to rerun.
"""

from __future__ import annotations

import argparse
import os

import mysql.connector


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Enforce player foreign keys for lineup/injury tables")
    parser.add_argument("--host", default=os.getenv("MYSQL_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("MYSQL_PORT", "3306")))
    parser.add_argument("--user", default=os.getenv("MYSQL_USER", "football_admin"))
    parser.add_argument("--database", default=os.getenv("MYSQL_DATABASE", "historic_football_data"))
    parser.add_argument("--mysql-password-env", default="MYSQL_PASSWORD")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned changes without applying them",
    )
    return parser.parse_args()


def has_constraint(cur: mysql.connector.cursor.MySQLCursor, table_name: str, constraint_name: str) -> bool:
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


def count_orphans(cur: mysql.connector.cursor.MySQLCursor, table_name: str, column_name: str) -> int:
    cur.execute(
        f"""
        SELECT COUNT(*)
        FROM {table_name} t
        LEFT JOIN player_dim pd ON pd.provider_player_id = t.{column_name}
        WHERE pd.provider_player_id IS NULL
        """
    )
    row = cur.fetchone()
    return int(row[0] if row else 0)


def main() -> None:
    args = parse_args()
    conn = mysql.connector.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=os.getenv(args.mysql_password_env, ""),
        database=args.database,
        charset="utf8mb4",
        use_unicode=True,
        use_pure=True,
        autocommit=False,
    )

    try:
        cur = conn.cursor()

        injury_orphans_before = count_orphans(cur, "player_injury", "provider_player_id")
        lineup_orphans_before = count_orphans(cur, "fixture_lineup_player", "player_id")
        print(f"orphan_injury_before={injury_orphans_before}")
        print(f"orphan_lineup_before={lineup_orphans_before}")

        if not args.dry_run:
            # Backfill missing players from injury data.
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
            print(f"player_dim_backfilled_from_injury={cur.rowcount}")

            # Backfill missing players from lineup data.
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
            print(f"player_dim_backfilled_from_lineup={cur.rowcount}")

        injury_orphans_after = count_orphans(cur, "player_injury", "provider_player_id")
        lineup_orphans_after = count_orphans(cur, "fixture_lineup_player", "player_id")
        print(f"orphan_injury_after={injury_orphans_after}")
        print(f"orphan_lineup_after={lineup_orphans_after}")

        if args.dry_run:
            print("dry_run=true; skipping constraint creation")
            conn.rollback()
            return

        if injury_orphans_after > 0 or lineup_orphans_after > 0:
            raise RuntimeError("Orphans remain after backfill; refusing to add foreign keys")

        if not has_constraint(cur, "player_injury", "fk_player_injury_player"):
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
            print("added_fk=fk_player_injury_player")
        else:
            print("existing_fk=fk_player_injury_player")

        if not has_constraint(cur, "fixture_lineup_player", "fk_lineup_player_player"):
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
            print("added_fk=fk_lineup_player_player")
        else:
            print("existing_fk=fk_lineup_player_player")

        conn.commit()
        print("status=ok")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
