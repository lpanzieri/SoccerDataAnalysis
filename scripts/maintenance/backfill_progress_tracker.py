#!/usr/bin/env python3
"""Persistent progress tracker for multi-day backfill execution.

This tool does not make API calls. It tracks planned tasks and daily execution state
inside DB tables backfill_task and backfill_day_log.
"""

import argparse
import csv
import os
import sys
from typing import Optional

import mysql.connector


VALID_TASK_STATUS = {"pending", "in_progress", "completed", "skipped", "blocked"}
VALID_DAY_STATUS = {"pending", "in_progress", "completed", "blocked"}


def guard_unsafe_secret_flags() -> None:
    for arg in sys.argv[1:]:
        if arg == "--password" or arg.startswith("--password="):
            raise SystemExit("Unsafe secret flag detected. Use environment variable MYSQL_PASSWORD.")


def parse_args() -> argparse.Namespace:
    guard_unsafe_secret_flags()
    p = argparse.ArgumentParser(description="Track multi-day backfill progress")
    p.add_argument("--host", default=os.getenv("MYSQL_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.getenv("MYSQL_PORT", "3306")))
    p.add_argument("--user", default=os.getenv("MYSQL_USER", "football_admin"))
    p.add_argument("--database", default=os.getenv("MYSQL_DATABASE", "historic_football_data"))
    p.add_argument("--mysql-password-env", default="MYSQL_PASSWORD")

    sub = p.add_subparsers(dest="cmd", required=True)

    init_p = sub.add_parser("init", help="Initialize tracker from schedule CSV")
    init_p.add_argument("--csv", required=True, help="Schedule CSV path")

    status_p = sub.add_parser("status", help="Print progress summary")
    status_p.add_argument("--day", type=int, default=0, help="Optional day filter")

    mark_day_p = sub.add_parser("mark-day", help="Update day-level status")
    mark_day_p.add_argument("--day", type=int, required=True)
    mark_day_p.add_argument("--status", required=True, choices=sorted(VALID_DAY_STATUS))
    mark_day_p.add_argument("--actual-calls", type=int, default=-1)
    mark_day_p.add_argument("--api-remaining", type=int, default=-1)
    mark_day_p.add_argument("--notes", default="")

    mark_task_p = sub.add_parser("mark-task", help="Update a task status")
    mark_task_p.add_argument("--day", type=int, required=True)
    mark_task_p.add_argument("--item-type", required=True)
    mark_task_p.add_argument("--league-code", required=True)
    mark_task_p.add_argument("--start-year", type=int, required=True)
    mark_task_p.add_argument("--status", required=True, choices=sorted(VALID_TASK_STATUS))
    mark_task_p.add_argument("--notes", default="")

    auto_p = sub.add_parser("auto-mark-events", help="Auto-complete league_season tasks when events are fully polled")
    auto_p.add_argument("--day", type=int, default=0, help="Optional day filter")

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
        autocommit=False,
    )


def ensure_tables(conn) -> None:
    needed = {"backfill_task", "backfill_day_log", "event_fixture", "event_api_call_log"}
    cur = conn.cursor()
    try:
        cur.execute("SHOW TABLES")
        existing = {r[0] for r in cur.fetchall()}
        missing = sorted(needed - existing)
        if missing:
            raise RuntimeError("Missing required tables: " + ", ".join(missing) + ". Run setup_schema.py first.")
    finally:
        cur.close()


def to_int_or_none(value: str) -> Optional[int]:
    if value is None:
        return None
    s = str(value).strip()
    if s == "":
        return None
    return int(s)


def cmd_init(conn, csv_path: str) -> None:
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        raise RuntimeError("Schedule CSV is empty")

    cur = conn.cursor()
    try:
        inserted_tasks = 0
        for r in rows:
            day_no = int(r["day"])
            item_type = r["item_type"].strip()
            league_code = r["league_code"].strip()
            league_name = (r.get("league_name") or "").strip() or None
            api_league_id = to_int_or_none(r.get("api_league_id"))
            start_year = to_int_or_none(r.get("start_year"))
            estimated_calls = int(r.get("estimated_calls") or 0)
            notes = (r.get("notes") or "").strip() or None

            cur.execute(
                """
                INSERT INTO backfill_task (
                    day_no, item_type, league_code, league_name,
                    api_league_id, start_year, estimated_calls, notes
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                    league_name = VALUES(league_name),
                    api_league_id = VALUES(api_league_id),
                    estimated_calls = VALUES(estimated_calls),
                    notes = VALUES(notes)
                """,
                (
                    day_no,
                    item_type,
                    league_code,
                    league_name,
                    api_league_id,
                    start_year,
                    estimated_calls,
                    notes,
                ),
            )
            inserted_tasks += 1

        cur.execute("SELECT day_no, SUM(estimated_calls) FROM backfill_task GROUP BY day_no")
        for day_no, planned_calls in cur.fetchall():
            cur.execute(
                """
                INSERT INTO backfill_day_log (day_no, planned_calls)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE planned_calls = VALUES(planned_calls)
                """,
                (int(day_no), int(planned_calls or 0)),
            )

        conn.commit()
        print(f"Initialized/updated tasks from CSV: {inserted_tasks}")
        print("Day-level planned call totals refreshed.")
    finally:
        cur.close()


def cmd_status(conn, day: int) -> None:
    cur = conn.cursor()
    try:
        filter_sql = "WHERE day_no = %s" if day > 0 else ""
        params = (day,) if day > 0 else ()

        cur.execute(f"SELECT COUNT(*) FROM backfill_task {filter_sql}", params)
        total_tasks = int(cur.fetchone()[0])

        cur.execute(
            f"SELECT status, COUNT(*) FROM backfill_task {filter_sql} GROUP BY status ORDER BY status",
            params,
        )
        by_status = cur.fetchall()

        cur.execute(
            f"SELECT day_no, planned_calls, actual_calls, api_remaining, status FROM backfill_day_log {filter_sql} ORDER BY day_no",
            params,
        )
        day_rows = cur.fetchall()

        cur.execute(
            """
            SELECT COUNT(*)
            FROM event_api_call_log
            WHERE DATE(created_at) = CURRENT_DATE()
            """
        )
        calls_today = int(cur.fetchone()[0])

        cur.execute(
            """
            SELECT requests_remaining
            FROM event_api_call_log
            WHERE requests_remaining IS NOT NULL
            ORDER BY call_id DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
        last_remaining = int(row[0]) if row and row[0] is not None else None

        print(f"Tasks in scope: {total_tasks}")
        print("Task status breakdown:")
        for s, c in by_status:
            print(f"  {s}: {c}")

        print("Day log:")
        for day_no, planned, actual, remaining, status in day_rows:
            print(
                f"  day={day_no} planned={planned} actual={actual} "
                f"api_remaining={remaining} status={status}"
            )

        print(f"API call log entries today: {calls_today}")
        print(f"Latest provider requests_remaining seen: {last_remaining}")
    finally:
        cur.close()


def cmd_mark_day(conn, day: int, status: str, actual_calls: int, api_remaining: int, notes: str) -> None:
    cur = conn.cursor()
    try:
        set_started = "started_at = COALESCE(started_at, UTC_TIMESTAMP())," if status == "in_progress" else ""
        set_completed = "completed_at = UTC_TIMESTAMP()," if status == "completed" else ""

        updates = ["status = %s", "notes = %s"]
        values = [status, notes]

        if actual_calls >= 0:
            updates.append("actual_calls = %s")
            values.append(actual_calls)
        if api_remaining >= 0:
            updates.append("api_remaining = %s")
            values.append(api_remaining)

        sql = (
            "UPDATE backfill_day_log SET "
            + set_started
            + set_completed
            + ", ".join(updates)
            + " WHERE day_no = %s"
        )
        values.append(day)
        cur.execute(sql, tuple(values))
        if cur.rowcount == 0:
            raise RuntimeError(f"No day log row found for day {day}; run init first.")
        conn.commit()
        print(f"Updated day {day} status -> {status}")
    finally:
        cur.close()


def cmd_mark_task(
    conn,
    day: int,
    item_type: str,
    league_code: str,
    start_year: int,
    status: str,
    notes: str,
) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE backfill_task
            SET status = %s, notes = %s
            WHERE day_no = %s
              AND item_type = %s
              AND league_code = %s
              AND start_year = %s
            """,
            (status, notes, day, item_type, league_code, start_year),
        )
        if cur.rowcount == 0:
            raise RuntimeError("No matching task found to update.")
        conn.commit()
        print(
            "Updated task "
            f"day={day} type={item_type} league={league_code} year={start_year} -> {status}"
        )
    finally:
        cur.close()


def cmd_auto_mark_events(conn, day: int) -> None:
    cur = conn.cursor()
    try:
        where_day = "AND bt.day_no = %s" if day > 0 else ""
        params = (day,) if day > 0 else ()

        cur.execute(
            f"""
            SELECT bt.task_id, bt.api_league_id, bt.start_year
            FROM backfill_task bt
            WHERE bt.item_type = 'league_season'
              AND bt.status IN ('pending', 'in_progress')
              {where_day}
            """,
            params,
        )
        candidates = cur.fetchall()

        updated = 0
        for task_id, api_league_id, start_year in candidates:
            if api_league_id is None or start_year is None:
                continue
            cur.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN events_polled_at IS NOT NULL OR last_events_http_code = 200 THEN 1 ELSE 0 END) AS completed
                FROM event_fixture
                WHERE league_id = %s
                  AND season_year = %s
                """,
                (int(api_league_id), int(start_year)),
            )
            total, completed = cur.fetchone()
            total = int(total or 0)
            completed = int(completed or 0)
            if total > 0 and completed >= total:
                cur.execute(
                    "UPDATE backfill_task SET status = 'completed' WHERE task_id = %s",
                    (int(task_id),),
                )
                updated += 1

        conn.commit()
        print(f"Auto-mark completed league_season tasks: {updated}")
    finally:
        cur.close()


def main() -> None:
    args = parse_args()
    conn = connect_db(args)
    try:
        ensure_tables(conn)
        if args.cmd == "init":
            cmd_init(conn, args.csv)
        elif args.cmd == "status":
            cmd_status(conn, args.day)
        elif args.cmd == "mark-day":
            cmd_mark_day(conn, args.day, args.status, args.actual_calls, args.api_remaining, args.notes)
        elif args.cmd == "mark-task":
            cmd_mark_task(
                conn,
                args.day,
                args.item_type,
                args.league_code,
                args.start_year,
                args.status,
                args.notes,
            )
        elif args.cmd == "auto-mark-events":
            cmd_auto_mark_events(conn, args.day)
        else:
            raise RuntimeError("Unsupported command")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
