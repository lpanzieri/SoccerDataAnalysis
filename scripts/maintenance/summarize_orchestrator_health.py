#!/usr/bin/env python3
"""Summarize orchestrator health for queue execution.

Provides textual summaries for:
- task status counts
- blocked reasons from backfill_task.notes
- per-day task status counts
- API call response code counts for a target date
- recent blocked task examples

Optional: also parse worker error log for reason frequencies.
"""

from __future__ import annotations

import argparse
import collections
import datetime as dt
import os
import re
from pathlib import Path
from typing import Dict, List, Optional

import mysql.connector


RE_REASON = re.compile(r"reason=([a-zA-Z0-9_\-]+)")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Summarize orchestrator health")
    p.add_argument("--host", default=os.getenv("MYSQL_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.getenv("MYSQL_PORT", "3306")))
    p.add_argument("--user", default=os.getenv("MYSQL_USER", "football_admin"))
    p.add_argument("--database", default=os.getenv("MYSQL_DATABASE", "historic_football_data"))
    p.add_argument("--mysql-password-env", default="MYSQL_PASSWORD")

    p.add_argument(
        "--date",
        default=dt.date.today().isoformat(),
        help="Target UTC date in YYYY-MM-DD for API/day snapshots",
    )
    p.add_argument(
        "--error-log",
        default="",
        help="Optional worker error log path (defaults to logs/worker_errors_<date>.log if exists)",
    )
    p.add_argument("--max-examples", type=int, default=8)
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


def classify_block_reason(notes: Optional[str]) -> str:
    if not notes:
        return "blocked_unspecified"

    m = RE_REASON.search(notes)
    if m:
        return m.group(1)

    text = notes.lower()
    if "missing api_league_id" in text or "missing api mapping" in text:
        return "missing_api_mapping_or_year"
    if "season" in text and "not" in text and "available" in text:
        return "season_not_available_or_invalid"
    if "rate" in text and "limit" in text:
        return "api_rate_limit_reached"
    if "plan" in text and "restriction" in text:
        return "api_plan_restriction"
    if "sync failed" in text:
        return "sync_failed_unknown"
    if "linker failed" in text:
        return "linker_failed"
    return "blocked_other"


def summarize_error_log(path: Path) -> Dict[str, int]:
    out: Dict[str, int] = collections.Counter()
    if not path.exists():
        return out
    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            m = RE_REASON.search(line)
            if m:
                out[m.group(1)] += 1
    return dict(out)


def main() -> None:
    args = parse_args()
    try:
        target_date = dt.date.fromisoformat(args.date)
    except ValueError as exc:
        raise SystemExit(f"Invalid --date {args.date!r}: {exc}")

    conn = connect_db(args)
    cur = conn.cursor()
    try:
        print("Orchestrator Health Summary")
        print(f"Date: {target_date.isoformat()}")
        print()

        cur.execute("SELECT status, COUNT(*) FROM backfill_task GROUP BY status ORDER BY status")
        status_rows = cur.fetchall()
        print("Task status counts:")
        if not status_rows:
            print("  (no tasks)")
        for status, count in status_rows:
            print(f"  {status}: {count}")
        print()

        cur.execute(
            """
            SELECT day_no, status, COUNT(*)
            FROM backfill_task
            GROUP BY day_no, status
            ORDER BY day_no, status
            """
        )
        per_day = cur.fetchall()
        print("Per-day status counts:")
        if not per_day:
            print("  (no task rows)")
        else:
            current_day = None
            for day_no, status, count in per_day:
                if day_no != current_day:
                    current_day = day_no
                    print(f"  day {day_no}:")
                print(f"    {status}: {count}")
        print()

        cur.execute("SELECT notes FROM backfill_task WHERE status='blocked'")
        blocked_notes = [r[0] for r in cur.fetchall()]
        blocked_by_reason: Dict[str, int] = collections.Counter(
            classify_block_reason(n) for n in blocked_notes
        )

        print("Blocked reasons (from backfill_task.notes):")
        if not blocked_by_reason:
            print("  (no blocked tasks)")
        else:
            for reason, count in sorted(blocked_by_reason.items(), key=lambda x: (-x[1], x[0])):
                print(f"  {reason}: {count}")
        print()

        cur.execute(
            """
            SELECT response_code, COUNT(*)
            FROM event_api_call_log
            WHERE DATE(created_at) = %s
            GROUP BY response_code
            ORDER BY response_code
            """,
            (target_date,),
        )
        api_codes = cur.fetchall()
        cur.execute(
            """
            SELECT COUNT(*), MIN(created_at), MAX(created_at)
            FROM event_api_call_log
            WHERE DATE(created_at) = %s
            """,
            (target_date,),
        )
        total_calls, first_call, last_call = cur.fetchone()

        print("API call log for date:")
        print(f"  total calls: {int(total_calls or 0)}")
        print(f"  first call: {first_call}")
        print(f"  last call: {last_call}")
        if api_codes:
            print("  by HTTP code:")
            for code, count in api_codes:
                print(f"    {code}: {count}")
        print()

        cur.execute(
            """
            SELECT task_id, day_no, league_code, start_year, notes, updated_at
            FROM backfill_task
            WHERE status='blocked'
            ORDER BY updated_at DESC
            LIMIT %s
            """,
            (args.max_examples,),
        )
        examples = cur.fetchall()
        print("Recent blocked task examples:")
        if not examples:
            print("  (none)")
        else:
            for task_id, day_no, league_code, start_year, notes, updated_at in examples:
                reason = classify_block_reason(notes)
                print(
                    f"  task_id={task_id} day={day_no} league={league_code} year={start_year} "
                    f"reason={reason} updated_at={updated_at}"
                )
                if notes:
                    print(f"    notes={notes}")
        print()

        if args.error_log:
            error_log = Path(args.error_log)
        else:
            error_log = Path(f"logs/worker_errors_{target_date.isoformat()}.log")

        log_summary = summarize_error_log(error_log)
        print(f"Error log reason counts ({error_log}):")
        if not error_log.exists():
            print("  (file not found)")
        elif not log_summary:
            print("  (no reason=... lines found)")
        else:
            for reason, count in sorted(log_summary.items(), key=lambda x: (-x[1], x[0])):
                print(f"  {reason}: {count}")

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
