#!/usr/bin/env python3
"""Benchmark dynamic helper latency and DB execute call counts.

Usage example:
  MYSQL_PASSWORD='***' python3 scripts/maintenance/benchmark_helpers.py \
    --question "graph of the goals scored by inter, milan, juventus and napoli in the last 10 years" \
    --runs 20 --warmups 3 --user root
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import statistics
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

import mysql.connector.cursor as mysql_cursor

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.helpers.dynamic_helper_manager import answer_question_with_helpers
from scripts.helpers.league_records import DBConfig


@dataclass
class RunStat:
    index: int
    duration_ms: float
    execute_calls: int
    cache_hit: bool
    intent: str
    image: bool
    base64_image: bool


def _percentile(values: List[float], pct: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    sorted_vals = sorted(values)
    rank = (pct / 100.0) * (len(sorted_vals) - 1)
    low = int(rank)
    high = min(low + 1, len(sorted_vals) - 1)
    weight = rank - low
    return sorted_vals[low] * (1.0 - weight) + sorted_vals[high] * weight


@contextmanager
def patched_execute_counter() -> Any:
    execute_counter = {"count": 0}
    patched_targets = []

    # Patch whichever cursor classes are present in this mysql connector build.
    candidate_class_names = [
        "MySQLCursor",
        "MySQLCursorDict",
        "CMySQLCursor",
        "CMySQLCursorDict",
    ]

    for class_name in candidate_class_names:
        cls = getattr(mysql_cursor, class_name, None)
        if cls is None:
            continue
        original = getattr(cls, "execute", None)
        if original is None:
            continue

        def make_wrapper(orig):
            def wrapper(self, operation, params=None, map_results=False):
                execute_counter["count"] += 1
                return orig(self, operation, params, map_results)

            return wrapper

        setattr(cls, "execute", make_wrapper(original))
        patched_targets.append((cls, original))

    try:
        yield execute_counter
    finally:
        for cls, original in patched_targets:
            setattr(cls, "execute", original)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark dynamic helper performance")
    parser.add_argument("--question", required=True, help="Question to benchmark")
    parser.add_argument("--runs", type=int, default=20)
    parser.add_argument("--warmups", type=int, default=3)
    parser.add_argument("--host", default=os.getenv("MYSQL_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("MYSQL_PORT", "3306")))
    parser.add_argument("--user", default=os.getenv("MYSQL_USER", "football_admin"))
    parser.add_argument("--database", default=os.getenv("MYSQL_DATABASE", "historic_football_data"))
    parser.add_argument("--mysql-password-env", default="MYSQL_PASSWORD")
    parser.add_argument("--cache", action="store_true", help="Enable helper response cache")
    parser.add_argument("--cache-ttl-seconds", type=int, default=None)
    parser.add_argument(
        "--output-dir",
        default="benchmarks",
        help="Directory where benchmark JSON report is written",
    )
    return parser.parse_args()


def run_once(question: str, db: DBConfig, use_cache: bool, cache_ttl_seconds: int | None, idx: int) -> RunStat:
    with patched_execute_counter() as counter:
        t0 = time.perf_counter()
        result = answer_question_with_helpers(
            question=question,
            db=db,
            use_cache=use_cache,
            cache_ttl_seconds=cache_ttl_seconds,
        )
        t1 = time.perf_counter()

    cache_info = result.get("cache", {}) if isinstance(result, dict) else {}
    return RunStat(
        index=idx,
        duration_ms=(t1 - t0) * 1000.0,
        execute_calls=int(counter["count"]),
        cache_hit=bool(cache_info.get("hit", False)),
        intent=str(result.get("intent", "")) if isinstance(result, dict) else "",
        image=bool(result.get("image")) if isinstance(result, dict) else False,
        base64_image=bool(result.get("base64_image")) if isinstance(result, dict) else False,
    )


def summarize(stats: List[RunStat]) -> Dict[str, Any]:
    durations = [s.duration_ms for s in stats]
    executes = [s.execute_calls for s in stats]
    cache_hits = sum(1 for s in stats if s.cache_hit)

    return {
        "run_count": len(stats),
        "latency_ms": {
            "min": min(durations) if durations else 0.0,
            "max": max(durations) if durations else 0.0,
            "avg": statistics.mean(durations) if durations else 0.0,
            "p50": _percentile(durations, 50),
            "p95": _percentile(durations, 95),
        },
        "db_execute_calls": {
            "total": sum(executes),
            "avg_per_run": statistics.mean(executes) if executes else 0.0,
            "min": min(executes) if executes else 0,
            "max": max(executes) if executes else 0,
        },
        "cache": {
            "hits": cache_hits,
            "hit_rate": (cache_hits / len(stats)) if stats else 0.0,
        },
    }


def main() -> None:
    args = parse_args()
    password = os.getenv(args.mysql_password_env, "")

    db = DBConfig(
        host=args.host,
        port=args.port,
        user=args.user,
        password=password,
        database=args.database,
    )

    # Warmup runs (not included in aggregate metrics).
    for i in range(args.warmups):
        _ = run_once(args.question, db, args.cache, args.cache_ttl_seconds, -(i + 1))

    stats: List[RunStat] = []
    for i in range(args.runs):
        stats.append(run_once(args.question, db, args.cache, args.cache_ttl_seconds, i + 1))

    summary = summarize(stats)

    report = {
        "question": args.question,
        "db": {
            "host": args.host,
            "port": args.port,
            "user": args.user,
            "database": args.database,
        },
        "options": {
            "runs": args.runs,
            "warmups": args.warmups,
            "cache": args.cache,
            "cache_ttl_seconds": args.cache_ttl_seconds,
        },
        "summary": summary,
        "runs": [
            {
                "index": s.index,
                "duration_ms": round(s.duration_ms, 3),
                "execute_calls": s.execute_calls,
                "cache_hit": s.cache_hit,
                "intent": s.intent,
                "image": s.image,
                "base64_image": s.base64_image,
            }
            for s in stats
        ],
    }

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S", time.localtime())
    out_path = out_dir / f"helper_benchmark_{ts}.json"
    out_path.write_text(json.dumps(report, indent=2, ensure_ascii=True), encoding="utf-8")

    print("Benchmark complete")
    print("report:", out_path)
    print("summary:", json.dumps(summary, indent=2, ensure_ascii=True))


if __name__ == "__main__":
    main()
