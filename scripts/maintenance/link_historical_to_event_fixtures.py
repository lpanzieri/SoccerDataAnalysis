#!/usr/bin/env python3
"""Link API fixtures to historical CSV matches.

This script populates event_fixture_match_map with best-effort matches using:
- date proximity (exact date preferred, +/-1 day fallback)
- home/away team name similarity
- league name similarity

It prints textual progress logs and a final coverage summary.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import re
import unicodedata
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Dict, List, Optional, Tuple

import mysql.connector


@dataclass
class FixtureRow:
    provider_fixture_id: int
    league_name: Optional[str]
    season_year: int
    fixture_date_utc: Optional[dt.datetime]
    home_team_name: Optional[str]
    away_team_name: Optional[str]


@dataclass
class CandidateRow:
    match_id: int
    match_date: Optional[dt.date]
    league_name: str
    season_start_year: int
    home_team_name: str
    away_team_name: str


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Link historical matches to API fixtures")
    p.add_argument("--host", default=os.getenv("MYSQL_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.getenv("MYSQL_PORT", "3306")))
    p.add_argument("--user", default=os.getenv("MYSQL_USER", "football_admin"))
    p.add_argument("--database", default=os.getenv("MYSQL_DATABASE", "historic_football_data"))
    p.add_argument("--mysql-password-env", default="MYSQL_PASSWORD")

    p.add_argument("--min-season-year", type=int, default=2008)
    p.add_argument("--max-season-year", type=int, default=2025)
    p.add_argument("--only-unmapped", action="store_true", default=True)
    p.add_argument("--include-mapped", action="store_true", help="Process fixtures already in mapping table")
    p.add_argument("--limit", type=int, default=0, help="Process at most N fixtures (0 = all)")
    p.add_argument("--progress-every", type=int, default=200)
    p.add_argument("--min-confidence", type=float, default=72.0)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--log-file", default="", help="Optional plain-text log file path")
    return p.parse_args()


def emit(log_lines: List[str], message: str) -> None:
    print(message)
    log_lines.append(message)


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


def normalize_text(text: Optional[str]) -> str:
    if not text:
        return ""
    value = unicodedata.normalize("NFKD", text)
    value = value.encode("ascii", "ignore").decode("ascii")
    value = value.lower().strip()
    value = re.sub(r"\b(fc|ac|cf|club|de|the)\b", " ", value)
    value = re.sub(r"[^a-z0-9]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def similarity(a: str, b: str) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    return SequenceMatcher(None, a, b).ratio()


def fetch_fixtures(conn, args: argparse.Namespace) -> List[FixtureRow]:
    where = ["ef.season_year BETWEEN %s AND %s"]
    params: List[object] = [args.min_season_year, args.max_season_year]

    if not args.include_mapped:
        where.append("m.provider_fixture_id IS NULL")

    limit_clause = ""
    if args.limit and args.limit > 0:
        limit_clause = "LIMIT %s"
        params.append(args.limit)

    sql = f"""
        SELECT
            ef.provider_fixture_id,
            ef.league_name,
            ef.season_year,
            ef.fixture_date_utc,
            ef.home_team_name,
            ef.away_team_name
        FROM event_fixture ef
        LEFT JOIN event_fixture_match_map m ON m.provider_fixture_id = ef.provider_fixture_id
        WHERE {' AND '.join(where)}
        ORDER BY ef.season_year DESC, ef.fixture_date_utc ASC, ef.provider_fixture_id ASC
        {limit_clause}
    """

    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        out: List[FixtureRow] = []
        for row in cur.fetchall():
            out.append(
                FixtureRow(
                    provider_fixture_id=int(row[0]),
                    league_name=row[1],
                    season_year=int(row[2]),
                    fixture_date_utc=row[3],
                    home_team_name=row[4],
                    away_team_name=row[5],
                )
            )
        return out
    finally:
        cur.close()


def fetch_candidates(conn, season_year: int, from_date: dt.date, to_date: dt.date) -> List[CandidateRow]:
    sql = """
        SELECT
            mg.match_id,
            mg.match_date,
            l.league_name,
            s.start_year,
            ht.team_name AS home_team_name,
            at.team_name AS away_team_name
        FROM match_game mg
        JOIN season s ON s.season_id = mg.season_id
        JOIN league l ON l.league_id = mg.league_id
        JOIN team ht ON ht.team_id = mg.home_team_id
        JOIN team at ON at.team_id = mg.away_team_id
        WHERE s.start_year = %s
          AND mg.match_date BETWEEN %s AND %s
    """

    cur = conn.cursor()
    try:
        cur.execute(sql, (season_year, from_date, to_date))
        rows = []
        for row in cur.fetchall():
            rows.append(
                CandidateRow(
                    match_id=int(row[0]),
                    match_date=row[1],
                    league_name=str(row[2]),
                    season_start_year=int(row[3]),
                    home_team_name=str(row[4]),
                    away_team_name=str(row[5]),
                )
            )
        return rows
    finally:
        cur.close()


def score_candidate(fixture: FixtureRow, cand: CandidateRow) -> Tuple[float, str]:
    f_home = normalize_text(fixture.home_team_name)
    f_away = normalize_text(fixture.away_team_name)
    c_home = normalize_text(cand.home_team_name)
    c_away = normalize_text(cand.away_team_name)
    f_league = normalize_text(fixture.league_name)
    c_league = normalize_text(cand.league_name)

    date_score = 0.0
    if fixture.fixture_date_utc and cand.match_date:
        delta_days = abs((fixture.fixture_date_utc.date() - cand.match_date).days)
        if delta_days == 0:
            date_score = 35.0
        elif delta_days == 1:
            date_score = 22.0
        else:
            date_score = max(0.0, 10.0 - (delta_days * 3.0))

    home_sim = similarity(f_home, c_home)
    away_sim = similarity(f_away, c_away)
    league_sim = similarity(f_league, c_league)

    team_score = 35.0 * home_sim + 35.0 * away_sim
    league_score = 10.0 * league_sim

    score = date_score + team_score + league_score

    if home_sim == 1.0 and away_sim == 1.0 and date_score >= 22.0:
        method = "exact_teams_date"
    elif home_sim >= 0.9 and away_sim >= 0.9 and date_score >= 22.0:
        method = "strong_teams_date"
    elif home_sim >= 0.82 and away_sim >= 0.82:
        method = "fuzzy_teams_date"
    else:
        method = "weak_candidate"

    return score, method


def choose_best_candidate(fixture: FixtureRow, candidates: List[CandidateRow], min_confidence: float):
    scored: List[Tuple[float, str, CandidateRow]] = []
    for cand in candidates:
        score, method = score_candidate(fixture, cand)
        scored.append((score, method, cand))

    if not scored:
        return None, "no_candidates"

    scored.sort(key=lambda x: x[0], reverse=True)
    best_score, best_method, best = scored[0]

    if best_score < min_confidence:
        return None, "below_confidence"

    if len(scored) > 1 and (best_score - scored[1][0]) < 1.5:
        return None, "ambiguous_top2"

    return (best, best_score, best_method), "ok"


def upsert_mapping(
    conn,
    provider_fixture_id: int,
    match_id: int,
    score: float,
    method: str,
    notes: str,
):
    sql = """
        INSERT INTO event_fixture_match_map
            (provider_fixture_id, match_id, confidence_score, match_method, notes, linked_at)
        VALUES
            (%s, %s, %s, %s, %s, NOW())
        ON DUPLICATE KEY UPDATE
            match_id = VALUES(match_id),
            confidence_score = VALUES(confidence_score),
            match_method = VALUES(match_method),
            notes = VALUES(notes),
            linked_at = NOW()
    """
    cur = conn.cursor()
    try:
        cur.execute(sql, (provider_fixture_id, match_id, round(score, 2), method, notes))
    finally:
        cur.close()


def main() -> None:
    args = parse_args()
    if args.include_mapped:
        args.only_unmapped = False

    conn = connect_db(args)
    log_lines: List[str] = []
    try:
        fixtures = fetch_fixtures(conn, args)
        total = len(fixtures)
        emit(log_lines, "Starting historical-to-API fixture linker")
        emit(
            log_lines,
            f"Scope: season_year={args.min_season_year}-{args.max_season_year}, "
            f"fixtures={total}, min_confidence={args.min_confidence}, dry_run={args.dry_run}"
        )

        if total == 0:
            emit(log_lines, "No fixtures selected. Nothing to do.")
            if args.log_file:
                with open(args.log_file, "w", encoding="utf-8") as f:
                    f.write("\n".join(log_lines) + "\n")
                print(f"Log file written: {args.log_file}")
            return

        cache: Dict[Tuple[int, dt.date, dt.date], List[CandidateRow]] = {}
        mapped = 0
        skipped = 0
        reason_counts: Dict[str, int] = {}

        for idx, fixture in enumerate(fixtures, start=1):
            if fixture.fixture_date_utc is None:
                skipped += 1
                reason_counts["missing_fixture_date"] = reason_counts.get("missing_fixture_date", 0) + 1
                continue

            from_date = fixture.fixture_date_utc.date() - dt.timedelta(days=1)
            to_date = fixture.fixture_date_utc.date() + dt.timedelta(days=1)
            key = (fixture.season_year, from_date, to_date)

            if key not in cache:
                cache[key] = fetch_candidates(conn, fixture.season_year, from_date, to_date)

            selected, reason = choose_best_candidate(fixture, cache[key], args.min_confidence)
            if selected is None:
                skipped += 1
                reason_counts[reason] = reason_counts.get(reason, 0) + 1
            else:
                best, score, method = selected
                notes = (
                    f"fixture_date={fixture.fixture_date_utc.date()} "
                    f"hist_date={best.match_date} "
                    f"fixture_teams={fixture.home_team_name} vs {fixture.away_team_name} "
                    f"hist_teams={best.home_team_name} vs {best.away_team_name}"
                )
                if not args.dry_run:
                    upsert_mapping(
                        conn,
                        provider_fixture_id=fixture.provider_fixture_id,
                        match_id=best.match_id,
                        score=score,
                        method=method,
                        notes=notes[:255],
                    )
                mapped += 1

            if idx % args.progress_every == 0 or idx == total:
                pct = (idx / total) * 100.0
                emit(
                    log_lines,
                    f"Progress {idx}/{total} ({pct:.1f}%) | mapped={mapped} skipped={skipped} "
                    f"cache_windows={len(cache)}"
                )

        if not args.dry_run:
            conn.commit()

        emit(log_lines, "Linking finished")
        emit(log_lines, f"Mapped: {mapped}")
        emit(log_lines, f"Skipped: {skipped}")
        if total > 0:
            emit(log_lines, f"Coverage: {mapped / total * 100.0:.2f}%")

        if reason_counts:
            emit(log_lines, "Skip reasons:")
            for key_name in sorted(reason_counts):
                emit(log_lines, f"  - {key_name}: {reason_counts[key_name]}")

        # Show global mapped coverage after this run.
        cur = conn.cursor()
        try:
            cur.execute("SELECT COUNT(*) FROM event_fixture")
            total_fixtures = int(cur.fetchone()[0])
            cur.execute("SELECT COUNT(*) FROM event_fixture_match_map WHERE match_id IS NOT NULL")
            linked = int(cur.fetchone()[0])
            emit(
                log_lines,
                f"DB mapping coverage: {linked}/{total_fixtures} "
                f"({(linked/total_fixtures*100.0 if total_fixtures else 0):.2f}%)",
            )
        finally:
            cur.close()

        if args.log_file:
            with open(args.log_file, "w", encoding="utf-8") as f:
                f.write("\n".join(log_lines) + "\n")
            print(f"Log file written: {args.log_file}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
