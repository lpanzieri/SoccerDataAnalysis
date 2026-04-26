#!/usr/bin/env python3
"""Walk historical local fixtures and compare reusable predictions to actual results.

This script only uses local data already stored in the database.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

import mysql.connector

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.helpers.league_records import DBConfig, predict_match_outcome


FINISHED_STATUSES = ("FT", "AET", "PEN", "FT_PEN", "AWD", "WO")
FINISHED_STATUS_SQL = ", ".join(f"'{status}'" for status in FINISHED_STATUSES)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate local match predictions against historical results")
    parser.add_argument("--league-code", required=True, help="League code like I1, E0, SP1")
    parser.add_argument("--season-year", type=int, default=None, help="Optional event_fixture season_year filter")
    parser.add_argument("--start-date", default=None, help="Optional inclusive UTC date filter YYYY-MM-DD")
    parser.add_argument("--end-date", default=None, help="Optional inclusive UTC date filter YYYY-MM-DD")
    parser.add_argument("--limit", type=int, default=100, help="Max number of finished fixtures to evaluate")
    parser.add_argument("--head-to-head-weight", type=float, default=0.2)
    parser.add_argument("--draw-probability-floor", type=float, default=0.0)
    parser.add_argument("--draw-margin", type=float, default=0.06)
    parser.add_argument("--home-win-bias", type=float, default=0.01)
    parser.add_argument("--host", default=os.getenv("MYSQL_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("MYSQL_PORT", "3306")))
    parser.add_argument("--user", default=os.getenv("MYSQL_USER", "football_admin"))
    parser.add_argument("--database", default=os.getenv("MYSQL_DATABASE", "historic_football_data"))
    parser.add_argument("--mysql-password-env", default="MYSQL_PASSWORD")
    return parser.parse_args()


def connect_db(args: argparse.Namespace):
    return mysql.connector.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=os.getenv(args.mysql_password_env, ""),
        database=args.database,
        charset="utf8mb4",
        use_unicode=True,
        use_pure=True,
        autocommit=True,
    )


def resolve_event_league_name(conn, league_code: str) -> str:
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT league_name
            FROM league
            WHERE league_code = %s
            LIMIT 1
            """,
            (league_code,),
        )
        row = cur.fetchone()
        if not row:
            raise ValueError(f"League code not found: {league_code!r}")
        historical_name = str(row["league_name"])
        normalized_name = historical_name.replace("_", " ").strip()
        parts = normalized_name.split()
        short_name = " ".join(parts[1:]) if len(parts) > 1 else normalized_name

        cur.execute(
            """
            SELECT league_name
            FROM event_fixture
            WHERE league_name IN (%s, %s)
            ORDER BY fixture_date_utc DESC
            LIMIT 1
            """,
            (short_name, normalized_name),
        )
        event_row = cur.fetchone()
        return str(event_row["league_name"]) if event_row and event_row.get("league_name") else short_name
    finally:
        cur.close()


def actual_outcome(goals_home: Any, goals_away: Any) -> str:
    home = int(goals_home or 0)
    away = int(goals_away or 0)
    if home > away:
        return "home_win"
    if away > home:
        return "away_win"
    return "draw"


def brier_score(prob_home: float, prob_draw: float, prob_away: float, actual: str) -> float:
    targets = {
        "home_win": (1.0, 0.0, 0.0),
        "draw": (0.0, 1.0, 0.0),
        "away_win": (0.0, 0.0, 1.0),
    }
    t_home, t_draw, t_away = targets[actual]
    return (
        (prob_home - t_home) ** 2
        + (prob_draw - t_draw) ** 2
        + (prob_away - t_away) ** 2
    ) / 3.0


def build_fixture_query(args: argparse.Namespace, event_league_name: str) -> tuple[str, List[Any]]:
    filters = [f"ef.status_short IN ({FINISHED_STATUS_SQL})"]
    params: List[Any] = []

    if args.season_year is not None:
        filters.append("ef.season_year = %s")
        params.append(args.season_year)
    if args.start_date:
        filters.append("DATE(ef.fixture_date_utc) >= %s")
        params.append(args.start_date)
    if args.end_date:
        filters.append("DATE(ef.fixture_date_utc) <= %s")
        params.append(args.end_date)

    where_sql = " AND ".join(filters)
    sql = f"""
        SELECT
          ef.provider_fixture_id,
          ef.fixture_date_utc,
          ef.season_year,
          ef.home_team_name,
          ef.away_team_name,
          ef.goals_home,
          ef.goals_away,
          ef.status_short
        FROM event_fixture ef
        WHERE ef.league_name = %s
          AND {where_sql}
        ORDER BY ef.fixture_date_utc DESC
        LIMIT %s
    """
    return sql, [event_league_name, *params, args.limit]


def main() -> None:
    args = parse_args()
    db = DBConfig(
        host=args.host,
        port=args.port,
        user=args.user,
        password=os.getenv(args.mysql_password_env, ""),
        database=args.database,
    )

    conn = connect_db(args)
    try:
        event_league_name = resolve_event_league_name(conn, args.league_code)
        cur = conn.cursor(dictionary=True)
        sql, params = build_fixture_query(args, event_league_name)
        cur.execute(sql, params)
        fixtures = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    evaluations: List[Dict[str, Any]] = []
    correct = 0
    by_actual = {"home_win": 0, "draw": 0, "away_win": 0}
    by_predicted = {"home_win": 0, "draw": 0, "away_win": 0}
    confusion = {
        "home_win": {"home_win": 0, "draw": 0, "away_win": 0},
        "draw": {"home_win": 0, "draw": 0, "away_win": 0},
        "away_win": {"home_win": 0, "draw": 0, "away_win": 0},
    }
    cumulative_brier = 0.0

    for fixture in fixtures:
        fixture_ts = fixture["fixture_date_utc"]
        as_of_utc = fixture_ts.strftime("%Y-%m-%d %H:%M:%S") if hasattr(fixture_ts, "strftime") else str(fixture_ts)
        prediction = predict_match_outcome(
            db=db,
            home_team_name=str(fixture["home_team_name"]),
            away_team_name=str(fixture["away_team_name"]),
            league_code=args.league_code,
            season_year=int(fixture["season_year"]),
            as_of_utc=as_of_utc,
            head_to_head_weight=args.head_to_head_weight,
            draw_probability_floor=args.draw_probability_floor,
            draw_margin=args.draw_margin,
            home_win_bias=args.home_win_bias,
        )
        actual = actual_outcome(fixture["goals_home"], fixture["goals_away"])
        predicted = str(prediction["prediction"]["predicted_outcome"])
        by_actual[actual] += 1
        by_predicted[predicted] += 1
        confusion[actual][predicted] += 1
        cumulative_brier += brier_score(
            float(prediction["prediction"]["home_win_probability"]),
            float(prediction["prediction"]["draw_probability"]),
            float(prediction["prediction"]["away_win_probability"]),
            actual,
        )
        is_correct = actual == predicted
        if is_correct:
            correct += 1

        evaluations.append(
            {
                "provider_fixture_id": int(fixture["provider_fixture_id"]),
                "fixture_date_utc": as_of_utc,
                "season_year": int(fixture["season_year"]),
                "home_team": fixture["home_team_name"],
                "away_team": fixture["away_team_name"],
                "actual_score": f"{int(fixture['goals_home'] or 0)}-{int(fixture['goals_away'] or 0)}",
                "actual_outcome": actual,
                "predicted_outcome": predicted,
                "most_likely_score": prediction["prediction"]["most_likely_score"],
                "home_win_probability": prediction["prediction"]["home_win_probability"],
                "draw_probability": prediction["prediction"]["draw_probability"],
                "away_win_probability": prediction["prediction"]["away_win_probability"],
                "correct": is_correct,
            }
        )

    balanced_accuracy = 0.0
    classes_with_support = 0
    for label in ("home_win", "draw", "away_win"):
        support = by_actual[label]
        if support > 0:
            balanced_accuracy += confusion[label][label] / support
            classes_with_support += 1
    if classes_with_support > 0:
        balanced_accuracy /= classes_with_support

    predicted_draws = by_predicted["draw"]
    actual_draws = by_actual["draw"]
    true_draws = confusion["draw"]["draw"]
    draw_precision = (true_draws / predicted_draws) if predicted_draws else 0.0
    draw_recall = (true_draws / actual_draws) if actual_draws else 0.0

    summary = {
        "league_code": args.league_code,
        "season_year": args.season_year,
        "evaluated_matches": len(evaluations),
        "correct_predictions": correct,
        "accuracy": round((correct / len(evaluations)), 4) if evaluations else 0.0,
        "balanced_accuracy": round(balanced_accuracy, 4),
        "brier_score": round((cumulative_brier / len(evaluations)), 4) if evaluations else 0.0,
        "draw_precision": round(draw_precision, 4),
        "draw_recall": round(draw_recall, 4),
        "confusion_matrix": confusion,
        "calibration": {
            "head_to_head_weight": args.head_to_head_weight,
            "draw_probability_floor": args.draw_probability_floor,
            "draw_margin": args.draw_margin,
            "home_win_bias": args.home_win_bias,
        },
        "actual_outcomes": by_actual,
        "predicted_outcomes": by_predicted,
    }

    print(json.dumps({"summary": summary, "matches": evaluations}, indent=2, ensure_ascii=True))


if __name__ == "__main__":
    main()