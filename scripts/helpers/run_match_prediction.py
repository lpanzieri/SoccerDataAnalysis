#!/usr/bin/env python3
"""CLI wrapper for the reusable local-data match prediction helper.

Example:
  python scripts/helpers/run_match_prediction.py \
    --league-code I1 \
    --home-team Torino \
    --away-team Inter \
    --season-year 2025 \
    --as-of-utc '2026-04-26 16:00:00'
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Pin matplotlib config/cache to a stable path inside the project so it never
# depends on the HOME env var (which callers may have overwritten, e.g. by using
# "HOME" as a loop variable in bash, causing matplotlib to create folders named
# after football teams in the working directory).
os.environ.setdefault(
    "MPLCONFIGDIR",
    str(ROOT / ".cache" / "matplotlib"),
)

from scripts.helpers.league_records import DBConfig, predict_match_outcome
from scripts.helpers.prediction_html_report import default_report_path, write_prediction_html_report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Predict a football match using local historical data")
    parser.add_argument("--league-code", required=True, help="League code like I1, E0, SP1")
    parser.add_argument("--home-team", required=True, help="Home team name")
    parser.add_argument("--away-team", required=True, help="Away team name")
    parser.add_argument("--season-year", type=int, default=None, help="event_fixture season_year to evaluate")
    parser.add_argument("--as-of-utc", default=None, help="UTC cutoff like 2026-04-26 16:00:00")
    parser.add_argument("--head-to-head-weight", type=float, default=0.2)
    parser.add_argument("--draw-probability-floor", type=float, default=0.0)
    parser.add_argument("--draw-margin", type=float, default=0.06)
    parser.add_argument("--home-win-bias", type=float, default=0.01)
    injury_group = parser.add_mutually_exclusive_group()
    injury_group.add_argument(
        "--with-injury-adjustment",
        action="store_true",
        help="Enable injury-based expected-goals adjustments (disabled by default)",
    )
    injury_group.add_argument(
        "--no-injury-adjustment",
        action="store_true",
        help="Disable injury-based expected-goals adjustments (legacy compatibility flag)",
    )
    parser.add_argument(
        "--injury-weight",
        type=float,
        default=0.005,
        help="Injury adjustment strength when enabled (0.0 to 0.2)",
    )
    xi_group = parser.add_mutually_exclusive_group()
    xi_group.add_argument(
        "--with-xi-boost",
        action="store_true",
        help="Enable expected-starting-XI strength adjustment (disabled by default)",
    )
    xi_group.add_argument(
        "--no-xi-boost",
        action="store_true",
        help="Disable expected-starting-XI strength adjustment (legacy compatibility flag)",
    )
    parser.add_argument(
        "--xi-boost-weight",
        type=float,
        default=0.05,
        help="Projected XI adjustment strength when enabled (0.0 to 0.2)",
    )
    parser.add_argument("--host", default=os.getenv("MYSQL_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("MYSQL_PORT", "3306")))
    parser.add_argument("--user", default=os.getenv("MYSQL_USER", "football_admin"))
    parser.add_argument("--database", default=os.getenv("MYSQL_DATABASE", "historic_football_data"))
    parser.add_argument("--mysql-password-env", default="MYSQL_PASSWORD")
    parser.add_argument(
        "--report-dir",
        default="plans/reports/predictions",
        help="Directory for generated HTML reports",
    )
    parser.add_argument(
        "--report-path",
        default=None,
        help="Explicit report output path. If omitted, an auto timestamped filename is used.",
    )
    parser.add_argument(
        "--no-html-report",
        action="store_true",
        help="Disable HTML report generation for this run",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    db = DBConfig(
        host=args.host,
        port=args.port,
        user=args.user,
        password=os.getenv(args.mysql_password_env, ""),
        database=args.database,
    )
    result = predict_match_outcome(
        db=db,
        home_team_name=args.home_team,
        away_team_name=args.away_team,
        league_code=args.league_code,
        season_year=args.season_year,
        as_of_utc=args.as_of_utc,
        head_to_head_weight=args.head_to_head_weight,
        draw_probability_floor=args.draw_probability_floor,
        draw_margin=args.draw_margin,
        home_win_bias=args.home_win_bias,
        include_injuries=args.with_injury_adjustment and not args.no_injury_adjustment,
        injury_weight=args.injury_weight,
        include_xi_boost=args.with_xi_boost and not args.no_xi_boost,
        xi_boost_weight=args.xi_boost_weight,
    )

    if not args.no_html_report:
        report_path = Path(args.report_path) if args.report_path else default_report_path(result, ROOT / args.report_dir)
        written_path = write_prediction_html_report(result, report_path)
        result["html_report_path"] = str(written_path)

    print(json.dumps(result, indent=2, ensure_ascii=True))


if __name__ == "__main__":
    main()