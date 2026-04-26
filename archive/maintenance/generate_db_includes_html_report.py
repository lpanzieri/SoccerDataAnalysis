#!/usr/bin/env python3
"""Generate an HTML snapshot of what the current database contains.

The report is read-only and focuses on:
- core table footprint and season span
- major-5 league coverage for 2016-2025
- all-league footprint and season drilldowns
- backfill queue health
- finished-fixture player-stat gaps
"""

from __future__ import annotations

import argparse
import datetime as dt
import html
import os
import sys
from pathlib import Path
from typing import Iterable, Sequence

import mysql.connector


FINISHED_STATUSES = ("FT", "AET", "PEN", "FT_PEN", "AWD", "WO")
NON_PLAYED_STATUSES = ("CANC", "PST", "ABD", "SUSP", "INT")
FINISHED_SQL = ", ".join(f"'{status}'" for status in FINISHED_STATUSES)
NON_PLAYED_SQL = ", ".join(f"'{status}'" for status in NON_PLAYED_STATUSES)


def guard_unsafe_secret_flags() -> None:
    for arg in sys.argv[1:]:
        if arg == "--password" or arg.startswith("--password="):
            raise SystemExit("Unsafe secret flag detected. Use environment variable MYSQL_PASSWORD.")


def parse_args() -> argparse.Namespace:
    guard_unsafe_secret_flags()
    parser = argparse.ArgumentParser(description="Generate HTML DB snapshot report")
    parser.add_argument("--host", default=os.getenv("MYSQL_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("MYSQL_PORT", "3306")))
    parser.add_argument("--user", default=os.getenv("MYSQL_USER", "football_admin"))
    parser.add_argument("--database", default=os.getenv("MYSQL_DATABASE", "historic_football_data"))
    parser.add_argument("--mysql-password-env", default="MYSQL_PASSWORD")
    parser.add_argument("--min-year", type=int, default=2016)
    parser.add_argument("--max-year", type=int, default=2025)
    parser.add_argument("--max-open-tasks", type=int, default=40)
    parser.add_argument("--max-gap-rows", type=int, default=100)
    parser.add_argument(
        "--out",
        default="",
        help="Optional explicit output path. Defaults to plans/reports/db_includes_report_<timestamp>.html",
    )
    return parser.parse_args()


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


def fetch_rows(cur, sql: str, params: Sequence[object] | None = None):
    cur.execute(sql, params or ())
    columns = [desc[0] for desc in cur.description]
    return columns, cur.fetchall()


def classify_cell(value: object) -> str:
    if value is None:
        return ""
    text = str(value)
    if text in {"COMPLETE", "SEASON_FINISHED"}:
        return "cell-good"
    if text in {"PARTIAL", "IN_SEASON", "NO_DATA_EXPECTED_YET"}:
        return "cell-warn"
    if text in {"MISSING", "SEASON_TERMINATED_OR_CANCELLED", "NO_FIXTURE_DATA"}:
        return "cell-bad"
    return ""


def render_table(columns: Sequence[str], rows: Iterable[Sequence[object]]) -> str:
    header_html = "".join(f"<th>{html.escape(str(col))}</th>" for col in columns)
    body_rows = []
    for row in rows:
        cells = "".join(
            f'<td class="{classify_cell(value)}">{html.escape("" if value is None else str(value))}</td>'
            for value in row
        )
        body_rows.append(f"<tr>{cells}</tr>")
    body_html = "".join(body_rows) if body_rows else f"<tr><td colspan=\"{len(columns)}\">No rows</td></tr>"
    return f"<table class=\"report-table\"><thead><tr>{header_html}</tr></thead><tbody>{body_html}</tbody></table>"


def render_kpi_cards(cards: Sequence[dict[str, str]]) -> str:
    items = []
    for card in cards:
        items.append(
            "".join(
                [
                    '<div class="kpi-card">',
                    f'<div class="kpi-label">{html.escape(card["label"])}</div>',
                    f'<div class="kpi-value">{html.escape(card["value"])}</div>',
                    f'<div class="kpi-note">{html.escape(card["note"])}</div>',
                    "</div>",
                ]
            )
        )
    return f'<section class="card col-12"><h2>Snapshot Highlights</h2><div class="kpi-grid">{"".join(items)}</div></section>'


def render_bar_chart(title: str, rows: Sequence[dict[str, object]], value_key: str, note_key: str) -> str:
    max_value = max((float(row[value_key]) for row in rows), default=0.0)
    body = []
    for row in rows:
        label = html.escape(str(row["label"]))
        value = float(row[value_key])
        note = html.escape(str(row[note_key]))
        width_pct = 0.0 if max_value <= 0 else round((value * 100.0) / max_value, 2)
        value_text = html.escape(str(row["value_text"]))
        body.append(
            "".join(
                [
                    '<div class="bar-row">',
                    '<div class="bar-head">',
                    f'<span class="bar-label">{label}</span>',
                    f'<span class="bar-value">{value_text}</span>',
                    "</div>",
                    f'<div class="bar-track"><div class="bar-fill" style="width: {width_pct}%"></div></div>',
                    f'<div class="bar-note">{note}</div>',
                    "</div>",
                ]
            )
        )
    return f'<section class="card col-6"><h2>{html.escape(title)}</h2><div class="bar-chart">{"".join(body)}</div></section>'


def render_summary(title: str, items: Sequence[str]) -> str:
    if not items:
        items = ["No notable summary items were derived from the current snapshot."]
    content = "".join(f"<li>{html.escape(item)}</li>" for item in items)
    return f'<section class="card col-12"><h2>{html.escape(title)}</h2><ul class="summary-list">{content}</ul></section>'


def render_league_drilldowns(rows: Sequence[dict[str, object]]) -> str:
    grouped: dict[tuple[object, object], list[dict[str, object]]] = {}
    for row in rows:
        key = (row["league_id"], row["league_name"])
        grouped.setdefault(key, []).append(row)

    blocks: list[str] = []
    for (league_id, league_name), league_rows in sorted(grouped.items(), key=lambda item: str(item[0][1])):
        ordered = sorted(league_rows, key=lambda item: int(item["season_year"]))
        summary_bits = []
        finished_count = sum(1 for row in ordered if row["season_state"] == "SEASON_FINISHED")
        partial_count = sum(1 for row in ordered if row["player_data_status"] == "PARTIAL")
        in_season_count = sum(1 for row in ordered if row["season_state"] == "IN_SEASON")
        if finished_count:
            summary_bits.append(f"finished seasons={finished_count}")
        if partial_count:
            summary_bits.append(f"player partial seasons={partial_count}")
        if in_season_count:
            summary_bits.append(f"in-season={in_season_count}")
        if not summary_bits:
            summary_bits.append("no notable flags")

        columns = [
            "season_year",
            "fixtures_total",
            "fixtures_finished",
            "fixtures_non_played",
            "season_state",
            "fixtures_with_timeline",
            "fixtures_with_goals",
            "fixtures_with_player_stats",
            "timeline_pct_of_finished",
            "player_pct_of_finished",
            "timeline_status",
            "player_data_status",
        ]
        table_rows = [[row[col] for col in columns] for row in ordered]
        blocks.append(
            "".join(
                [
                    '<details class="league-detail">',
                    f'<summary><span>{html.escape(str(league_name))} ({html.escape(str(league_id))})</span><span class="detail-note">{html.escape(" | ".join(summary_bits))}</span></summary>',
                    render_table(columns, table_rows),
                    "</details>",
                ]
            )
        )
    return '<section class="card col-12"><h2>All-League Season Drilldowns</h2><div class="detail-stack">' + "".join(blocks) + "</div></section>"


def report_header(snapshot_time: str) -> str:
    return f"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Database Includes Report</title>
  <style>
    :root {{
      --bg: #f0f8f8;
      --panel: #f8fbfc;
      --ink: #1a3a3a;
      --muted: #4a6b6b;
      --line: #c5dfe0;
      --accent: #7c3aed;
      --ok: #0b7e66;
      --warn: #8b5cf6;
      --bad: #c7254e;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      color: var(--ink);
      font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
      background: radial-gradient(circle at top left, #e0f9ff 0, var(--bg) 40%), linear-gradient(180deg, #f0f8f8 0%, #e8f5f7 100%);
    }}
    .wrap {{ max-width: 1360px; margin: 0 auto; padding: 28px 18px 40px; }}
    .hero, .card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 16px;
      box-shadow: 0 10px 24px rgba(11, 126, 102, 0.08);
    }}
    .hero {{ padding: 22px; }}
    .search-box {{
      display: flex;
      gap: 12px;
      margin-bottom: 20px;
      align-items: center;
    }}
    .search-input {{
      flex: 1;
      padding: 12px 14px;
      border: 2px solid var(--line);
      border-radius: 10px;
      font-size: 14px;
      font-family: inherit;
      background: #fafcfc;
      color: var(--ink);
      transition: border-color 0.2s;
    }}
    .search-input:focus {{
      outline: none;
      border-color: var(--accent);
      background: #fff;
    }}
    .search-hint {{
      font-size: 12px;
      color: var(--muted);
    }}
    .grid {{ display: grid; grid-template-columns: repeat(12, 1fr); gap: 14px; margin-top: 14px; }}
    .card {{ padding: 16px; }}
    .col-12 {{ grid-column: span 12; }}
    .col-6 {{ grid-column: span 6; }}
    h1 {{ margin: 0 0 8px; font-size: 30px; color: var(--ink); }}
    h2 {{ margin: 0 0 12px; font-size: 18px; color: var(--ink); }}
    p {{ margin: 0; }}
    .sub {{ color: var(--muted); font-size: 14px; }}
    .legend {{ display: flex; gap: 8px; flex-wrap: wrap; margin-top: 10px; }}
    .chip {{ padding: 4px 8px; border-radius: 999px; font-size: 12px; border: 1px solid var(--line); background: #fff; }}
    .chip.ok {{ color: var(--ok); background: #e8f8f4; border-color: #a6dcc9; }}
    .chip.warn {{ color: var(--warn); background: #f3e8ff; border-color: #e0b0ff; }}
    .chip.bad {{ color: var(--bad); background: #ffe5eb; border-color: #ffb3c1; }}
    .kpi-grid {{ display: grid; grid-template-columns: repeat(6, 1fr); gap: 12px; }}
    .kpi-card {{ border: 1px solid #c5dfe0; border-radius: 14px; padding: 14px; background: linear-gradient(180deg, #f8fbfc 0%, #f0f8f8 100%); }}
    .kpi-label {{ font-size: 12px; text-transform: uppercase; letter-spacing: 0.08em; color: var(--muted); margin-bottom: 8px; }}
    .kpi-value {{ font-size: 28px; line-height: 1; font-weight: 700; color: #0b7e66; margin-bottom: 8px; }}
    .kpi-note {{ font-size: 12px; color: var(--muted); }}
    .bar-chart {{ display: grid; gap: 10px; }}
    .bar-row {{ border: 1px solid #d5e8e9; border-radius: 12px; padding: 10px 12px; background: #f5fbfc; }}
    .bar-head {{ display: flex; justify-content: space-between; gap: 12px; font-size: 13px; margin-bottom: 6px; }}
    .bar-label {{ font-weight: 600; color: var(--ink); }}
    .bar-value {{ color: var(--muted); }}
    .bar-track {{ width: 100%; height: 10px; background: #d5e8e9; border-radius: 999px; overflow: hidden; }}
    .bar-fill {{ height: 100%; background: linear-gradient(90deg, #0b7e66 0%, #059669 100%); border-radius: 999px; }}
    .bar-note {{ margin-top: 6px; font-size: 12px; color: var(--muted); }}
    .summary-list {{ margin: 0; padding-left: 18px; display: grid; gap: 8px; }}
    .summary-list li {{ color: #1a3a3a; }}
    .league-detail {{ border: 1px solid #c5dfe0; border-radius: 14px; background: #f5fbfc; overflow: hidden; }}
    .league-detail + .league-detail {{ margin-top: 10px; }}
    .league-detail summary {{ cursor: pointer; display: flex; justify-content: space-between; gap: 16px; padding: 14px 16px; font-weight: 600; color: #0b7e66; }}
    .league-detail[open] summary {{ border-bottom: 1px solid #c5dfe0; background: #f0f8f8; }}
    .detail-note {{ color: var(--muted); font-weight: 400; font-size: 12px; }}
    .detail-stack {{ display: grid; gap: 10px; }}
    .report-table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    .report-table th, .report-table td {{ text-align: left; padding: 8px 9px; border-bottom: 1px solid #d5e8e9; vertical-align: top; }}
    .report-table thead th {{ background: #e0f0f1; position: sticky; top: 0; z-index: 1; color: #0b7e66; font-weight: 600; }}
    .report-table tbody tr:nth-child(even) {{ background: #f5fbfc; }}
    .cell-good {{ color: var(--ok); font-weight: 600; }}
    .cell-warn {{ color: var(--warn); font-weight: 600; }}
    .cell-bad {{ color: var(--bad); font-weight: 600; }}
    .hidden {{ display: none !important; }}
    @media (max-width: 980px) {{
      .col-6, .col-12 {{ grid-column: span 12; }}
      .kpi-grid {{ grid-template-columns: repeat(2, 1fr); }}
      .league-detail summary {{ flex-direction: column; align-items: flex-start; }}
      h1 {{ font-size: 24px; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>Database Includes Report</h1>
      <p class="sub">Snapshot generated: {html.escape(snapshot_time)}. Scope: current database footprint, major-5 season coverage, all-league coverage, queue health, and unresolved finished-fixture player gaps.</p>
      <div class="legend">
        <span class="chip ok">COMPLETE means coverage matches finished fixtures</span>
        <span class="chip warn">PARTIAL means some finished fixtures still miss payloads</span>
        <span class="chip bad">TERMINATED/CANCELLED marks seasons closed by non-played fixtures</span>
      </div>
      <div class="search-box">
        <input type="text" id="searchInput" class="search-input" placeholder="Filter tables, sections, and text across the entire report...">
        <span class="search-hint">Type to filter</span>
      </div>
    </section>
    <div class="grid">
"""


def report_footer() -> str:
    return """
    </div>
  </div>
  <script>
    (function() {
      const searchInput = document.getElementById('searchInput');
      if (!searchInput) return;

      function normalizeText(text) {
        return (text || '').toLowerCase().trim();
      }

      function getElementText(el) {
        let text = '';
        if (el.nodeType === Node.TEXT_NODE) {
          return el.textContent;
        }
        for (let node of el.childNodes) {
          text += getElementText(node);
        }
        return text;
      }

      function filter(query) {
        const cards = document.querySelectorAll('.card');
        const details = document.querySelectorAll('.league-detail');
        const normalizedQuery = normalizeText(query);

        cards.forEach(card => {
          let isVisible = false;
          const heading = card.querySelector('h2');
          const headingText = heading ? normalizeText(heading.textContent) : '';
          
          if (!normalizedQuery || headingText.includes(normalizedQuery)) {
            isVisible = true;
          } else {
            const tables = card.querySelectorAll('.report-table');
            for (let table of tables) {
              const text = normalizeText(getElementText(table));
              if (text.includes(normalizedQuery)) {
                isVisible = true;
                break;
              }
            }
            if (!isVisible) {
              const summaryLists = card.querySelectorAll('.summary-list');
              for (let list of summaryLists) {
                const text = normalizeText(getElementText(list));
                if (text.includes(normalizedQuery)) {
                  isVisible = true;
                  break;
                }
              }
            }
            if (!isVisible) {
              const barCharts = card.querySelectorAll('.bar-chart');
              for (let chart of barCharts) {
                const text = normalizeText(getElementText(chart));
                if (text.includes(normalizedQuery)) {
                  isVisible = true;
                  break;
                }
              }
            }
          }
          
          card.classList.toggle('hidden', !isVisible);
        });

        details.forEach(detail => {
          let isVisible = false;
          const summary = detail.querySelector('summary');
          const summaryText = summary ? normalizeText(summary.textContent) : '';
          
          if (!normalizedQuery || summaryText.includes(normalizedQuery)) {
            isVisible = true;
          } else {
            const tables = detail.querySelectorAll('.report-table');
            for (let table of tables) {
              const text = normalizeText(getElementText(table));
              if (text.includes(normalizedQuery)) {
                isVisible = true;
                break;
              }
            }
          }
          
          detail.classList.toggle('hidden', !isVisible);
        });
      }

      searchInput.addEventListener('input', (e) => {
        filter(e.target.value);
      });
    })();
  </script>
</body>
</html>
"""


def main() -> None:
    args = parse_args()
    timestamp = dt.datetime.now(dt.timezone.utc)
    output_path = Path(args.out) if args.out else Path(
        f"plans/reports/db_includes_report_{timestamp.strftime('%Y%m%d_%H%M%S')}.html"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)

    conn = connect_db(args)
    cur = conn.cursor()
    try:
        core_columns, core_rows = fetch_rows(
            cur,
            """
            SELECT 'event_fixture_rows' AS metric, COUNT(*) AS value FROM event_fixture
            UNION ALL SELECT 'event_timeline_rows', COUNT(*) FROM event_timeline
            UNION ALL SELECT 'event_goal_rows', COUNT(*) FROM event_goal
            UNION ALL SELECT 'player_match_stats_rows', COUNT(*) FROM player_match_stats
            UNION ALL SELECT 'event_api_call_log_rows', COUNT(*) FROM event_api_call_log
            UNION ALL SELECT 'backfill_task_rows', COUNT(*) FROM backfill_task
            UNION ALL SELECT 'distinct_fixture_in_timeline', COUNT(DISTINCT provider_fixture_id) FROM event_timeline
            UNION ALL SELECT 'distinct_fixture_in_player_stats', COUNT(DISTINCT provider_fixture_id) FROM player_match_stats
            UNION ALL SELECT 'distinct_leagues_in_fixture', COUNT(DISTINCT league_id) FROM event_fixture
            UNION ALL SELECT 'fixture_min_season', MIN(season_year) FROM event_fixture
            UNION ALL SELECT 'fixture_max_season', MAX(season_year) FROM event_fixture
            """,
        )

        major5_global_columns, major5_global_rows = fetch_rows(
            cur,
            """
            SELECT
              l.league_code,
              l.league_name,
              COALESCE(COUNT(DISTINCT ef.provider_fixture_id), 0) AS fixture_count,
              COALESCE(COUNT(DISTINCT et.provider_fixture_id), 0) AS fixture_with_timeline,
              COALESCE(COUNT(DISTINCT eg.provider_fixture_id), 0) AS fixture_with_goals,
              COALESCE(COUNT(DISTINCT pms.provider_fixture_id), 0) AS fixture_with_player_stats,
              MIN(ef.season_year) AS min_season,
              MAX(ef.season_year) AS max_season
            FROM (
              SELECT 'E0' AS league_code, 'Premier League' AS league_name, 39 AS api_league_id
              UNION ALL SELECT 'SP1', 'La Liga', 140
              UNION ALL SELECT 'I1', 'Serie A', 135
              UNION ALL SELECT 'D1', 'Bundesliga', 78
              UNION ALL SELECT 'F1', 'Ligue 1', 61
            ) l
            LEFT JOIN event_fixture ef ON ef.league_id = l.api_league_id
            LEFT JOIN (SELECT DISTINCT provider_fixture_id FROM event_timeline) et ON et.provider_fixture_id = ef.provider_fixture_id
            LEFT JOIN (SELECT DISTINCT provider_fixture_id FROM event_goal) eg ON eg.provider_fixture_id = ef.provider_fixture_id
            LEFT JOIN (SELECT DISTINCT provider_fixture_id FROM player_match_stats) pms ON pms.provider_fixture_id = ef.provider_fixture_id
            GROUP BY l.league_code, l.league_name
            ORDER BY l.league_code
            """,
        )

        all_leagues_columns, all_leagues_rows = fetch_rows(
            cur,
            f"""
            WITH timeline_fixtures AS (
              SELECT DISTINCT provider_fixture_id FROM event_timeline
            ),
            goal_fixtures AS (
              SELECT DISTINCT provider_fixture_id FROM event_goal
            ),
            player_fixtures AS (
              SELECT DISTINCT provider_fixture_id FROM player_match_stats
            )
            SELECT
              ef.league_id,
              MIN(COALESCE(NULLIF(ef.league_name, ''), CONCAT('League ', ef.league_id))) AS league_name,
              COUNT(DISTINCT ef.provider_fixture_id) AS fixtures_total,
              COUNT(DISTINCT CASE WHEN ef.status_short IN ({FINISHED_SQL}) THEN ef.provider_fixture_id END) AS fixtures_finished,
              COUNT(DISTINCT CASE WHEN ef.status_short IN ({NON_PLAYED_SQL}) THEN ef.provider_fixture_id END) AS fixtures_non_played,
              COUNT(DISTINCT tf.provider_fixture_id) AS fixtures_with_timeline,
              COUNT(DISTINCT gf.provider_fixture_id) AS fixtures_with_goals,
              COUNT(DISTINCT pf.provider_fixture_id) AS fixtures_with_player_stats,
              ROUND(CASE WHEN COUNT(DISTINCT CASE WHEN ef.status_short IN ({FINISHED_SQL}) THEN ef.provider_fixture_id END) = 0 THEN 0 ELSE 100.0 * COUNT(DISTINCT tf.provider_fixture_id) / COUNT(DISTINCT CASE WHEN ef.status_short IN ({FINISHED_SQL}) THEN ef.provider_fixture_id END) END, 2) AS timeline_pct_of_finished,
              ROUND(CASE WHEN COUNT(DISTINCT CASE WHEN ef.status_short IN ({FINISHED_SQL}) THEN ef.provider_fixture_id END) = 0 THEN 0 ELSE 100.0 * COUNT(DISTINCT pf.provider_fixture_id) / COUNT(DISTINCT CASE WHEN ef.status_short IN ({FINISHED_SQL}) THEN ef.provider_fixture_id END) END, 2) AS player_pct_of_finished,
              MIN(ef.season_year) AS min_season,
              MAX(ef.season_year) AS max_season
            FROM event_fixture ef
            LEFT JOIN timeline_fixtures tf ON tf.provider_fixture_id = ef.provider_fixture_id
            LEFT JOIN goal_fixtures gf ON gf.provider_fixture_id = ef.provider_fixture_id
            LEFT JOIN player_fixtures pf ON pf.provider_fixture_id = ef.provider_fixture_id
            GROUP BY ef.league_id
            ORDER BY fixtures_total DESC, ef.league_id ASC
            """,
(),
        )

        season_columns, season_rows = fetch_rows(
            cur,
            f"""
            WITH RECURSIVE seasons AS (
              SELECT %s AS season_year
              UNION ALL
              SELECT season_year + 1 FROM seasons WHERE season_year < %s
            ),
            leagues AS (
              SELECT 'E0' AS league_code, 'Premier League' AS league_name, 39 AS api_league_id
              UNION ALL SELECT 'SP1', 'La Liga', 140
              UNION ALL SELECT 'I1', 'Serie A', 135
              UNION ALL SELECT 'D1', 'Bundesliga', 78
              UNION ALL SELECT 'F1', 'Ligue 1', 61
            ),
            timeline_fixtures AS (
              SELECT DISTINCT provider_fixture_id FROM event_timeline
            ),
            goal_fixtures AS (
              SELECT DISTINCT provider_fixture_id FROM event_goal
            ),
            player_fixtures AS (
              SELECT DISTINCT provider_fixture_id FROM player_match_stats
            ),
            agg AS (
              SELECT
                ef.league_id,
                ef.season_year,
                COUNT(DISTINCT ef.provider_fixture_id) AS fixtures_total,
                COUNT(DISTINCT CASE WHEN ef.status_short IN ({FINISHED_SQL}) THEN ef.provider_fixture_id END) AS fixtures_finished,
                COUNT(DISTINCT CASE WHEN ef.status_short IN ({NON_PLAYED_SQL}) THEN ef.provider_fixture_id END) AS fixtures_non_played,
                COUNT(DISTINCT tf.provider_fixture_id) AS fixtures_with_match_data,
                COUNT(DISTINCT gf.provider_fixture_id) AS fixtures_with_goals,
                COUNT(DISTINCT pf.provider_fixture_id) AS fixtures_with_player_data
              FROM event_fixture ef
              LEFT JOIN timeline_fixtures tf ON tf.provider_fixture_id = ef.provider_fixture_id
              LEFT JOIN goal_fixtures gf ON gf.provider_fixture_id = ef.provider_fixture_id
              LEFT JOIN player_fixtures pf ON pf.provider_fixture_id = ef.provider_fixture_id
              WHERE ef.league_id IN (39, 140, 135, 78, 61)
                AND ef.season_year BETWEEN %s AND %s
              GROUP BY ef.league_id, ef.season_year
            )
            SELECT
              l.league_code,
              l.league_name,
              s.season_year,
              COALESCE(a.fixtures_total, 0) AS fixtures_total,
              COALESCE(a.fixtures_finished, 0) AS fixtures_finished,
              COALESCE(a.fixtures_non_played, 0) AS fixtures_non_played,
              CASE
                WHEN COALESCE(a.fixtures_total, 0) = 0 THEN 'NO_FIXTURE_DATA'
                WHEN COALESCE(a.fixtures_finished, 0) = COALESCE(a.fixtures_total, 0) THEN 'SEASON_FINISHED'
                WHEN COALESCE(a.fixtures_finished, 0) + COALESCE(a.fixtures_non_played, 0) = COALESCE(a.fixtures_total, 0)
                     AND COALESCE(a.fixtures_non_played, 0) > 0 THEN 'SEASON_TERMINATED_OR_CANCELLED'
                ELSE 'IN_SEASON'
              END AS season_state,
              COALESCE(a.fixtures_with_match_data, 0) AS fixtures_with_match_data,
              COALESCE(a.fixtures_with_goals, 0) AS fixtures_with_goals,
              COALESCE(a.fixtures_with_player_data, 0) AS fixtures_with_player_data,
              ROUND(CASE WHEN COALESCE(a.fixtures_finished, 0) = 0 THEN 0 ELSE 100.0 * COALESCE(a.fixtures_with_match_data, 0) / a.fixtures_finished END, 2) AS match_cov_pct_of_finished,
              ROUND(CASE WHEN COALESCE(a.fixtures_finished, 0) = 0 THEN 0 ELSE 100.0 * COALESCE(a.fixtures_with_player_data, 0) / a.fixtures_finished END, 2) AS player_cov_pct_of_finished,
              CASE
                WHEN COALESCE(a.fixtures_finished, 0) = 0 THEN 'NO_DATA_EXPECTED_YET'
                WHEN COALESCE(a.fixtures_with_match_data, 0) >= COALESCE(a.fixtures_finished, 0) THEN 'COMPLETE'
                WHEN COALESCE(a.fixtures_with_match_data, 0) = 0 THEN 'MISSING'
                ELSE 'PARTIAL'
              END AS match_data_status,
              CASE
                WHEN COALESCE(a.fixtures_finished, 0) = 0 THEN 'NO_DATA_EXPECTED_YET'
                WHEN COALESCE(a.fixtures_with_player_data, 0) >= COALESCE(a.fixtures_finished, 0) THEN 'COMPLETE'
                WHEN COALESCE(a.fixtures_with_player_data, 0) = 0 THEN 'MISSING'
                ELSE 'PARTIAL'
              END AS player_data_status
            FROM leagues l
            CROSS JOIN seasons s
            LEFT JOIN agg a ON a.league_id = l.api_league_id AND a.season_year = s.season_year
            ORDER BY l.league_code, s.season_year
            """,
            (args.min_year, args.max_year, args.min_year, args.max_year),
        )

        all_league_season_columns, all_league_season_rows = fetch_rows(
            cur,
            f"""
            WITH timeline_fixtures AS (
              SELECT DISTINCT provider_fixture_id FROM event_timeline
            ),
            goal_fixtures AS (
              SELECT DISTINCT provider_fixture_id FROM event_goal
            ),
            player_fixtures AS (
              SELECT DISTINCT provider_fixture_id FROM player_match_stats
            )
            SELECT
              ef.league_id,
              MIN(COALESCE(NULLIF(ef.league_name, ''), CONCAT('League ', ef.league_id))) AS league_name,
              ef.season_year,
              COUNT(DISTINCT ef.provider_fixture_id) AS fixtures_total,
              COUNT(DISTINCT CASE WHEN ef.status_short IN ({FINISHED_SQL}) THEN ef.provider_fixture_id END) AS fixtures_finished,
              COUNT(DISTINCT CASE WHEN ef.status_short IN ({NON_PLAYED_SQL}) THEN ef.provider_fixture_id END) AS fixtures_non_played,
              CASE
                WHEN COUNT(DISTINCT ef.provider_fixture_id) = 0 THEN 'NO_FIXTURE_DATA'
                WHEN COUNT(DISTINCT CASE WHEN ef.status_short IN ({FINISHED_SQL}) THEN ef.provider_fixture_id END) = COUNT(DISTINCT ef.provider_fixture_id) THEN 'SEASON_FINISHED'
                WHEN COUNT(DISTINCT CASE WHEN ef.status_short IN ({FINISHED_SQL}) THEN ef.provider_fixture_id END)
                     + COUNT(DISTINCT CASE WHEN ef.status_short IN ({NON_PLAYED_SQL}) THEN ef.provider_fixture_id END)
                     = COUNT(DISTINCT ef.provider_fixture_id)
                     AND COUNT(DISTINCT CASE WHEN ef.status_short IN ({NON_PLAYED_SQL}) THEN ef.provider_fixture_id END) > 0
                THEN 'SEASON_TERMINATED_OR_CANCELLED'
                ELSE 'IN_SEASON'
              END AS season_state,
              COUNT(DISTINCT tf.provider_fixture_id) AS fixtures_with_timeline,
              COUNT(DISTINCT gf.provider_fixture_id) AS fixtures_with_goals,
              COUNT(DISTINCT pf.provider_fixture_id) AS fixtures_with_player_stats,
              ROUND(CASE WHEN COUNT(DISTINCT CASE WHEN ef.status_short IN ({FINISHED_SQL}) THEN ef.provider_fixture_id END) = 0 THEN 0 ELSE 100.0 * COUNT(DISTINCT tf.provider_fixture_id) / COUNT(DISTINCT CASE WHEN ef.status_short IN ({FINISHED_SQL}) THEN ef.provider_fixture_id END) END, 2) AS timeline_pct_of_finished,
              ROUND(CASE WHEN COUNT(DISTINCT CASE WHEN ef.status_short IN ({FINISHED_SQL}) THEN ef.provider_fixture_id END) = 0 THEN 0 ELSE 100.0 * COUNT(DISTINCT pf.provider_fixture_id) / COUNT(DISTINCT CASE WHEN ef.status_short IN ({FINISHED_SQL}) THEN ef.provider_fixture_id END) END, 2) AS player_pct_of_finished,
              CASE
                WHEN COUNT(DISTINCT CASE WHEN ef.status_short IN ({FINISHED_SQL}) THEN ef.provider_fixture_id END) = 0 THEN 'NO_DATA_EXPECTED_YET'
                WHEN COUNT(DISTINCT tf.provider_fixture_id) >= COUNT(DISTINCT CASE WHEN ef.status_short IN ({FINISHED_SQL}) THEN ef.provider_fixture_id END) THEN 'COMPLETE'
                WHEN COUNT(DISTINCT tf.provider_fixture_id) = 0 THEN 'MISSING'
                ELSE 'PARTIAL'
              END AS timeline_status,
              CASE
                WHEN COUNT(DISTINCT CASE WHEN ef.status_short IN ({FINISHED_SQL}) THEN ef.provider_fixture_id END) = 0 THEN 'NO_DATA_EXPECTED_YET'
                WHEN COUNT(DISTINCT pf.provider_fixture_id) >= COUNT(DISTINCT CASE WHEN ef.status_short IN ({FINISHED_SQL}) THEN ef.provider_fixture_id END) THEN 'COMPLETE'
                WHEN COUNT(DISTINCT pf.provider_fixture_id) = 0 THEN 'MISSING'
                ELSE 'PARTIAL'
              END AS player_data_status
            FROM event_fixture ef
            LEFT JOIN timeline_fixtures tf ON tf.provider_fixture_id = ef.provider_fixture_id
            LEFT JOIN goal_fixtures gf ON gf.provider_fixture_id = ef.provider_fixture_id
            LEFT JOIN player_fixtures pf ON pf.provider_fixture_id = ef.provider_fixture_id
            GROUP BY ef.league_id, ef.season_year
            ORDER BY league_name, ef.season_year
            """,
(),
        )

        queue_columns, queue_rows = fetch_rows(
            cur,
            """
            SELECT status, COUNT(*) AS task_count
            FROM backfill_task
            GROUP BY status
            ORDER BY FIELD(status, 'in_progress', 'pending', 'blocked', 'completed', 'skipped'), status
            """,
        )

        open_columns, open_rows = fetch_rows(
            cur,
            """
            SELECT task_id, status, day_no, league_code, start_year, estimated_calls, notes, updated_at
            FROM backfill_task
            WHERE status IN ('in_progress', 'pending', 'blocked')
            ORDER BY FIELD(status, 'in_progress', 'pending', 'blocked'), day_no, updated_at DESC
            LIMIT %s
            """,
            (args.max_open_tasks,),
        )

        gaps_columns, gaps_rows = fetch_rows(
            cur,
            f"""
            SELECT
              ef.league_id,
              ef.season_year,
              ef.provider_fixture_id,
              DATE(ef.fixture_date_utc) AS fixture_date,
              ef.home_team_name,
              ef.away_team_name,
              ef.status_short,
              ef.status_long
            FROM event_fixture ef
            LEFT JOIN player_match_stats pms ON pms.provider_fixture_id = ef.provider_fixture_id
            WHERE ef.league_id IN (39, 140, 135, 78, 61)
              AND ef.season_year BETWEEN %s AND %s
              AND ef.status_short IN ({FINISHED_SQL})
              AND pms.provider_fixture_id IS NULL
            ORDER BY ef.season_year, ef.league_id, ef.fixture_date_utc
            LIMIT %s
            """,
            (args.min_year, args.max_year, args.max_gap_rows),
        )

        core_map = {str(metric): value for metric, value in core_rows}
        queue_map = {str(status): int(count) for status, count in queue_rows}
        all_leagues_dicts = [dict(zip(all_leagues_columns, row)) for row in all_leagues_rows]
        season_drilldown_dicts = [dict(zip(all_league_season_columns, row)) for row in all_league_season_rows]

        snapshot_cards = [
            {
                "label": "Fixtures",
                "value": f"{int(core_map.get('event_fixture_rows', 0)):,}",
                "note": f"Across {int(core_map.get('distinct_leagues_in_fixture', 0))} leagues and {int(core_map.get('fixture_max_season', 0)) - int(core_map.get('fixture_min_season', 0)) + 1 if core_map.get('fixture_max_season') is not None and core_map.get('fixture_min_season') is not None else 0} seasons",
            },
            {
                "label": "Timeline Rows",
                "value": f"{int(core_map.get('event_timeline_rows', 0)):,}",
                "note": f"{int(core_map.get('distinct_fixture_in_timeline', 0)):,} fixtures with event timelines",
            },
            {
                "label": "Player Stat Rows",
                "value": f"{int(core_map.get('player_match_stats_rows', 0)):,}",
                "note": f"{int(core_map.get('distinct_fixture_in_player_stats', 0)):,} fixtures with player payloads",
            },
            {
                "label": "API Log Rows",
                "value": f"{int(core_map.get('event_api_call_log_rows', 0)):,}",
                "note": "Request audit trail currently stored in DB",
            },
            {
                "label": "Open Queue",
                "value": f"{queue_map.get('in_progress', 0) + queue_map.get('pending', 0) + queue_map.get('blocked', 0):,}",
                "note": f"in_progress={queue_map.get('in_progress', 0)}, pending={queue_map.get('pending', 0)}, blocked={queue_map.get('blocked', 0)}",
            },
            {
                "label": "All Leagues",
                "value": f"{len(all_leagues_dicts):,}",
                "note": "Distinct leagues currently represented in event_fixture",
            },
        ]

        all_league_bars = [
            {
                "label": f"{row['league_name']} ({row['league_id']})",
                "fixtures_total": float(row["fixtures_total"] or 0),
                "value_text": f"{int(row['fixtures_total'] or 0):,} fixtures",
                "note": f"Seasons {row['min_season']} to {row['max_season']} | player coverage {float(row['player_pct_of_finished'] or 0):.2f}%",
            }
            for row in all_leagues_dicts[:10]
        ]
        player_cov_bars = [
            {
                "label": str(row["league_name"]),
                "player_pct_of_finished": float(row["player_pct_of_finished"] or 0),
                "value_text": f"{float(row['player_pct_of_finished'] or 0):.2f}%",
                "note": f"Finished fixtures={int(row['fixtures_finished'] or 0):,}, player fixtures={int(row['fixtures_with_player_stats'] or 0):,}",
            }
            for row in sorted(all_leagues_dicts, key=lambda item: float(item["player_pct_of_finished"] or 0), reverse=True)[:10]
        ]

        executive_summary: list[str] = []
        top_fixture_league = all_leagues_dicts[0] if all_leagues_dicts else None
        if top_fixture_league:
            executive_summary.append(
                f"Largest league footprint is {top_fixture_league['league_name']} ({top_fixture_league['league_id']}) with {int(top_fixture_league['fixtures_total'] or 0):,} fixtures across seasons {top_fixture_league['min_season']} to {top_fixture_league['max_season']}."
            )
        complete_player_leagues = [row for row in all_leagues_dicts if float(row["player_pct_of_finished"] or 0) >= 99.9]
        if complete_player_leagues:
            executive_summary.append(
                "Near-complete player coverage is currently concentrated in the major top-flight leagues: "
                + ", ".join(str(row["league_name"]) for row in complete_player_leagues[:4])
                + "."
            )
        low_player_leagues = [
            row
            for row in all_leagues_dicts
            if int(row["fixtures_finished"] or 0) > 0 and float(row["player_pct_of_finished"] or 0) < 35.0
        ]
        if low_player_leagues:
            executive_summary.append(
                "Lower-tier leagues with the biggest player-data gaps include "
                + ", ".join(
                    f"{row['league_name']} ({float(row['player_pct_of_finished'] or 0):.2f}%)"
                    for row in low_player_leagues[:5]
                )
                + "."
            )
        terminated_seasons = [row for row in season_drilldown_dicts if row["season_state"] == "SEASON_TERMINATED_OR_CANCELLED"]
        if terminated_seasons:
            executive_summary.append(
                "Cancelled or terminated seasons are explicitly flagged now, including "
                + ", ".join(f"{row['league_name']} {row['season_year']}" for row in terminated_seasons[:3])
                + "."
            )
        if queue_map.get("in_progress", 0) or queue_map.get("pending", 0) or queue_map.get("blocked", 0):
            executive_summary.append(
                f"The worker is still active in the background with in_progress={queue_map.get('in_progress', 0)}, pending={queue_map.get('pending', 0)}, blocked={queue_map.get('blocked', 0)}."
            )

        html_parts = [report_header(timestamp.strftime("%Y-%m-%d %H:%M:%S UTC"))]
        html_parts.append(render_summary("Executive Summary", executive_summary))
        html_parts.append(render_kpi_cards(snapshot_cards))
        html_parts.append(render_bar_chart("Top Leagues By Fixture Count", all_league_bars, "fixtures_total", "note"))
        html_parts.append(render_bar_chart("Top Leagues By Player Coverage", player_cov_bars, "player_pct_of_finished", "note"))

        sections = [
            ("Core Table Snapshot", core_columns, core_rows, "col-6"),
            ("Major-5 Global Footprint", major5_global_columns, major5_global_rows, "col-6"),
            ("All Leagues Global Footprint", all_leagues_columns, all_leagues_rows, "col-12"),
            (f"Major-5 Season Coverage ({args.min_year}-{args.max_year})", season_columns, season_rows, "col-12"),
            ("Backfill Queue Status", queue_columns, queue_rows, "col-6"),
            (f"Open/Blocked Queue Items (Top {args.max_open_tasks})", open_columns, open_rows, "col-6"),
            (f"Finished Fixtures Missing Player Stats (Top {args.max_gap_rows})", gaps_columns, gaps_rows, "col-12"),
        ]
        for title, columns, rows, col_class in sections:
            html_parts.append(
                f'<section class="card {col_class}"><h2>{html.escape(title)}</h2>{render_table(columns, rows)}</section>'
            )
        html_parts.append(render_league_drilldowns(season_drilldown_dicts))
        html_parts.append(report_footer())
        output_path.write_text("".join(html_parts), encoding="utf-8")
    finally:
        cur.close()
        conn.close()

    print(output_path)


if __name__ == "__main__":
    main()
