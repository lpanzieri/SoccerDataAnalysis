from __future__ import annotations

import html
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple


def _slugify(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-") or "unknown"


def _safe_int(value: Any) -> int:
    return int(value or 0)


def _safe_float(value: Any) -> float:
    return float(value or 0.0)


def _pct(value: Any) -> float:
    return round(100.0 * _safe_float(value), 1)


def _outcome_label(predicted_outcome: str, home: str, away: str) -> str:
    if predicted_outcome == "home_win":
        return f"{home} Win"
    if predicted_outcome == "away_win":
        return f"{away} Win"
    return "Draw"


def default_report_path(prediction: Dict[str, Any], report_dir: Path) -> Path:
    report_dir.mkdir(parents=True, exist_ok=True)
    home = _slugify(str(prediction.get("home_team") or "home"))
    away = _slugify(str(prediction.get("away_team") or "away"))
    league = _slugify(str(prediction.get("league_code") or "league"))
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return report_dir / f"prediction_report_{league}_{home}_vs_{away}_{timestamp}.html"


def _top_score_rows(prediction: Dict[str, Any]) -> str:
    rows = prediction.get("prediction", {}).get("score_grid", []) or []
    html_rows: List[str] = []
    for item in rows[:10]:
        hg = _safe_int(item.get("home_goals"))
        ag = _safe_int(item.get("away_goals"))
        prob = _pct(item.get("probability"))
        html_rows.append(f"<tr><td>{hg}-{ag}</td><td>{prob:.1f}%</td></tr>")
    return "\n".join(html_rows)


def _recent_h2h_rows(prediction: Dict[str, Any]) -> str:
    rows = prediction.get("head_to_head", {}).get("recent_matches", []) or []
    html_rows: List[str] = []
    for item in rows[:10]:
        match_date = html.escape(str(item.get("match_date") or ""))
        home = html.escape(str(item.get("home_team") or ""))
        away = html.escape(str(item.get("away_team") or ""))
        hg = _safe_int(item.get("ft_home_goals"))
        ag = _safe_int(item.get("ft_away_goals"))
        result = html.escape(str(item.get("ft_result") or ""))
        html_rows.append(
            f"<tr><td>{match_date}</td><td>{home}</td><td>{away}</td><td>{hg}-{ag}</td><td>{result}</td></tr>"
        )
    return "\n".join(html_rows)


def _insights(prediction: Dict[str, Any]) -> List[str]:
    home = str(prediction.get("home_team") or "Home")
    away = str(prediction.get("away_team") or "Away")
    pred = prediction.get("prediction", {}) or {}
    h2h = prediction.get("head_to_head", {}) or {}
    form = prediction.get("current_form", {}) or {}
    xi_impact = prediction.get("xi_impact", {}) or {}
    injury_impact = prediction.get("injury_impact", {}) or {}

    home_win = _pct(pred.get("home_win_probability"))
    draw = _pct(pred.get("draw_probability"))
    away_win = _pct(pred.get("away_win_probability"))

    best = pred.get("most_likely_score", {}) or {}
    best_score = f"{_safe_int(best.get('home_goals'))}-{_safe_int(best.get('away_goals'))}"
    best_score_prob = _pct(best.get("probability"))

    home_form = form.get("home_team_home_split", {}) or {}
    away_form = form.get("away_team_away_split", {}) or {}
    h2h_overall = h2h.get("overall", {}) or {}

    lines = [
        f"Model edge: {away if away_win >= home_win else home} has the highest single-outcome probability ({max(home_win, away_win):.1f}%).",
        f"Draw risk remains {draw:.1f}%, with the most likely exact score at {best_score} ({best_score_prob:.1f}%).",
        f"Current split form: {home} home goals/game {_safe_float(home_form.get('goals_for_per_game')):.2f}, {away} away goals/game {_safe_float(away_form.get('goals_for_per_game')):.2f}.",
        f"Historical head-to-head sample in this league: {_safe_int(h2h_overall.get('matches'))} matches.",
    ]

    home_missing = _safe_int(injury_impact.get("home_missing_count"))
    away_missing = _safe_int(injury_impact.get("away_missing_count"))
    xi_enabled = bool(xi_impact.get("enabled"))
    xi_delta = _safe_float(xi_impact.get("strength_delta"))
    if xi_enabled and abs(xi_delta) >= 0.01:
        stronger_side = home if xi_delta > 0 else away
        lines.append(
            f"Projected XI strength favors {stronger_side}; xG adjusted via lineup-strength multipliers."
        )
    if home_missing or away_missing:
        lines.append(
            f"Active absences modeled: {home} missing {home_missing}, {away} missing {away_missing}; xG adjusted via injury multipliers."
        )
    return lines


def _completeness_flags(prediction: Dict[str, Any]) -> List[Tuple[str, str]]:
    flags: List[Tuple[str, str]] = []
    form = prediction.get("current_form", {}) or {}
    h2h = prediction.get("head_to_head", {}) or {}
    league_avg = prediction.get("league_averages", {}) or {}
    xi_impact = prediction.get("xi_impact", {}) or {}

    home_played = _safe_int((form.get("home_team_home_split", {}) or {}).get("played"))
    away_played = _safe_int((form.get("away_team_away_split", {}) or {}).get("played"))
    h2h_matches = _safe_int((h2h.get("overall", {}) or {}).get("matches"))
    league_matches = _safe_int(league_avg.get("matches"))

    if home_played < 8 or away_played < 8:
        flags.append(("warning", "Low current-season sample for one or both teams (<8 split matches)."))
    else:
        flags.append(("ok", "Current-season split samples are healthy for both teams."))

    if h2h_matches < 6:
        flags.append(("warning", "Head-to-head history is limited; prediction relies more on form and league baselines."))
    else:
        flags.append(("ok", "Head-to-head sample is large enough to add useful context."))

    if league_matches < 100:
        flags.append(("warning", "League baseline sample is still small; expected-goal scaling may be noisier."))
    else:
        flags.append(("ok", "League scoring baseline is well populated."))

    has_lineups = bool((prediction.get("lineups", {}) or {}).get("home", {}).get("starters"))
    has_injuries = bool((prediction.get("injuries", {}) or {}).get("home") or (prediction.get("injuries", {}) or {}).get("away"))
    has_xi_boost = bool(xi_impact.get("enabled"))
    if has_lineups or has_injuries or has_xi_boost:
        flags.append(("info", "Local lineup/injury context is included in this report."))
    else:
        flags.append(("info", "No lineup/injury context found locally for this fixture yet."))
    return flags


def _lineups_html_section(home: str, away: str, prediction: Dict[str, Any]) -> str:
    lineups = prediction.get("lineups", {}) or {}
    home_lineup = lineups.get("home", {}) or {}
    away_lineup = lineups.get("away", {}) or {}

    home_starters = home_lineup.get("starters", []) or []
    away_starters = away_lineup.get("starters", []) or []
    if not home_starters and not away_starters:
        return ""

    def render_players(players: List[Dict[str, Any]], limit: int) -> str:
        if not players:
            return "<li>Not available</li>"
        return "\n".join(
            f"<li>{html.escape(str(p.get('player_name') or 'Unknown'))}"
            f" (#{html.escape(str(p.get('player_number') or '?'))})"
            f" - {html.escape(str(p.get('player_pos') or '?'))}</li>"
            for p in players[:limit]
        )

    home_subs = home_lineup.get("substitutes", []) or []
    away_subs = away_lineup.get("substitutes", []) or []

    return f"""<section class=\"card span-6\">
        <h3>Projected Starting XI</h3>
        <div class=\"split\">
          <div>
            <h4>{home} ({html.escape(str(home_lineup.get('formation') or '?'))})</h4>
            <ul>{render_players(home_starters, 11)}</ul>
          </div>
          <div>
            <h4>{away} ({html.escape(str(away_lineup.get('formation') or '?'))})</h4>
            <ul>{render_players(away_starters, 11)}</ul>
          </div>
        </div>
      </section>

      <section class=\"card span-6\">
        <h3>Key Bench Players</h3>
        <div class=\"split\">
          <div>
            <h4>{home}</h4>
            <ul>{render_players(home_subs, 5)}</ul>
          </div>
          <div>
            <h4>{away}</h4>
            <ul>{render_players(away_subs, 5)}</ul>
          </div>
        </div>
      </section>"""


def _injuries_html_section(home: str, away: str, prediction: Dict[str, Any]) -> str:
    injuries = prediction.get("injuries", {}) or {}
    home_injuries = injuries.get("home", []) or []
    away_injuries = injuries.get("away", []) or []

    if not home_injuries and not away_injuries:
        return ""

    def render_injuries(rows: List[Dict[str, Any]]) -> str:
        if not rows:
            return "<li>None reported</li>"
        return "\n".join(
            f"<li>{html.escape(str(r.get('player_name') or 'Unknown'))}"
            f" - {html.escape(str(r.get('injury_reason') or r.get('injury_type') or 'Unavailable'))}"
            f" (Return: {html.escape(str(r.get('return_date') or 'TBD'))})</li>"
            for r in rows[:8]
        )

    return f"""<section class=\"card span-8\">
        <h3>Injury & Suspension Status</h3>
        <div class=\"split\">
          <div>
            <h4>{home} Absentees</h4>
            <ul>{render_injuries(home_injuries)}</ul>
          </div>
          <div>
            <h4>{away} Absentees</h4>
            <ul>{render_injuries(away_injuries)}</ul>
          </div>
        </div>
      </section>"""


def write_prediction_html_report(prediction: Dict[str, Any], output_path: Path) -> Path:
    home = html.escape(str(prediction.get("home_team") or "Home"))
    away = html.escape(str(prediction.get("away_team") or "Away"))
    league = html.escape(str(prediction.get("league_name") or prediction.get("league_code") or "League"))
    as_of = html.escape(str(prediction.get("as_of_utc") or ""))
    season_year = html.escape(str(prediction.get("season_year") or ""))

    pred = prediction.get("prediction", {}) or {}
    expected = prediction.get("expected_goals", {}) or {}
    form = prediction.get("current_form", {}) or {}
    h2h = prediction.get("head_to_head", {}) or {}
    league_avg = prediction.get("league_averages", {}) or {}
    xi_impact = prediction.get("xi_impact", {}) or {}

    home_win_pct = _pct(pred.get("home_win_probability"))
    draw_pct = _pct(pred.get("draw_probability"))
    away_win_pct = _pct(pred.get("away_win_probability"))
    outcome = _outcome_label(str(pred.get("predicted_outcome") or "draw"), home, away)

    ex_home = _safe_float(expected.get("home"))
    ex_away = _safe_float(expected.get("away"))
    ex_max = max(ex_home, ex_away, 0.1)
    home_xg_width = max(6.0, round((ex_home / ex_max) * 100.0, 1))
    away_xg_width = max(6.0, round((ex_away / ex_max) * 100.0, 1))
    xi_enabled = bool(xi_impact.get("enabled"))
    home_xi_strength = _safe_float(xi_impact.get("home_strength"))
    away_xi_strength = _safe_float(xi_impact.get("away_strength"))
    xi_delta = _safe_float(xi_impact.get("strength_delta"))
    xi_home_multiplier = _safe_float(xi_impact.get("home_multiplier") or 1.0)
    xi_away_multiplier = _safe_float(xi_impact.get("away_multiplier") or 1.0)
    xi_weight = _safe_float(xi_impact.get("xi_boost_weight"))

    home_split = form.get("home_team_home_split", {}) or {}
    away_split = form.get("away_team_away_split", {}) or {}
    h2h_overall = h2h.get("overall", {}) or {}
    h2h_venue = h2h.get("venue_specific", {}) or {}

    insights = "\n".join(f"<li>{html.escape(line)}</li>" for line in _insights(prediction))
    completeness = "\n".join(
        f"<li class=\"{kind}\">{html.escape(text)}</li>" for kind, text in _completeness_flags(prediction)
    )

    lineups_html = _lineups_html_section(home, away, prediction)
    injuries_html = _injuries_html_section(home, away, prediction)

    html_doc = f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"UTF-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" />
  <title>Prediction Report: {home} vs {away}</title>
  <style>
    :root {{
      --bg-a: #071c24;
      --bg-b: #1c1140;
      --panel: #0f2e37;
      --ink: #f4f6ff;
      --muted: #b8c3dd;
      --line: #2a4961;
      --ok: #22c55e;
      --warn: #f59e0b;
      --info: #7dd3fc;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      color: var(--ink);
      font-family: \"Poppins\", \"Segoe UI\", sans-serif;
      background:
        radial-gradient(circle at 15% 10%, rgba(45, 212, 191, 0.15), transparent 30%),
        radial-gradient(circle at 85% 15%, rgba(167, 139, 250, 0.2), transparent 35%),
        linear-gradient(145deg, var(--bg-a), var(--bg-b));
      min-height: 100vh;
    }}
    .wrap {{ max-width: 1240px; margin: 0 auto; padding: 24px 16px 36px; }}
    .hero {{
      background: linear-gradient(145deg, rgba(45,212,191,0.08), rgba(167,139,250,0.14));
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 20px;
    }}
    h1 {{ margin: 0 0 8px; font-size: 30px; }}
    .sub {{ margin: 0; color: var(--muted); font-size: 14px; }}
    .grid {{ display: grid; grid-template-columns: repeat(12, 1fr); gap: 14px; margin-top: 14px; }}
    .card {{
      grid-column: span 12;
      background: linear-gradient(145deg, rgba(15,46,55,0.95), rgba(33,25,75,0.9));
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 14px;
    }}
    .span-4 {{ grid-column: span 4; }}
    .span-6 {{ grid-column: span 6; }}
    .span-8 {{ grid-column: span 8; }}
    .kpi {{ font-size: 30px; font-weight: 700; margin: 8px 0 0; }}
    .kpi-note {{ color: var(--muted); font-size: 13px; }}
    .prob-bar {{
      height: 16px;
      border-radius: 999px;
      background: #0b2230;
      border: 1px solid #1e3a4f;
      overflow: hidden;
      display: flex;
      margin-top: 8px;
    }}
    .prob-home {{ background: linear-gradient(90deg, #14b8a6, #2dd4bf); }}
    .prob-draw {{ background: linear-gradient(90deg, #8b5cf6, #a78bfa); }}
    .prob-away {{ background: linear-gradient(90deg, #d946ef, #e879f9); }}
    .xg-row {{ margin-top: 10px; }}
    .xg-label {{ display: flex; justify-content: space-between; font-size: 13px; margin-bottom: 4px; }}
    .xg-track {{ height: 12px; border-radius: 999px; background: #0a1d2b; border: 1px solid #1f3850; overflow: hidden; }}
    .xg-fill-home {{ height: 100%; background: linear-gradient(90deg, #14b8a6, #2dd4bf); }}
    .xg-fill-away {{ height: 100%; background: linear-gradient(90deg, #a855f7, #e879f9); }}
    table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    th, td {{ text-align: left; padding: 8px; border-bottom: 1px solid #29415a; }}
    th {{ color: #d8e2ff; font-weight: 600; background: rgba(8, 27, 42, 0.5); }}
    ul {{ margin: 8px 0 0; padding-left: 20px; }}
    li {{ margin-bottom: 6px; color: #e8edff; }}
    li.ok {{ color: #b7f7ce; }}
    li.warning {{ color: #fde68a; }}
    li.info {{ color: #c7f0ff; }}
    .split {{ display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }}
    .chip {{
      display: inline-block;
      padding: 3px 8px;
      border-radius: 999px;
      border: 1px solid #2a5068;
      font-size: 12px;
      color: #ddf6ff;
      margin-right: 6px;
      margin-top: 6px;
    }}
    @media (max-width: 980px) {{
      .span-4, .span-6, .span-8 {{ grid-column: span 12; }}
      h1 {{ font-size: 24px; }}
      .split {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <div class=\"wrap\">
    <section class=\"hero\">
      <h1>{home} vs {away}</h1>
      <p class=\"sub\">{league} | Season {season_year} | Generated from local data only at {as_of}</p>
      <span class=\"chip\">Predicted Outcome: {html.escape(outcome)}</span>
      <span class=\"chip\">Expected Goals: {ex_home:.2f} - {ex_away:.2f}</span>
    </section>

    <div class=\"grid\">
      <section class=\"card span-4\">
        <h3>Outcome Probabilities</h3>
        <div class=\"kpi\">{html.escape(outcome)}</div>
        <div class=\"kpi-note\">Home {home_win_pct:.1f}% | Draw {draw_pct:.1f}% | Away {away_win_pct:.1f}%</div>
        <div class=\"prob-bar\" aria-label=\"Outcome probability bar\">
          <div class=\"prob-home\" style=\"width:{home_win_pct:.1f}%\"></div>
          <div class=\"prob-draw\" style=\"width:{draw_pct:.1f}%\"></div>
          <div class=\"prob-away\" style=\"width:{away_win_pct:.1f}%\"></div>
        </div>
      </section>

      <section class=\"card span-4\">
        <h3>Expected Goals</h3>
        <div class=\"xg-row\">
          <div class=\"xg-label\"><span>{home}</span><strong>{ex_home:.2f}</strong></div>
          <div class=\"xg-track\"><div class=\"xg-fill-home\" style=\"width:{home_xg_width:.1f}%\"></div></div>
        </div>
        <div class=\"xg-row\">
          <div class=\"xg-label\"><span>{away}</span><strong>{ex_away:.2f}</strong></div>
          <div class=\"xg-track\"><div class=\"xg-fill-away\" style=\"width:{away_xg_width:.1f}%\"></div></div>
        </div>
      </section>

      <section class=\"card span-4\">
        <h3>Completeness Snapshot</h3>
        <div class=\"kpi\">{_safe_int(league_avg.get('matches'))}</div>
        <div class=\"kpi-note\">League finished fixtures used for baseline</div>
        <ul>{completeness}</ul>
      </section>

      <section class=\"card span-6\">
        <h3>Top Scoreline Grid</h3>
        <table>
          <thead><tr><th>Score</th><th>Probability</th></tr></thead>
          <tbody>{_top_score_rows(prediction)}</tbody>
        </table>
      </section>

      <section class="card span-4">
        <h3>Projected XI Strength</h3>
        <div class="kpi">{"On" if xi_enabled else "Off"}</div>
        <div class="kpi-note">Weight {xi_weight:.3f} | Delta {xi_delta:+.3f}</div>
        <p class="sub">{home}: strength {home_xi_strength:.3f} | xG multiplier {xi_home_multiplier:.3f}</p>
        <p class="sub">{away}: strength {away_xi_strength:.3f} | xG multiplier {xi_away_multiplier:.3f}</p>
      </section>

      <section class=\"card span-6\">
        <h3>Form and Head-to-Head Stats</h3>
        <div class=\"split\">
          <div>
            <h4>{home} Home Split</h4>
            <p class=\"sub\">Played {_safe_int(home_split.get('played'))} | W {_safe_int(home_split.get('wins'))} D {_safe_int(home_split.get('draws'))} L {_safe_int(home_split.get('losses'))}</p>
            <p class=\"sub\">GF {_safe_int(home_split.get('goals_for'))} | GA {_safe_int(home_split.get('goals_against'))}</p>
          </div>
          <div>
            <h4>{away} Away Split</h4>
            <p class=\"sub\">Played {_safe_int(away_split.get('played'))} | W {_safe_int(away_split.get('wins'))} D {_safe_int(away_split.get('draws'))} L {_safe_int(away_split.get('losses'))}</p>
            <p class=\"sub\">GF {_safe_int(away_split.get('goals_for'))} | GA {_safe_int(away_split.get('goals_against'))}</p>
          </div>
        </div>
        <hr style=\"border:0;border-top:1px solid #2a4961;margin:12px 0\" />
        <p class=\"sub\">Overall H2H: {_safe_int(h2h_overall.get('matches'))} matches | Home-team wins {_safe_int(h2h_overall.get('home_team_wins'))} | Draws {_safe_int(h2h_overall.get('draws'))} | Away-team wins {_safe_int(h2h_overall.get('away_team_wins'))}</p>
        <p class=\"sub\">Venue H2H: {_safe_int(h2h_venue.get('matches'))} matches | Goals {_safe_int(h2h_venue.get('home_team_goals'))}-{_safe_int(h2h_venue.get('away_team_goals'))}</p>
      </section>

      <section class=\"card span-8\">
        <h3>Recent Head-to-Head Matches</h3>
        <table>
          <thead><tr><th>Date</th><th>Home</th><th>Away</th><th>Score</th><th>Result</th></tr></thead>
          <tbody>{_recent_h2h_rows(prediction)}</tbody>
        </table>
      </section>

      {lineups_html}

      {injuries_html}

      <section class=\"card span-4\">
        <h3>Analyst Notes</h3>
        <ul>{insights}</ul>
        <h3 style=\"margin-top:14px\">Method</h3>
        <p class="sub">Expected goals blend league baseline rates, current split form, venue head-to-head weighting, and optional projected-XI and injury multipliers.</p>
      </section>
    </div>
  </div>
</body>
</html>
"""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html_doc, encoding="utf-8")
    return output_path
