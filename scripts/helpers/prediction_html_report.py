from __future__ import annotations

import base64
import colorsys
import html
import io
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _slugify(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-") or "unknown"


def _team_name_key(value: str) -> str:
  value = value.strip().lower()
  return re.sub(r"[^a-z0-9]+", "", value)


NEUTRAL_AWAY_TEAM_KEYS = {
  "juventus",
  "udinese",
}


def _safe_int(value: Any) -> int:
    return int(value or 0)


def _safe_float(value: Any) -> float:
    return float(value or 0.0)


def _pct(value: Any) -> float:
    return round(100.0 * _safe_float(value), 1)


def _dec_odds(pct: float) -> str:
    """Convert a percentage probability to decimal odds string, e.g. 40.0 -> '2.50'."""
    if pct <= 0:
        return "—"
    return f"{100.0 / pct:.2f}"


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


def _clamp_color(v: float) -> int:
    return max(0, min(255, int(round(v))))


def _rgb_to_hex(rgb: Tuple[int, int, int]) -> str:
    r, g, b = rgb
    return f"#{r:02x}{g:02x}{b:02x}"


def _rgb_to_css(rgb: Tuple[int, int, int]) -> str:
    r, g, b = rgb
    return f"{r}, {g}, {b}"


def _tune_rgb(rgb: Tuple[int, int, int], *, sat_mul: float = 1.0, val_mul: float = 1.0) -> Tuple[int, int, int]:
    r, g, b = rgb
    h, s, v = colorsys.rgb_to_hsv(r / 255.0, g / 255.0, b / 255.0)
    s = max(0.0, min(1.0, s * sat_mul))
    v = max(0.0, min(1.0, v * val_mul))
    rr, gg, bb = colorsys.hsv_to_rgb(h, s, v)
    return (_clamp_color(rr * 255.0), _clamp_color(gg * 255.0), _clamp_color(bb * 255.0))


def _mix_rgb(a: Tuple[int, int, int], b: Tuple[int, int, int], weight: float) -> Tuple[int, int, int]:
    w = max(0.0, min(1.0, weight))
    return (
        _clamp_color((a[0] * (1.0 - w)) + (b[0] * w)),
        _clamp_color((a[1] * (1.0 - w)) + (b[1] * w)),
        _clamp_color((a[2] * (1.0 - w)) + (b[2] * w)),
    )


def _normalize_theme_base(rgb: Tuple[int, int, int]) -> Tuple[int, int, int]:
    r, g, b = rgb
    h, s, v = colorsys.rgb_to_hsv(r / 255.0, g / 255.0, b / 255.0)
    is_neutral = s < 0.10
    # Keep identity but avoid near-black themes that flatten the UI.
    if v < 0.28:
        v = 0.28
    if (not is_neutral) and s < 0.22 and v < 0.40:
        s = 0.35
    if is_neutral:
        s = 0.0
    rr, gg, bb = colorsys.hsv_to_rgb(h, s, v)
    return (_clamp_color(rr * 255.0), _clamp_color(gg * 255.0), _clamp_color(bb * 255.0))


def _rgb_distance(a: Tuple[int, int, int], b: Tuple[int, int, int]) -> float:
    dr = float(a[0] - b[0])
    dg = float(a[1] - b[1])
    db = float(a[2] - b[2])
    return (dr * dr + dg * dg + db * db) ** 0.5


def _relative_luminance(rgb: Tuple[int, int, int]) -> float:
    def channel(v: int) -> float:
      x = v / 255.0
      return x / 12.92 if x <= 0.03928 else ((x + 0.055) / 1.055) ** 2.4

    r, g, b = rgb
    return (0.2126 * channel(r)) + (0.7152 * channel(g)) + (0.0722 * channel(b))


def _contrast_ratio(a: Tuple[int, int, int], b: Tuple[int, int, int]) -> float:
    la = _relative_luminance(a)
    lb = _relative_luminance(b)
    hi, lo = (la, lb) if la >= lb else (lb, la)
    return (hi + 0.05) / (lo + 0.05)


def _ensure_contrast(rgb: Tuple[int, int, int], *, bg: Tuple[int, int, int], min_ratio: float) -> Tuple[int, int, int]:
    if _contrast_ratio(rgb, bg) >= min_ratio:
        return rgb

    r, g, b = rgb
    h, s, v = colorsys.rgb_to_hsv(r / 255.0, g / 255.0, b / 255.0)
    keep_neutral = s < 0.10
    for _ in range(10):
        v = min(1.0, v + 0.07)
        if not keep_neutral:
            s = min(1.0, s + 0.03)
        else:
            s = 0.0
        rr, gg, bb = colorsys.hsv_to_rgb(h, s, v)
        candidate = (_clamp_color(rr * 255.0), _clamp_color(gg * 255.0), _clamp_color(bb * 255.0))
        if _contrast_ratio(candidate, bg) >= min_ratio:
            return candidate
    return (_clamp_color(rr * 255.0), _clamp_color(gg * 255.0), _clamp_color(bb * 255.0))


def _is_neutral(rgb: Tuple[int, int, int], sat_threshold: float = 0.22) -> bool:
    r, g, b = rgb
    _, sat, _ = colorsys.rgb_to_hsv(r / 255.0, g / 255.0, b / 255.0)
    return sat <= sat_threshold


def _value_component(rgb: Tuple[int, int, int]) -> float:
    r, g, b = rgb
    _, _, val = colorsys.rgb_to_hsv(r / 255.0, g / 255.0, b / 255.0)
    return val


def _text_color_for_bg(rgb: Tuple[int, int, int]) -> str:
    # Choose dark or light ink based on stronger contrast for accent surfaces.
    dark_ink = (9, 12, 22)
    light_ink = (244, 248, 255)
    if _contrast_ratio(rgb, dark_ink) >= _contrast_ratio(rgb, light_ink):
      return "#090c16"
    return "#f4f8ff"


def _rotate_hue(rgb: Tuple[int, int, int], degrees: float) -> Tuple[int, int, int]:
    r, g, b = rgb
    h, s, v = colorsys.rgb_to_hsv(r / 255.0, g / 255.0, b / 255.0)
    h = (h + (degrees / 360.0)) % 1.0
    rr, gg, bb = colorsys.hsv_to_rgb(h, s, v)
    return (_clamp_color(rr * 255.0), _clamp_color(gg * 255.0), _clamp_color(bb * 255.0))


def _extract_badge_palette(badge_data_uri: str, limit: int = 6) -> List[Tuple[int, int, int]]:
    if not badge_data_uri or "," not in badge_data_uri:
      return []
    if not badge_data_uri.startswith("data:image"):
      return []

    try:
      encoded = badge_data_uri.split(",", 1)[1]
      image_bytes = base64.b64decode(encoded)
    except Exception:
      return []

    try:
      from PIL import Image
    except Exception:
      return []

    try:
      img = Image.open(io.BytesIO(image_bytes)).convert("RGBA")
      img.thumbnail((72, 72))
      colors = img.getcolors(maxcolors=72 * 72)
      if not colors:
        return []

      ranked: List[Tuple[float, Tuple[int, int, int]]] = []
      for count, (r, g, b, a) in colors:
        if a < 64:
          continue
        _, sat, val = colorsys.rgb_to_hsv(r / 255.0, g / 255.0, b / 255.0)
        # Ignore near-white/near-gray pixels that are usually transparent/logo padding.
        if sat < 0.12 and val > 0.82:
          continue
        # Prefer visible and vivid colors but still allow dark badge tones.
        score = float(count) * (0.35 + sat) * (0.55 + val)
        ranked.append((score, (r, g, b)))

      if not ranked:
        return []

      ranked.sort(key=lambda x: x[0], reverse=True)
      palette: List[Tuple[int, int, int]] = []
      for _, rgb in ranked:
        if any(_rgb_distance(rgb, existing) < 42.0 for existing in palette):
          continue
        palette.append(rgb)
        if len(palette) >= limit:
          break
      return palette
    except Exception:
      return []


def _pick_away_color(
    home_base: Tuple[int, int, int],
    away_palette: List[Tuple[int, int, int]],
    *,
    prefer_neutral: bool,
) -> Tuple[int, int, int]:
    if not away_palette:
        return _rotate_hue(home_base, 145.0)

    primary = away_palette[0]
    neutrals = [c for c in away_palette if _is_neutral(c)]
    vivid = [c for c in away_palette if not _is_neutral(c, sat_threshold=0.28)]

    if prefer_neutral and neutrals:
        distinct_neutrals = [c for c in neutrals if _rgb_distance(home_base, c) >= 58.0]
        if distinct_neutrals:
            dark_neutrals = [c for c in distinct_neutrals if _value_component(c) <= 0.45]
            if dark_neutrals:
                dark_neutrals.sort(key=lambda c: _rgb_distance(home_base, c), reverse=True)
                return dark_neutrals[0]
            distinct_neutrals.sort(key=lambda c: _rgb_distance(home_base, c), reverse=True)
            return distinct_neutrals[0]

    # For non-neutral clubs, if primary is neutral prefer a vivid secondary.
    if (not prefer_neutral) and _is_neutral(primary) and vivid:
        vivid_distinct = [c for c in vivid if _rgb_distance(home_base, c) >= 58.0]
        if vivid_distinct:
            vivid_distinct.sort(key=lambda c: _rgb_distance(home_base, c), reverse=True)
            return vivid_distinct[0]

    if _rgb_distance(home_base, primary) >= 58.0:
        return primary

    for candidate in away_palette[1:]:
        if _rgb_distance(home_base, candidate) >= 58.0:
            return candidate

    return _rotate_hue(primary, 125.0)


def _theme_from_badges(
    home_badge_b64: str,
    away_badge_b64: str,
    *,
    away_team_name: str = "",
) -> Dict[str, str]:
    bg = (17, 24, 39)
    home_palette = _extract_badge_palette(home_badge_b64)
    away_palette = _extract_badge_palette(away_badge_b64)
    away_key = _team_name_key(away_team_name)
    prefer_neutral = away_key in NEUTRAL_AWAY_TEAM_KEYS

    home_base = home_palette[0] if home_palette else (20, 184, 166)
    away_base = (
        _pick_away_color(home_base, away_palette, prefer_neutral=prefer_neutral)
        if away_palette
        else (139, 92, 246)
    )

    home_base = _normalize_theme_base(home_base)
    away_base = _normalize_theme_base(away_base)

    # Keep colors vivid enough for gradients while preserving the badge identity.
    home_base = _tune_rgb(home_base, sat_mul=1.12, val_mul=0.96)
    away_base = _tune_rgb(away_base, sat_mul=1.12, val_mul=0.96)
    home_base = _ensure_contrast(home_base, bg=bg, min_ratio=2.2)
    away_base = _ensure_contrast(away_base, bg=bg, min_ratio=2.2)

    home_hi = _tune_rgb(home_base, sat_mul=0.95, val_mul=1.20)
    away_hi = _tune_rgb(away_base, sat_mul=0.95, val_mul=1.20)
    away_deep = _tune_rgb(away_base, sat_mul=1.08, val_mul=0.78)

    draw_base = _mix_rgb(home_base, away_base, 0.5)
    draw_base = _tune_rgb(draw_base, sat_mul=0.65, val_mul=0.90)
    draw_base = _ensure_contrast(draw_base, bg=bg, min_ratio=2.0)
    draw_hi = _tune_rgb(draw_base, sat_mul=0.90, val_mul=1.20)

    accent_mix = _mix_rgb(home_base, away_base, 0.5)
    accent_ink = _text_color_for_bg(accent_mix)

    return {
        "home": _rgb_to_hex(home_base),
        "home_hi": _rgb_to_hex(home_hi),
        "away": _rgb_to_hex(away_base),
        "away_hi": _rgb_to_hex(away_hi),
        "away_deep": _rgb_to_hex(away_deep),
        "draw": _rgb_to_hex(draw_base),
        "draw_hi": _rgb_to_hex(draw_hi),
        "home_rgb": _rgb_to_css(home_base),
        "away_rgb": _rgb_to_css(away_base),
        "accent_ink": accent_ink,
    }


def _betting_markets_html(home: str, away: str, prediction: Dict[str, Any]) -> str:
    pred = prediction.get("prediction", {}) or {}
    markets = pred.get("markets", {}) or {}

    hw = _safe_float(pred.get("home_win_probability"))
    dp = _safe_float(pred.get("draw_probability"))
    aw = _safe_float(pred.get("away_win_probability"))

    over25  = _safe_float(markets.get("over_25"))
    under25 = _safe_float(markets.get("under_25"))
    over35  = _safe_float(markets.get("over_35"))
    under35 = _safe_float(markets.get("under_35"))
    gg      = _safe_float(markets.get("gg"))
    ng      = _safe_float(markets.get("ng"))

    # double-chance
    hx = min(hw + dp, 1.0)
    h2 = min(hw + aw, 1.0)
    x2 = min(dp + aw, 1.0)

    def row(label: str, prob: float, sublabel: str = "") -> str:
        pct = prob * 100
        sub = f'<span style="font-size:11px;color:#7a8ab0;margin-left:6px">{html.escape(sublabel)}</span>' if sublabel else ""
        return (
            f'<tr><td>{html.escape(label)}{sub}</td>'
            f'<td><span class="odds-tag">{_dec_odds(pct)}</span></td>'
            f'<td>{pct:.1f}%</td></tr>'
        )

    def section(title: str, rows: str) -> str:
        return (
            f'<div class="mkt-section">'
            f'<div class="mkt-title">{html.escape(title)}</div>'
            f'<table><thead><tr><th>Selection</th><th>Odds</th><th>%</th></tr></thead>'
            f'<tbody>{rows}</tbody></table>'
            f'</div>'
        )

    s1 = section(f"1 · X · 2",
        row(f"1 – {home} Win", hw) +
        row("X – Draw",        dp) +
        row(f"2 – {away} Win",  aw))

    s2 = section("Double Chance",
        row(f"1X – {home} or Draw",  hx) +
        row(f"12 – {home} or {away}", h2) +
        row(f"X2 – Draw or {away}",   x2))

    s3 = section("Over / Under 2.5",
      row("Over 2.5",  over25) +
      row("Under 2.5", under25) +
      row("Over 3.5",  over35) +
      row("Under 3.5", under35))

    s4 = section("Both Teams to Score (GG/NG)",
      row("GG – Both Score", gg) +
      row("NG – Not Both Score", ng))

    return f"""<section class="card span-12" style="margin-top:4px">
  <h3 class=\"section-title\"><span class=\"section-icon\">🎯</span>Betting Markets</h3>
  <div class="mkt-grid">
    {s1}{s2}{s3}{s4}
  </div>
</section>"""


def _executive_summary_html(home: str, away: str, prediction: Dict[str, Any]) -> str:
    pred = prediction.get("prediction", {}) or {}
    markets = pred.get("markets", {}) or {}

    hw = _safe_float(pred.get("home_win_probability"))
    dp = _safe_float(pred.get("draw_probability"))
    aw = _safe_float(pred.get("away_win_probability"))
    over25 = _safe_float(markets.get("over_25"))
    under25 = _safe_float(markets.get("under_25"))
    gg = _safe_float(markets.get("gg"))
    ng = _safe_float(markets.get("ng"))

    one_x_two = [
        (f"1 - {home} Win", hw),
        ("X - Draw", dp),
        (f"2 - {away} Win", aw),
    ]
    best_1x2_label, best_1x2_prob = max(one_x_two, key=lambda x: x[1])

    totals = [
        ("Over 2.5", over25),
        ("Under 2.5", under25),
        ("GG", gg),
        ("NG", ng),
    ]
    safest_totals_label, safest_totals_prob = max(totals, key=lambda x: x[1])
    risk_note = "High draw volatility" if dp >= 0.30 else "Balanced volatility"

    return f"""<section class=\"card span-12\">
  <h3 class=\"section-title\"><span class=\"section-icon\">🧭</span>Executive Summary</h3>
  <div class=\"summary-grid\">
    <div>
      <div class=\"kpi-note\">Best 1X2 Lean</div>
      <div class=\"summary-line\"><strong>{html.escape(best_1x2_label)}</strong> <span class=\"odds-tag\">{_dec_odds(best_1x2_prob * 100):s}</span> ({best_1x2_prob * 100:.1f}%)</div>
    </div>
    <div>
      <div class=\"kpi-note\">Safest Totals/BTTS</div>
      <div class=\"summary-line\"><strong>{html.escape(safest_totals_label)}</strong> <span class=\"odds-tag\">{_dec_odds(safest_totals_prob * 100):s}</span> ({safest_totals_prob * 100:.1f}%)</div>
    </div>
    <div>
      <div class=\"kpi-note\">Main Risk Flag</div>
      <div class=\"summary-line\"><strong>{risk_note}</strong> (Draw {dp * 100:.1f}%)</div>
    </div>
  </div>
</section>"""


def _top_score_rows(prediction: Dict[str, Any]) -> str:
    rows = prediction.get("prediction", {}).get("score_grid", []) or []
    html_rows: List[str] = []
    for item in rows[:10]:
        hg = _safe_int(item.get("home_goals"))
        ag = _safe_int(item.get("away_goals"))
        prob = _pct(item.get("probability"))
        html_rows.append(
            f"<tr><td>{hg}-{ag}</td>"
            f"<td><span class=\"odds-tag\">{_dec_odds(prob)}</span>{prob:.1f}%</td></tr>"
        )
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
      <h3 class=\"section-title\"><span class=\"section-icon\">🧩</span>Projected Starting XI</h3>
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
        <h3 class=\"section-title\"><span class=\"section-icon\">🪑</span>Key Bench Players</h3>
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
      <h3 class=\"section-title\"><span class=\"section-icon\">🚑</span>Injury & Suspension Status</h3>
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
    home_name = str(prediction.get("home_team") or "Home")
    away_name = str(prediction.get("away_team") or "Away")
    home = html.escape(home_name)
    away = html.escape(away_name)
    league = html.escape(str(prediction.get("league_name") or prediction.get("league_code") or "League"))
    as_of = html.escape(str(prediction.get("as_of_utc") or ""))
    season_year = html.escape(str(prediction.get("season_year") or ""))
    fixture_context = prediction.get("fixture_context", {}) or {}
    match_date = str(fixture_context.get("fixture_date_utc") or "")
    match_date_label = html.escape(match_date.replace("T", " ").replace("+00:00", " UTC") if match_date else "")

    pred = prediction.get("prediction", {}) or {}
    expected = prediction.get("expected_goals", {}) or {}
    form = prediction.get("current_form", {}) or {}
    h2h = prediction.get("head_to_head", {}) or {}
    league_avg = prediction.get("league_averages", {}) or {}
    xi_impact = prediction.get("xi_impact", {}) or {}

    home_badge_b64 = str(prediction.get("home_badge_b64") or "")
    away_badge_b64 = str(prediction.get("away_badge_b64") or "")
    theme = _theme_from_badges(home_badge_b64, away_badge_b64, away_team_name=away_name)

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
    betting_html = _betting_markets_html(home, away, prediction)
    executive_summary_html = _executive_summary_html(home, away, prediction)
    team_news_block = ""
    if lineups_html or injuries_html:
      team_news_block = f"""
      <div class=\"section-kicker\">Team News</div>
      {lineups_html}
      {injuries_html}
  """

    html_doc = f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"UTF-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" />
  <title>Prediction Report: {home} vs {away}</title>
  <style>
    :root {{
      --bg-a: #0a0e1a;
      --bg-b: #0d1a2a;
      --panel: #111827;
      --ink: #f0f4ff;
      --muted: #9aa8c8;
      --line: #2a3655;
      --ok: #22c55e;
      --warn: #f59e0b;
      --info: #7dd3fc;
      --teal: {theme['home']};
      --teal-hi: {theme['home_hi']};
      --purple: {theme['away']};
      --purple-hi: {theme['away_hi']};
      --purple-deep: {theme['away_deep']};
      --draw: {theme['draw']};
      --draw-hi: {theme['draw_hi']};
      --home-rgb: {theme['home_rgb']};
      --away-rgb: {theme['away_rgb']};
      --accent-ink: {theme['accent_ink']};
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      color: var(--ink);
      font-family: "Poppins", "Segoe UI", sans-serif;
      background:
        radial-gradient(ellipse at 10% 0%,   rgba(var(--home-rgb),0.24), transparent 40%),
        radial-gradient(ellipse at 90% 5%,   rgba(var(--away-rgb),0.24), transparent 40%),
        radial-gradient(ellipse at 50% 90%,  rgba(var(--away-rgb),0.12), transparent 50%),
        linear-gradient(160deg, #080d18 0%, #0d1a2a 100%);
      min-height: 100vh;
    }}
    .wrap {{ max-width: 1240px; margin: 0 auto; padding: 0 16px 36px; }}
    /* matchup header */
    .matchup-header {{
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 12px;
      padding: 28px 24px;
      background: linear-gradient(135deg, rgba(var(--home-rgb),0.16) 0%, rgba(var(--away-rgb),0.16) 100%);
      border-bottom: 2px solid transparent;
      border-image: linear-gradient(90deg, rgba(var(--home-rgb),0.70), rgba(var(--away-rgb),0.70)) 1;
      margin-bottom: 20px;
    }}
    .team-side {{
      display: flex;
      align-items: center;
      gap: 14px;
      flex: 1;
    }}
    .team-side.home {{ justify-content: flex-end; }}
    .team-side.away {{ justify-content: flex-start; }}
    .team-name {{
      font-size: 22px;
      font-weight: 700;
      letter-spacing: 0.01em;
      color: var(--ink);
    }}
    .badge {{
      width: 60px;
      height: 60px;
      object-fit: contain;
      filter: drop-shadow(0 2px 6px rgba(0,0,0,0.5));
    }}
    .vs-divider {{
      font-size: 28px;
      font-weight: 800;
      background: linear-gradient(135deg, var(--teal-hi), var(--purple-hi));
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      background-clip: text;
      padding: 0 8px;
      flex-shrink: 0;
    }}
    /* meta strip */
    .meta-strip {{
      background: rgba(10,14,26,0.75);
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 12px 20px;
      margin-bottom: 14px;
      display: flex;
      align-items: center;
      flex-wrap: wrap;
      gap: 10px;
    }}
    .meta-strip p {{ margin: 0; color: var(--muted); font-size: 14px; }}
    .section-kicker {{
      grid-column: span 12;
      margin: 2px 0 -2px;
      color: #dbe7ff;
      font-size: 12px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      opacity: 0.9;
    }}
    h1 {{ margin: 0 0 8px; font-size: 30px; }}
    .sub {{ margin: 0; color: var(--muted); font-size: 14px; }}
    .grid {{ display: grid; grid-template-columns: repeat(12, 1fr); gap: 14px; margin-top: 14px; }}
    .card {{
      grid-column: span 12;
      background: linear-gradient(145deg, rgba(13,22,42,0.97), rgba(10,16,32,0.95));
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 14px;
    }}
    .section-title {{
      display: flex;
      align-items: center;
      gap: 8px;
      margin: 0;
    }}
    .section-icon {{
      width: 24px;
      height: 24px;
      border-radius: 7px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      font-size: 13px;
      line-height: 1;
      background: linear-gradient(135deg, rgba(var(--home-rgb),0.18), rgba(var(--away-rgb),0.18));
      border: 1px solid rgba(var(--away-rgb),0.40);
      box-shadow: inset 0 0 0 1px rgba(255,255,255,0.06);
      color: var(--accent-ink);
    }}
    .span-4 {{ grid-column: span 4; }}
    .span-6 {{ grid-column: span 6; }}
    .span-8 {{ grid-column: span 8; }}
    .kpi {{ font-size: 30px; font-weight: 700; margin: 8px 0 0; }}
    .kpi-note {{ color: var(--muted); font-size: 13px; }}
    .prob-bar {{
      height: 16px;
      border-radius: 999px;
      background: #08101e;
      border: 1px solid #1e2e50;
      overflow: hidden;
      display: flex;
      margin-top: 8px;
    }}
    .prob-home {{ background: linear-gradient(90deg, var(--teal), var(--teal-hi)); }}
    .prob-draw {{ background: linear-gradient(90deg, var(--draw), var(--draw-hi)); }}
    .prob-away {{ background: linear-gradient(90deg, var(--purple-deep), var(--purple)); }}
    .xg-row {{ margin-top: 10px; }}
    .xg-label {{ display: flex; justify-content: space-between; font-size: 13px; margin-bottom: 4px; }}
    .xg-track {{ height: 12px; border-radius: 999px; background: #08101e; border: 1px solid #1e2e50; overflow: hidden; }}
    .xg-fill-home {{ height: 100%; background: linear-gradient(90deg, var(--teal), var(--teal-hi)); }}
    .xg-fill-away {{ height: 100%; background: linear-gradient(90deg, var(--purple), var(--purple-hi)); }}
    table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    th, td {{ text-align: left; padding: 8px; border-bottom: 1px solid #1e2e50; }}
    th {{ color: #d8e2ff; font-weight: 600; background: rgba(8,12,28,0.6); }}
    ul {{ margin: 8px 0 0; padding-left: 20px; }}
    li {{ margin-bottom: 6px; color: #e8edff; }}
    li.ok {{ color: #b7f7ce; }}
    li.warning {{ color: #fde68a; }}
    li.info {{ color: #c7f0ff; }}
    .split {{ display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }}
    .chip {{
      display: inline-block;
      padding: 3px 10px;
      border-radius: 999px;
      background: linear-gradient(135deg, rgba(var(--home-rgb),0.16), rgba(var(--away-rgb),0.16));
      border: 1px solid rgba(var(--away-rgb),0.42);
      font-size: 12px;
      color: var(--accent-ink);
      margin-right: 6px;
      margin-top: 6px;
    }}
    .odds-tag {{
      font-size: 12px;
      font-weight: 700;
      background: linear-gradient(90deg, var(--teal-hi), var(--purple-hi));
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      background-clip: text;
      margin-right: 4px;
    }}
    .mkt-grid {{
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 14px;
      margin-top: 10px;
    }}
    .summary-grid {{
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 12px;
      margin-top: 6px;
    }}
    .summary-line {{
      margin-top: 6px;
      font-size: 14px;
      color: #eaf1ff;
    }}
    .mkt-section {{}}
    .mkt-title {{
      font-size: 13px;
      font-weight: 600;
      color: var(--teal-hi);
      margin-bottom: 6px;
      padding-bottom: 4px;
      border-bottom: 1px solid rgba(var(--home-rgb),0.35);
    }}
    @media (max-width: 980px) {{
      .span-4, .span-6, .span-8 {{ grid-column: span 12; }}
      .team-name {{ font-size: 16px; }}
      .badge {{ width: 44px; height: 44px; }}
      .split {{ grid-template-columns: 1fr; }}
      .mkt-grid {{ grid-template-columns: 1fr 1fr; }}
      .summary-grid {{ grid-template-columns: 1fr; }}
    }}
    @media (max-width: 600px) {{
      .mkt-grid {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <div class=\"wrap\">
    <div class="matchup-header">
      <div class="team-side home">
        <span class="team-name">{home}</span>
        {'<img class="badge" src="' + home_badge_b64 + '" alt="' + home + '" />' if home_badge_b64 else '<div class="badge"></div>'}
      </div>
      <div class="vs-divider">vs</div>
      <div class="team-side away">
        {'<img class="badge" src="' + away_badge_b64 + '" alt="' + away + '" />' if away_badge_b64 else '<div class="badge"></div>'}
        <span class="team-name">{away}</span>
      </div>
    </div>
    <div class="meta-strip">
      <p>{league} &nbsp;|&nbsp; Season {season_year} &nbsp;|&nbsp; Generated at {as_of}</p>
      {'<span class="chip">Match Date: ' + match_date_label + '</span>' if match_date_label else ''}
      <span class="chip">Predicted: {html.escape(outcome)}</span>
      <span class="chip">xG: {ex_home:.2f} – {ex_away:.2f}</span>
    </div>

    <div class=\"grid\">
      {executive_summary_html}
      <div class="section-kicker">Prediction Snapshot</div>
      <section class=\"card span-4\">
        <h3 class="section-title"><span class="section-icon">📈</span>Outcome Probabilities</h3>
        <div class=\"kpi\">{html.escape(outcome)}</div>
        <table style="margin-top:10px;font-size:13px">
          <thead><tr><th>Outcome</th><th>Odds</th><th>Chance</th></tr></thead>
          <tbody>
            <tr><td>{home} Win</td><td><span class="odds-tag">{_dec_odds(home_win_pct)}</span></td><td>{home_win_pct:.1f}%</td></tr>
            <tr><td>Draw</td><td><span class="odds-tag">{_dec_odds(draw_pct)}</span></td><td>{draw_pct:.1f}%</td></tr>
            <tr><td>{away} Win</td><td><span class="odds-tag">{_dec_odds(away_win_pct)}</span></td><td>{away_win_pct:.1f}%</td></tr>
          </tbody>
        </table>
        <div class="prob-bar" aria-label="Outcome probability bar" style="margin-top:10px">
          <div class=\"prob-home\" style=\"width:{home_win_pct:.1f}%\"></div>
          <div class=\"prob-draw\" style=\"width:{draw_pct:.1f}%\"></div>
          <div class=\"prob-away\" style=\"width:{away_win_pct:.1f}%\"></div>
        </div>
      </section>

      <section class=\"card span-4\">
        <h3 class="section-title"><span class="section-icon">⚽</span>Expected Goals</h3>
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
        <h3 class="section-title"><span class="section-icon">🧮</span>Top Scoreline Grid</h3>
        <table>
          <thead><tr><th>Score</th><th>Odds / Chance</th></tr></thead>
          <tbody>{_top_score_rows(prediction)}</tbody>
        </table>
      </section>

      {betting_html}

      <div class="section-kicker">Context and Reliability</div>

      <section class="card span-4">
        <h3 class="section-title"><span class="section-icon">💪</span>Projected XI Strength</h3>
        <div class="kpi">{"On" if xi_enabled else "Off"}</div>
        <div class="kpi-note">Weight {xi_weight:.3f} | Delta {xi_delta:+.3f}</div>
        <p class="sub">{home}: strength {home_xi_strength:.3f} | xG multiplier {xi_home_multiplier:.3f}</p>
        <p class="sub">{away}: strength {away_xi_strength:.3f} | xG multiplier {xi_away_multiplier:.3f}</p>
      </section>

      <section class="card span-4">
        <h3 class="section-title"><span class="section-icon">✅</span>Completeness Snapshot</h3>
        <div class="kpi">{_safe_int(league_avg.get('matches'))}</div>
        <div class="kpi-note">League finished fixtures used for baseline</div>
        <ul>{completeness}</ul>
      </section>

      <section class="card span-4">
        <h3 class="section-title"><span class="section-icon">📝</span>Analyst Notes</h3>
        <ul>{insights}</ul>
        <h3 style="margin-top:14px">Method</h3>
        <p class="sub">Expected goals blend league baseline rates, current split form, venue head-to-head weighting, and optional projected-XI and injury multipliers.</p>
      </section>

      <section class=\"card span-6\">
        <h3 class="section-title"><span class="section-icon">🔎</span>Form and Head-to-Head Stats</h3>
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
        <h3 class="section-title"><span class="section-icon">🕘</span>Recent Head-to-Head Matches</h3>
        <table>
          <thead><tr><th>Date</th><th>Home</th><th>Away</th><th>Score</th><th>Result</th></tr></thead>
          <tbody>{_recent_h2h_rows(prediction)}</tbody>
        </table>
      </section>

      {team_news_block}
    </div>
  </div>
</body>
</html>
"""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html_doc, encoding="utf-8")
    return output_path
