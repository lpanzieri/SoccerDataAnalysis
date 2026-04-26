from __future__ import annotations

import base64
from difflib import SequenceMatcher
import hashlib
import math
import os
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import matplotlib
import matplotlib.pyplot as plt
import mysql.connector
import numpy as np
from scipy.optimize import minimize as _scipy_minimize
from scipy.stats import poisson as _scipy_poisson

matplotlib.use("Agg")  # For headless environments

# Module-level cache for schema capability checks to avoid repeated information_schema queries
_SCHEMA_CAPABILITY_CACHE: Dict[str, bool] = {}

# Module-level cache for decoded badge images (with size limit to prevent unbounded memory growth)
_BADGE_IMAGE_CACHE: Dict[str, Any] = {}
_BADGE_CACHE_MAX_SIZE = 50  # Limit to prevent excessive memory usage


def _clear_schema_cache() -> None:
  """Clear the schema capability cache. Useful for testing or schema changes."""
  global _SCHEMA_CAPABILITY_CACHE
  _SCHEMA_CAPABILITY_CACHE.clear()


def _clear_badge_cache() -> None:
  """Clear the badge image cache. Useful for testing or memory pressure."""
  global _BADGE_IMAGE_CACHE
  _BADGE_IMAGE_CACHE.clear()


def _check_column_exists_cached(
    cur: mysql.connector.cursor.MySQLCursor,
    table_name: str,
    column_name: str,
) -> bool:
  """
  Check if a column exists in a table, using in-process cache to avoid repeated information_schema queries.
  
  Args:
    cur: MySQL cursor (dictionary mode)
    table_name: Name of the table
    column_name: Name of the column
  
  Returns:
    True if column exists, False otherwise
  """
  cache_key = f"{table_name}.{column_name}"
  if cache_key in _SCHEMA_CAPABILITY_CACHE:
    return _SCHEMA_CAPABILITY_CACHE[cache_key]
  
  # Query information_schema only once per table.column combination
  cur.execute(
    """
    SELECT COUNT(*) AS cnt
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = %s
      AND COLUMN_NAME = %s
    """,
    (table_name, column_name),
  )
  schema_row = cur.fetchone() or {}
  exists = bool(schema_row.get("cnt", 0))
  _SCHEMA_CAPABILITY_CACHE[cache_key] = exists
  return exists


def _decode_badge_image_cached(badge_raw: Optional[Any]) -> Optional[Any]:
  """
  Decode a badge image with in-memory caching to avoid repeated decode overhead.
  
  Supports both blob-backed (bytes) and path-backed (string) badges. Caches decoded
  numpy arrays keyed by hash of blob content or file path.
  
  Args:
    badge_raw: Badge data (bytes blob or file path string) or None
  
  Returns:
    Decoded image array (numpy ndarray) or None if decode fails or input is None
  """
  if badge_raw is None:
    return None
  
  # Create cache key from badge content/path
  try:
    if isinstance(badge_raw, (bytes, bytearray, memoryview)):
      cache_key = f"blob_{hashlib.md5(bytes(badge_raw)).hexdigest()}"
    else:
      cache_key = f"path_{hashlib.md5(str(badge_raw).encode()).hexdigest()}"
  except Exception:
    return None  # If we can't hash, skip caching
  
  # Return cached result if available
  if cache_key in _BADGE_IMAGE_CACHE:
    return _BADGE_IMAGE_CACHE[cache_key]
  
  # Decode the badge
  arr_img = None
  try:
    if isinstance(badge_raw, (bytes, bytearray, memoryview)):
      arr_img = plt.imread(BytesIO(bytes(badge_raw)))
    else:
      # For path-backed badges, resolve path first
      from pathlib import Path as PathlibPath
      p = PathlibPath(badge_raw) if badge_raw else None
      candidates = [
        p,
        PathlibPath.cwd() / (str(badge_raw) if badge_raw else ""),
        PathlibPath(__file__).resolve().parents[2] / (str(badge_raw) if badge_raw else ""),
        PathlibPath(__file__).resolve().parent / (str(badge_raw) if badge_raw else ""),
      ] if p else []
      badge_path = None
      for candidate in candidates:
        if candidate and candidate.is_file():
          badge_path = str(candidate)
          break
      if badge_path:
        arr_img = plt.imread(badge_path)
  except Exception:
    return None
  
  # Cache the decoded image if successful, but only if cache isn't full
  if arr_img is not None:
    if len(_BADGE_IMAGE_CACHE) < _BADGE_CACHE_MAX_SIZE:
      _BADGE_IMAGE_CACHE[cache_key] = arr_img
  
  return arr_img


def plot_goals_comparison(
  db: DBConfig,
  league_code: str,
  team_names: list = ["Inter", "Milan", "Juventus", "Napoli"],
  years_back: int = 10,
  image_dir: str = "generated_graphs",
) -> dict:
  """Generate a line graph of goals scored per season for teams, with badge legend."""
  from datetime import datetime
  import matplotlib.offsetbox as offsetbox

  def _resolve_badge_path(raw_path: Optional[str]) -> Optional[str]:
    if not raw_path:
      return None
    p = Path(raw_path)
    candidates = [
      p,
      Path.cwd() / raw_path,
      Path(__file__).resolve().parents[2] / raw_path,
      Path(__file__).resolve().parent / raw_path,
    ]
    for candidate in candidates:
      if candidate.is_file():
        return str(candidate)
    return None

  os.makedirs(image_dir, exist_ok=True)
  this_year = datetime.now().year
  start_year = this_year - years_back + 1

  conn = _connect(db)
  cur = conn.cursor(dictionary=True)
  try:
    cur.execute(
      """
      SELECT league_id, league_name
      FROM league
      WHERE league_code = %s
      LIMIT 1
      """,
      (league_code,),
    )
    league_row = cur.fetchone() or {}
    local_league_id = league_row.get("league_id", -1)
    local_league_name = str(league_row.get("league_name", "")).replace("_", " ").strip().lower()

    cur.execute(
      """
      SELECT s.season_id, s.start_year, s.season_label
      FROM season s
      WHERE s.start_year >= %s AND s.start_year <= %s
      ORDER BY s.start_year
      """,
      (start_year, this_year),
    )
    seasons = cur.fetchall()
    season_labels = [row["season_label"] for row in seasons]

    # Check if team table has badge_path column (cached to avoid repeated information_schema queries)
    has_team_badge_path = _check_column_exists_cached(cur, "team", "badge_path")

    team_ids = {}
    team_badges = {}
    for name in team_names:
      if has_team_badge_path:
        cur.execute(
          "SELECT team_id, badge_path AS badge_ref FROM team WHERE team_name LIKE %s LIMIT 1",
          (f"%{name}%",),
        )
      else:
        cur.execute(
          """
          SELECT t.team_id, tb.badge_image AS badge_ref, tb.badge_url
          FROM team t
          LEFT JOIN team_badge tb
            ON LOWER(tb.team_name) LIKE LOWER(CONCAT('%', t.team_name, '%'))
          WHERE t.team_name LIKE %s
          ORDER BY
            CASE
              WHEN tb.league_id = %s THEN 0
              WHEN LOWER(COALESCE(tb.league_name, '')) = %s THEN 1
              WHEN LOWER(COALESCE(tb.league_name, '')) LIKE %s THEN 2
              ELSE 3
            END,
            tb.season_year DESC,
            tb.updated_at DESC
          LIMIT 1
          """,
          (f"%{name}%", local_league_id, local_league_name, f"%{local_league_name}%"),
        )
      row = cur.fetchone()
      team_ids[name] = row["team_id"] if row else None
      team_badges[name] = row["badge_ref"] if row else None

    team_goals = {name: [0] * len(seasons) for name in team_names}
    tracked_team_ids = [team_id for team_id in team_ids.values() if team_id is not None]
    if seasons and tracked_team_ids:
      season_ids = [season["season_id"] for season in seasons]
      season_index = {season_id: idx for idx, season_id in enumerate(season_ids)}
      team_lookup = {team_id: name for name, team_id in team_ids.items() if team_id is not None}
      season_placeholders = ", ".join(["%s"] * len(season_ids))
      team_placeholders = ", ".join(["%s"] * len(tracked_team_ids))
      goals_query = f"""
        WITH grouped_goals AS (
          SELECT
            m.season_id,
            m.home_team_id AS team_id,
            COALESCE(m.ft_home_goals, 0) AS goals
          FROM match_game m
          JOIN league l ON l.league_id = m.league_id
          WHERE l.league_code = %s
            AND m.season_id IN ({season_placeholders})
            AND m.home_team_id IN ({team_placeholders})

          UNION ALL

          SELECT
            m.season_id,
            m.away_team_id AS team_id,
            COALESCE(m.ft_away_goals, 0) AS goals
          FROM match_game m
          JOIN league l ON l.league_id = m.league_id
          WHERE l.league_code = %s
            AND m.season_id IN ({season_placeholders})
            AND m.away_team_id IN ({team_placeholders})
        )
        SELECT season_id, team_id, SUM(goals) AS goals
        FROM grouped_goals
        GROUP BY season_id, team_id
      """
      query_params = (
        [league_code]
        + season_ids
        + tracked_team_ids
        + [league_code]
        + season_ids
        + tracked_team_ids
      )
      cur.execute(goals_query, query_params)
      for row in cur.fetchall():
        team_name = team_lookup.get(row["team_id"])
        season_idx = season_index.get(row["season_id"])
        if team_name is None or season_idx is None:
          continue
        team_goals[team_name][season_idx] = row["goals"] if row["goals"] is not None else 0

    plt.figure(figsize=(12, 7))
    lines = {}
    for name in team_names:
      (line,) = plt.plot(season_labels, team_goals[name], marker="o", label=name)
      lines[name] = line

    plt.xlabel("Season")
    plt.ylabel("Goals Scored")
    plt.title(f"Goals Scored per Season ({start_year}-{this_year})")
    plt.grid(True, axis="y", linestyle="--", alpha=0.5)

    plt.tight_layout()
    fig = plt.gcf()
    legend = plt.legend([lines[name] for name in team_names], team_names, loc="upper left", fontsize=10)

    # Render badge images next to legend labels in figure coordinates.
    # Using figure-space anchors avoids drifting when layout is recomputed.
    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    for text in legend.get_texts():
      name = text.get_text()
      badge_raw = team_badges.get(name)
      # Use cached badge decode to minimize decoding overhead
      arr_img = _decode_badge_image_cached(badge_raw)
      if arr_img is None:
        continue

      text_box = text.get_window_extent(renderer=renderer)
      x_disp = text_box.x0 - 20
      y_disp = (text_box.y0 + text_box.y1) / 2
      x_fig, y_fig = fig.transFigure.inverted().transform((x_disp, y_disp))

      imagebox = offsetbox.OffsetImage(arr_img, zoom=0.12)
      badge_artist = offsetbox.AnnotationBbox(
        imagebox,
        (x_fig, y_fig),
        xycoords=fig.transFigure,
        frameon=False,
        box_alignment=(1.0, 0.5),
        pad=0,
        zorder=7,
        annotation_clip=False,
      )
      fig.add_artist(badge_artist)

    filename = f"goals_comparison_{'_'.join([n.lower() for n in team_names])}_{start_year}_{this_year}.png"
    image_path = os.path.join(image_dir, filename)
    buf = BytesIO()
    fig.savefig(buf, format="png")
    image_bytes = buf.getvalue()
    Path(image_path).write_bytes(image_bytes)
    base64_image = base64.b64encode(image_bytes).decode("utf-8")
    plt.close(fig)
    return {
      "image_path": image_path,
      "base64_image": base64_image,
      "season_labels": season_labels,
      "team_goals": team_goals,
      "team_badges": {
        name: ("embedded_blob" if isinstance(path, (bytes, bytearray, memoryview)) else _resolve_badge_path(str(path) if path is not None else None))
        for name, path in team_badges.items()
      },
    }
  finally:
    cur.close()
    conn.close()
@dataclass
class DBConfig:
    host: str = "127.0.0.1"
    port: int = 3306
    user: str = "football_admin"
    password: str = ""
    database: str = "historic_football_data"


ALLOWED_TIE_BREAKERS = {
    "points": "points",
    "goal_diff": "goal_diff",
    "goals_for": "goals_for",
    "team_id": "team_id",
}

FINISHED_EVENT_STATUSES = ("FT", "AET", "PEN", "FT_PEN", "AWD", "WO")
FINISHED_EVENT_STATUS_SQL = ", ".join(f"'{status}'" for status in FINISHED_EVENT_STATUSES)


def _connect(db: DBConfig):
    return mysql.connector.connect(
        host=db.host,
        port=db.port,
        user=db.user,
        password=db.password,
        database=db.database,
        charset="utf8mb4",
        use_unicode=True,
        use_pure=True,
        autocommit=True,
    )


def _order_clause(tie_breakers: Sequence[str]) -> str:
    if not tie_breakers:
        tie_breakers = ("points", "goal_diff", "goals_for", "team_id")

    mapped: List[str] = []
    for key in tie_breakers:
        if key not in ALLOWED_TIE_BREAKERS:
            raise ValueError(
                f"Unsupported tie breaker: {key!r}. "
                f"Supported: {sorted(ALLOWED_TIE_BREAKERS)}"
            )
        mapped.append(ALLOWED_TIE_BREAKERS[key])

    return ", ".join(f"s.{col} DESC" for col in mapped)


def _league_filter_sql(league_code: Optional[str]) -> Tuple[str, Tuple[Any, ...]]:
    if league_code:
        return "WHERE l.league_code = %s", (league_code,)
    return "", ()


def _safe_per_game(total: Any, played: Any) -> float:
  if not played:
    return 0.0
  return float(total or 0.0) / float(played)


def _poisson_probability(mean: float, goals: int) -> float:
  if mean < 0:
    mean = 0.0
  return math.exp(-mean) * (mean ** goals) / math.factorial(goals)


def _score_probability_grid(
  expected_home_goals: float,
  expected_away_goals: float,
  rho: float = 0.0,
  max_goals: int = 10,
) -> Dict[str, Any]:
  """Numpy-vectorised Poisson score grid with optional Dixon-Coles rho correction.

  The Dixon-Coles tau factor adjusts the independent-Poisson probabilities for
  the four low-score cells (0-0, 1-0, 0-1, 1-1) which are systematically
  mis-estimated by the plain model. rho is expected to be <= 0 (typical range
  -0.15 to -0.05 for top-division football).
  """
  mu = float(expected_home_goals)
  nu = float(expected_away_goals)
  goals = np.arange(max_goals + 1, dtype=float)
  factorials = np.array([math.factorial(int(g)) for g in goals], dtype=float)
  home_pmf = np.exp(-mu) * np.power(mu, goals) / factorials
  away_pmf = np.exp(-nu) * np.power(nu, goals) / factorials
  grid = np.outer(home_pmf, away_pmf)

  if rho != 0.0:
    safe_rho = float(np.clip(rho, -1.0 / max(mu * nu, 1e-6), 0.99))
    tau = np.ones_like(grid)
    tau[0, 0] = max(1e-6, 1.0 - mu * nu * safe_rho)
    if max_goals >= 1:
      tau[0, 1] = max(1e-6, 1.0 + mu * safe_rho)
      tau[1, 0] = max(1e-6, 1.0 + nu * safe_rho)
      tau[1, 1] = max(1e-6, 1.0 - safe_rho)
    grid = grid * tau

  total = grid.sum()
  if total > 0:
    grid /= total

  home_win = float(np.tril(grid, -1).sum())
  draw = float(np.trace(grid))
  away_win = float(np.triu(grid, 1).sum())
  best_idx = np.unravel_index(np.argmax(grid), grid.shape)
  best_score = {
    "home_goals": int(best_idx[0]),
    "away_goals": int(best_idx[1]),
    "probability": round(float(grid[best_idx]), 6),
  }
  flat = [
    {"home_goals": int(i), "away_goals": int(j), "probability": round(float(grid[i, j]), 6)}
    for i in range(max_goals + 1)
    for j in range(max_goals + 1)
  ]
  flat.sort(key=lambda x: x["probability"], reverse=True)

  outcome = "draw"
  if home_win >= draw and home_win >= away_win:
    outcome = "home_win"
  elif away_win >= draw and away_win >= home_win:
    outcome = "away_win"

  return {
    "home_win_probability": round(home_win, 4),
    "draw_probability": round(draw, 4),
    "away_win_probability": round(away_win, 4),
    "most_likely_score": best_score,
    "predicted_outcome": outcome,
    "score_grid": flat[:10],
  }


def _calibrate_outcome_probabilities(
  probability_summary: Dict[str, Any],
  *,
  draw_probability_floor: float,
  draw_margin: float,
  home_win_bias: float,
) -> Dict[str, Any]:
  home_p = float(probability_summary.get("home_win_probability") or 0.0)
  draw_p = float(probability_summary.get("draw_probability") or 0.0)
  away_p = float(probability_summary.get("away_win_probability") or 0.0)

  adjusted_home = min(0.98, max(0.0, home_p + home_win_bias))
  adjusted_away = min(0.98, max(0.0, away_p - home_win_bias))
  adjusted_draw = min(0.98, max(0.0, draw_p))

  # Boost draw when teams are close in win probability.
  closeness = abs(adjusted_home - adjusted_away)
  if closeness < draw_margin:
    draw_boost = (draw_margin - closeness) * 0.6
    adjusted_draw = min(0.98, adjusted_draw + draw_boost)

  total = adjusted_home + adjusted_draw + adjusted_away
  if total <= 0:
    adjusted_home, adjusted_draw, adjusted_away = 1 / 3, 1 / 3, 1 / 3
  else:
    adjusted_home /= total
    adjusted_draw /= total
    adjusted_away /= total

  draw_floor_hit = draw_probability_floor > 0 and adjusted_draw >= draw_probability_floor
  closeness_draw = draw_margin > 0 and abs(adjusted_home - adjusted_away) <= draw_margin
  if draw_floor_hit or closeness_draw:
    predicted_outcome = "draw"
  elif adjusted_home >= adjusted_away:
    predicted_outcome = "home_win"
  else:
    predicted_outcome = "away_win"

  calibrated = dict(probability_summary)
  calibrated["raw_home_win_probability"] = round(home_p, 4)
  calibrated["raw_draw_probability"] = round(draw_p, 4)
  calibrated["raw_away_win_probability"] = round(away_p, 4)
  calibrated["home_win_probability"] = round(adjusted_home, 4)
  calibrated["draw_probability"] = round(adjusted_draw, 4)
  calibrated["away_win_probability"] = round(adjusted_away, 4)
  calibrated["predicted_outcome"] = predicted_outcome
  calibrated["calibration"] = {
    "draw_probability_floor": draw_probability_floor,
    "draw_margin": draw_margin,
    "home_win_bias": home_win_bias,
  }
  return calibrated


def _weighted_per_game(
  matches: List[Dict[str, Any]],
  field: str,
  cutoff_dt: datetime,
  decay_rate: float = 0.004,
) -> float:
  """Exponential time-decay weighted average of a per-match stat field.

  Matches closer to cutoff_dt receive higher weight (weight = exp(-decay_rate * days_ago)).
  A decay_rate of 0.004 gives a half-life of ~173 days (roughly one full league season).
  """
  total_w = 0.0
  total_v = 0.0
  for m in matches:
    fdate = m.get("fixture_date_utc")
    if fdate is None:
      continue
    if hasattr(fdate, "replace"):
      fdate = fdate.replace(tzinfo=None)
    elif isinstance(fdate, str):
      fdate = datetime.fromisoformat(fdate[:19])
    days = max(0, (cutoff_dt - fdate).days)
    w = math.exp(-decay_rate * days)
    total_w += w
    total_v += w * float(m.get(field) or 0.0)
  return total_v / total_w if total_w > 0 else 0.0


def _fit_league_model(
  fixtures: List[Dict[str, Any]],
  cutoff_dt: datetime,
  decay_rate: float = 0.004,
) -> Optional[Dict[str, Any]]:
  """Fit Maher attack/defense parameters and Dixon-Coles rho via time-weighted MLE.

  For each finished fixture, the model assumes:
    lambda_home = attack[home] * defense[away] * home_adv
    lambda_away = attack[away] * defense[home]

  The log-likelihood is maximised with exponential time-decay weighting so that
  recent matches contribute more than old ones. The Dixon-Coles rho parameter
  corrects the independent-Poisson under-estimate of low-score draws.

  Returns a dict with 'attack', 'defense', 'home_adv', 'rho', or None on failure.
  """
  from collections import Counter

  if len(fixtures) < 10:
    return None

  home_names = [f["home_team_name"] for f in fixtures]
  away_names = [f["away_team_name"] for f in fixtures]
  hg = np.array([float(f.get("goals_home") or 0) for f in fixtures])
  ag = np.array([float(f.get("goals_away") or 0) for f in fixtures])
  teams = sorted(set(home_names) | set(away_names))
  n = len(teams)
  if n < 4:
    return None

  game_counts = Counter(home_names + away_names)
  if min(game_counts.values()) < 3:
    return None

  idx = {t: i for i, t in enumerate(teams)}
  hi = np.array([idx[t] for t in home_names])
  ai = np.array([idx[t] for t in away_names])

  def _fdate(f: Dict[str, Any]) -> datetime:
    d = f["fixture_date_utc"]
    if hasattr(d, "replace"):
      return d.replace(tzinfo=None)
    return datetime.fromisoformat(str(d)[:19])

  weights = np.array(
    [math.exp(-decay_rate * max(0, (cutoff_dt - _fdate(f)).days)) for f in fixtures],
    dtype=float,
  )
  weights = np.clip(weights, 1e-6, None)

  hg_int = hg.astype(int)
  ag_int = ag.astype(int)

  x0 = np.zeros(2 * n + 2)
  x0[2 * n] = math.log(1.3)   # home advantage prior
  x0[2 * n + 1] = -0.1        # rho prior

  def neg_log_likelihood(params: np.ndarray) -> float:
    log_attack = params[:n]
    log_defense = params[n: 2 * n]
    home_adv = math.exp(float(np.clip(params[2 * n], -3.0, 3.0)))
    rho = float(params[2 * n + 1])
    attack = np.exp(log_attack)
    defense = np.exp(log_defense)
    mu = np.clip(attack[hi] * defense[ai] * home_adv, 1e-4, 20.0)
    nu = np.clip(attack[ai] * defense[hi], 1e-4, 20.0)
    tau = np.ones(len(mu))
    m00 = (hg_int == 0) & (ag_int == 0)
    m01 = (hg_int == 0) & (ag_int == 1)
    m10 = (hg_int == 1) & (ag_int == 0)
    m11 = (hg_int == 1) & (ag_int == 1)
    tau[m00] = np.clip(1.0 - mu[m00] * nu[m00] * rho, 1e-6, None)
    tau[m01] = np.clip(1.0 + mu[m01] * rho, 1e-6, None)
    tau[m10] = np.clip(1.0 + nu[m10] * rho, 1e-6, None)
    tau[m11] = np.clip(1.0 - rho, 1e-6, None)
    ll = weights * (
      np.log(tau)
      + _scipy_poisson.logpmf(hg_int, mu)
      + _scipy_poisson.logpmf(ag_int, nu)
    )
    # Soft identifiability constraint: mean(log_attack) ≈ 0
    penalty = 1000.0 * float(np.mean(log_attack) ** 2)
    return float(-np.nansum(ll) + penalty)

  bounds = [(None, None)] * (2 * n) + [(None, None), (-0.99, 0.0)]
  try:
    result = _scipy_minimize(
      neg_log_likelihood,
      x0,
      method="L-BFGS-B",
      bounds=bounds,
      options={"maxiter": 300, "ftol": 1e-9},
    )
  except Exception:
    return None

  if not np.isfinite(result.fun):
    return None

  params = result.x
  attack = {t: float(np.exp(params[idx[t]])) for t in teams}
  defense = {t: float(np.exp(params[n + idx[t]])) for t in teams}
  home_adv = float(np.exp(float(np.clip(params[2 * n], -3.0, 3.0))))
  rho = float(np.clip(params[2 * n + 1], -0.99, 0.0))

  return {
    "attack": attack,
    "defense": defense,
    "home_adv": home_adv,
    "rho": rho,
    "n_fixtures": len(fixtures),
    "n_teams": n,
  }


def _team_name_key(value: str) -> str:
  normalized = unicodedata.normalize("NFKD", value)
  normalized = normalized.encode("ascii", "ignore").decode("ascii")
  normalized = normalized.lower().replace("&", " and ")
  normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
  tokens = [token for token in normalized.split() if token]
  stop_tokens = {
    "ac", "as", "fc", "cf", "cd", "ssc", "sc", "afc", "calcio",
    "club", "de", "the", "fk", "sv",
  }
  filtered = [token for token in tokens if token not in stop_tokens]
  return " ".join(filtered if filtered else tokens)


def _resolve_team(cur: mysql.connector.cursor.MySQLCursor, team_name: str) -> Dict[str, Any]:

  cur.execute(
    """
    SELECT team_id, team_name
    FROM team
    WHERE LOWER(team_name) = LOWER(%s)
    LIMIT 1
    """,
    (team_name,),
  )
  row = cur.fetchone()
  if row:
    return row

  cur.execute(
    """
    SELECT team_id, team_name
    FROM team
    WHERE LOWER(team_name) LIKE LOWER(%s)
    ORDER BY CHAR_LENGTH(team_name) ASC, team_id ASC
    LIMIT 1
    """,
    (f"%{team_name}%",),
  )
  row = cur.fetchone()
  if row:
    return row

  team_key = _team_name_key(team_name)
  if team_key:
    team_tokens = sorted(set(team_key.split()), key=len, reverse=True)
    candidate_sql = """
      SELECT team_id, team_name
      FROM team
      WHERE {}
    """
    if team_tokens:
      like_clauses = ["LOWER(team_name) LIKE LOWER(%s)" for _ in team_tokens]
      cur.execute(
        candidate_sql.format(" OR ".join(like_clauses)),
        tuple(f"%{token}%" for token in team_tokens),
      )
      candidates = cur.fetchall() or []
    else:
      candidates = []

    if not candidates:
      cur.execute(
        """
        SELECT team_id, team_name
        FROM team
        """,
      )
      candidates = cur.fetchall() or []

    best_row: Optional[Dict[str, Any]] = None
    best_score = 0.0
    for candidate in candidates:
      candidate_name = str(candidate.get("team_name") or "")
      candidate_key = _team_name_key(candidate_name)
      if not candidate_key:
        continue

      score = 0.0
      if candidate_key == team_key:
        score = 10.0
      elif candidate_key.startswith(team_key) or team_key.startswith(candidate_key):
        score = 8.0
      elif candidate_key in team_key or team_key in candidate_key:
        score = 7.0
      elif all(token in candidate_key.split() for token in team_tokens):
        score = 6.0
      else:
        score = SequenceMatcher(None, team_key, candidate_key).ratio()

      if score > best_score:
        best_score = score
        best_row = candidate

    if best_row and best_score >= 0.75:
      return best_row

  raise ValueError(f"Team not found in local historical data: {team_name!r}")


def _find_fixture_context(
  cur: mysql.connector.cursor.MySQLCursor,
  season_year: int,
  event_league_name: str,
  home_names: Tuple[str, str],
  away_names: Tuple[str, str],
  cutoff_utc: str,
) -> Optional[Dict[str, Any]]:
  cur.execute(
    """
    SELECT
      provider_fixture_id,
      league_id,
      fixture_date_utc,
      status_short,
      home_team_id,
      away_team_id,
      home_team_name,
      away_team_name
    FROM event_fixture
    WHERE season_year = %s
      AND league_name = %s
      AND (
        (home_team_name IN (%s, %s) AND away_team_name IN (%s, %s))
        OR (home_team_name IN (%s, %s) AND away_team_name IN (%s, %s))
      )
    ORDER BY ABS(TIMESTAMPDIFF(SECOND, fixture_date_utc, %s)) ASC
    LIMIT 1
    """,
    (
      season_year,
      event_league_name,
      home_names[0],
      home_names[1],
      away_names[0],
      away_names[1],
      away_names[0],
      away_names[1],
      home_names[0],
      home_names[1],
      cutoff_utc,
    ),
  )
  return cur.fetchone()


def _fetch_fixture_lineups(
  cur: mysql.connector.cursor.MySQLCursor,
  provider_fixture_id: Optional[int],
  home_team_id: Optional[int],
  away_team_id: Optional[int],
  home_team_name: str,
  away_team_name: str,
) -> Dict[str, Any]:
  payload = {
    "home": {"formation": None, "starters": [], "substitutes": []},
    "away": {"formation": None, "starters": [], "substitutes": []},
  }
  if not provider_fixture_id:
    return payload

  try:
    cur.execute(
      """
      SELECT
        fl.team_id,
        fl.team_name,
        fl.formation,
        fp.player_name,
        fp.player_number,
        fp.player_pos,
        fp.is_starter
      FROM fixture_lineup fl
      LEFT JOIN fixture_lineup_player fp ON fp.lineup_id = fl.lineup_id
      WHERE fl.provider_fixture_id = %s
      ORDER BY fl.team_id, fp.is_starter DESC, fp.player_number ASC, fp.player_name ASC
      """,
      (provider_fixture_id,),
    )
  except mysql.connector.Error:
    return payload

  rows = cur.fetchall() or []
  if not rows:
    return payload

  grouped: Dict[str, Dict[str, Any]] = {}
  for row in rows:
    key = str(row.get("team_id") or row.get("team_name") or "")
    if key not in grouped:
      grouped[key] = {
        "team_id": row.get("team_id"),
        "team_name": row.get("team_name"),
        "formation": row.get("formation"),
        "starters": [],
        "substitutes": [],
      }
    if row.get("player_name"):
      player = {
        "player_name": row.get("player_name"),
        "player_number": row.get("player_number"),
        "player_pos": row.get("player_pos"),
      }
      if bool(row.get("is_starter")):
        grouped[key]["starters"].append(player)
      else:
        grouped[key]["substitutes"].append(player)

  home_key = _team_name_key(home_team_name)
  away_key = _team_name_key(away_team_name)
  values = list(grouped.values())
  home_lineup: Optional[Dict[str, Any]] = None
  away_lineup: Optional[Dict[str, Any]] = None
  for item in values:
    team_id = item.get("team_id")
    team_key = _team_name_key(str(item.get("team_name") or ""))
    if home_team_id and team_id == home_team_id:
      home_lineup = item
      continue
    if away_team_id and team_id == away_team_id:
      away_lineup = item
      continue
    if not home_lineup and team_key == home_key:
      home_lineup = item
    elif not away_lineup and team_key == away_key:
      away_lineup = item

  if home_lineup is None and values:
    home_lineup = values[0]
  if away_lineup is None and len(values) > 1:
    away_lineup = values[1]

  return {
    "home": home_lineup or payload["home"],
    "away": away_lineup or payload["away"],
  }


def _fetch_active_injuries(
  cur: mysql.connector.cursor.MySQLCursor,
  provider_fixture_id: Optional[int],
  league_id: Optional[int],
  season_year: int,
  cutoff_date: str,
  home_team_id: Optional[int],
  away_team_id: Optional[int],
  home_team_name: str,
  away_team_name: str,
) -> Dict[str, List[Dict[str, Any]]]:
  payload: Dict[str, List[Dict[str, Any]]] = {"home": [], "away": []}
  if league_id is None:
    return payload

  rows: List[Dict[str, Any]] = []
  try:
    if provider_fixture_id:
      cur.execute(
        """
        SELECT
          provider_team_id,
          team_name,
          provider_player_id,
          player_name,
          injury_type,
          injury_reason,
          injury_date,
          return_date
        FROM player_injury
        WHERE fixture_id = %s
        ORDER BY provider_player_id, injury_date DESC
        """,
        (provider_fixture_id,),
      )
      rows = cur.fetchall() or []

    if not rows:
      cur.execute(
        """
        SELECT
          provider_team_id,
          team_name,
          provider_player_id,
          player_name,
          injury_type,
          injury_reason,
          injury_date,
          return_date
        FROM player_injury
        WHERE league_id = %s
          AND season_year = %s
          AND (injury_date IS NULL OR injury_date <= %s)
          AND (return_date IS NULL OR return_date >= %s)
        ORDER BY provider_player_id, injury_date DESC
        """,
        (league_id, season_year, cutoff_date, cutoff_date),
      )
      rows = cur.fetchall() or []
  except mysql.connector.Error:
    return payload

  home_key = _team_name_key(home_team_name)
  away_key = _team_name_key(away_team_name)
  seen_home: set = set()
  seen_away: set = set()

  for row in rows:
    team_id = row.get("provider_team_id")
    team_name_key = _team_name_key(str(row.get("team_name") or ""))
    player_id = row.get("provider_player_id")
    player_name = str(row.get("player_name") or "")
    dedupe_key = player_id or player_name
    injury = {
      "player_id": player_id,
      "player_name": player_name,
      "injury_type": row.get("injury_type"),
      "injury_reason": row.get("injury_reason"),
      "injury_date": row.get("injury_date").isoformat() if hasattr(row.get("injury_date"), "isoformat") else row.get("injury_date"),
      "return_date": row.get("return_date").isoformat() if hasattr(row.get("return_date"), "isoformat") else row.get("return_date"),
    }

    is_home = bool(home_team_id and team_id == home_team_id) or (team_name_key and team_name_key == home_key)
    is_away = bool(away_team_id and team_id == away_team_id) or (team_name_key and team_name_key == away_key)

    if is_home and dedupe_key not in seen_home:
      payload["home"].append(injury)
      seen_home.add(dedupe_key)
    elif is_away and dedupe_key not in seen_away:
      payload["away"].append(injury)
      seen_away.add(dedupe_key)

  payload["home"] = payload["home"][:15]
  payload["away"] = payload["away"][:15]
  return payload


def _player_name_key(value: str) -> str:
  normalized = unicodedata.normalize("NFKD", value)
  normalized = normalized.encode("ascii", "ignore").decode("ascii")
  normalized = normalized.lower()
  normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
  tokens = [t for t in normalized.split() if t]
  return " ".join(tokens)


def _fetch_team_expected_starter_importance(
  cur: mysql.connector.cursor.MySQLCursor,
  team_id: Optional[int],
  league_id: Optional[int],
  season_year: int,
  cutoff_utc: str,
  recent_window: int = 8,
) -> List[Dict[str, Any]]:
  if team_id is None or league_id is None:
    return []

  cur.execute(
    f"""
    SELECT
      provider_fixture_id,
      goals_home,
      goals_away,
      home_team_id,
      away_team_id
    FROM event_fixture
    WHERE league_id = %s
      AND season_year = %s
      AND status_short IN ({FINISHED_EVENT_STATUS_SQL})
      AND fixture_date_utc < %s
      AND (home_team_id = %s OR away_team_id = %s)
    ORDER BY fixture_date_utc DESC
    LIMIT %s
    """,
    (league_id, season_year, cutoff_utc, team_id, team_id, recent_window),
  )
  recent_fixtures = cur.fetchall() or []
  if not recent_fixtures:
    return []

  recent_fixture_ids = [int(r["provider_fixture_id"]) for r in recent_fixtures if r.get("provider_fixture_id") is not None]
  if not recent_fixture_ids:
    return []

  placeholders_recent = ", ".join(["%s"] * len(recent_fixture_ids))
  cur.execute(
    f"""
    SELECT
      fp.player_id,
      MAX(fp.player_name) AS player_name,
      MAX(fp.player_pos) AS player_pos,
      COUNT(*) AS recent_starts
    FROM fixture_lineup_player fp
    JOIN fixture_lineup fl ON fl.lineup_id = fp.lineup_id
    WHERE fl.team_id = %s
      AND fp.is_starter = 1
      AND fl.provider_fixture_id IN ({placeholders_recent})
    GROUP BY fp.player_id
    """,
    (team_id, *recent_fixture_ids),
  )
  starters_recent = cur.fetchall() or []
  if not starters_recent:
    return []

  player_ids = [int(r["player_id"]) for r in starters_recent if r.get("player_id") is not None]
  if not player_ids:
    return []
  placeholders_players = ", ".join(["%s"] * len(player_ids))

  cur.execute(
    f"""
    SELECT COUNT(*) AS team_fixtures
    FROM event_fixture
    WHERE league_id = %s
      AND season_year = %s
      AND status_short IN ({FINISHED_EVENT_STATUS_SQL})
      AND fixture_date_utc < %s
      AND (home_team_id = %s OR away_team_id = %s)
    """,
    (league_id, season_year, cutoff_utc, team_id, team_id),
  )
  team_fixtures = int((cur.fetchone() or {}).get("team_fixtures") or 0)

  cur.execute(
    f"""
    SELECT
      fp.player_id,
      COUNT(*) AS season_starts
    FROM fixture_lineup_player fp
    JOIN fixture_lineup fl ON fl.lineup_id = fp.lineup_id
    JOIN event_fixture ef ON ef.provider_fixture_id = fl.provider_fixture_id
    WHERE fl.team_id = %s
      AND fp.is_starter = 1
      AND ef.league_id = %s
      AND ef.season_year = %s
      AND ef.status_short IN ({FINISHED_EVENT_STATUS_SQL})
      AND ef.fixture_date_utc < %s
      AND fp.player_id IN ({placeholders_players})
    GROUP BY fp.player_id
    """,
    (team_id, league_id, season_year, cutoff_utc, *player_ids),
  )
  season_starts_map = {int(r["player_id"]): int(r["season_starts"] or 0) for r in (cur.fetchall() or [])}

  cur.execute(
    f"""
    SELECT
      COUNT(*) AS team_goals
    FROM event_goal eg
    JOIN event_fixture ef ON ef.provider_fixture_id = eg.provider_fixture_id
    WHERE eg.team_id = %s
      AND ef.league_id = %s
      AND ef.season_year = %s
      AND ef.status_short IN ({FINISHED_EVENT_STATUS_SQL})
      AND ef.fixture_date_utc < %s
    """,
    (team_id, league_id, season_year, cutoff_utc),
  )
  team_goals_total = float((cur.fetchone() or {}).get("team_goals") or 0.0)

  cur.execute(
    f"""
    SELECT
      eg.player_id,
      COUNT(*) AS goals
    FROM event_goal eg
    JOIN event_fixture ef ON ef.provider_fixture_id = eg.provider_fixture_id
    WHERE eg.team_id = %s
      AND ef.league_id = %s
      AND ef.season_year = %s
      AND ef.status_short IN ({FINISHED_EVENT_STATUS_SQL})
      AND ef.fixture_date_utc < %s
      AND eg.player_id IN ({placeholders_players})
    GROUP BY eg.player_id
    """,
    (team_id, league_id, season_year, cutoff_utc, *player_ids),
  )
  goals_map = {int(r["player_id"]): int(r["goals"] or 0) for r in (cur.fetchall() or [])}

  cur.execute(
    f"""
    SELECT
      eg.assist_id AS player_id,
      COUNT(*) AS assists
    FROM event_goal eg
    JOIN event_fixture ef ON ef.provider_fixture_id = eg.provider_fixture_id
    WHERE eg.team_id = %s
      AND ef.league_id = %s
      AND ef.season_year = %s
      AND ef.status_short IN ({FINISHED_EVENT_STATUS_SQL})
      AND ef.fixture_date_utc < %s
      AND eg.assist_id IS NOT NULL
      AND eg.assist_id IN ({placeholders_players})
    GROUP BY eg.assist_id
    """,
    (team_id, league_id, season_year, cutoff_utc, *player_ids),
  )
  assists_map = {int(r["player_id"]): int(r["assists"] or 0) for r in (cur.fetchall() or [])}

  cur.execute(
    f"""
    SELECT
      AVG(CASE WHEN home_team_id = %s THEN goals_away ELSE goals_home END) AS team_ga_avg
    FROM event_fixture
    WHERE league_id = %s
      AND season_year = %s
      AND status_short IN ({FINISHED_EVENT_STATUS_SQL})
      AND fixture_date_utc < %s
      AND (home_team_id = %s OR away_team_id = %s)
    """,
    (team_id, league_id, season_year, cutoff_utc, team_id, team_id),
  )
  team_ga_avg = float((cur.fetchone() or {}).get("team_ga_avg") or 0.0)

  cur.execute(
    f"""
    SELECT
      fp.player_id,
      AVG(CASE WHEN ef.home_team_id = %s THEN ef.goals_away ELSE ef.goals_home END) AS ga_when_start
    FROM fixture_lineup_player fp
    JOIN fixture_lineup fl ON fl.lineup_id = fp.lineup_id
    JOIN event_fixture ef ON ef.provider_fixture_id = fl.provider_fixture_id
    WHERE fl.team_id = %s
      AND fp.is_starter = 1
      AND ef.league_id = %s
      AND ef.season_year = %s
      AND ef.status_short IN ({FINISHED_EVENT_STATUS_SQL})
      AND ef.fixture_date_utc < %s
      AND fp.player_id IN ({placeholders_players})
    GROUP BY fp.player_id
    """,
    (team_id, team_id, league_id, season_year, cutoff_utc, *player_ids),
  )
  ga_when_start_map = {
    int(r["player_id"]): float(r["ga_when_start"] or 0.0)
    for r in (cur.fetchall() or [])
  }

  recent_map = {int(r["player_id"]): r for r in starters_recent}
  sorted_players = sorted(
    player_ids,
    key=lambda pid: (
      int((recent_map.get(pid) or {}).get("recent_starts") or 0),
      int(season_starts_map.get(pid) or 0),
      int(goals_map.get(pid) or 0) + int(assists_map.get(pid) or 0),
    ),
    reverse=True,
  )[:11]

  position_weight = {"F": 1.00, "M": 0.92, "D": 0.86, "G": 0.82}
  expected: List[Dict[str, Any]] = []
  recent_total = max(1, len(recent_fixture_ids))
  team_fixtures = max(1, team_fixtures)
  team_goals_den = max(1.0, team_goals_total)

  for pid in sorted_players:
    row = recent_map.get(pid) or {}
    player_name = str(row.get("player_name") or "")
    pos = str(row.get("player_pos") or "M").upper()[:1]
    recent_starts = int(row.get("recent_starts") or 0)
    season_starts = int(season_starts_map.get(pid) or 0)
    goals = int(goals_map.get(pid) or 0)
    assists = int(assists_map.get(pid) or 0)

    weighted_contrib = goals + (0.7 * assists)
    involvement_share = min(1.0, weighted_contrib / team_goals_den)
    goal_efficiency = min(1.0, (weighted_contrib / max(1, season_starts)) / 0.6)
    recent_rate = min(1.0, recent_starts / recent_total)
    season_rate = min(1.0, season_starts / team_fixtures)
    def_impact = 0.0
    if pos in {"D", "G"} and team_ga_avg > 0.0:
      player_ga = float(ga_when_start_map.get(pid) or team_ga_avg)
      def_impact = max(0.0, min(1.0, (team_ga_avg - player_ga) / max(0.1, team_ga_avg)))

    base = (0.35 * recent_rate) + (0.20 * season_rate) + (0.30 * involvement_share) + (0.10 * goal_efficiency) + (0.05 * def_impact)
    importance = max(0.05, min(1.0, position_weight.get(pos, 0.9) * base))

    expected.append({
      "player_id": pid,
      "player_name": player_name,
      "player_pos": pos,
      "recent_starts": recent_starts,
      "season_starts": season_starts,
      "goals": goals,
      "assists": assists,
      "importance": round(importance, 4),
    })

  return expected


def _apply_weighted_injury_adjustment(
  expected_home_goals: float,
  expected_away_goals: float,
  home_expected_starters: List[Dict[str, Any]],
  away_expected_starters: List[Dict[str, Any]],
  home_injuries: List[Dict[str, Any]],
  away_injuries: List[Dict[str, Any]],
  injury_weight: float,
) -> Tuple[float, float, Dict[str, Any]]:
  def _missing_weight(expected: List[Dict[str, Any]], injuries: List[Dict[str, Any]]) -> Tuple[float, List[Dict[str, Any]]]:
    if not expected:
      return 0.0, []
    injured_ids = {int(i["player_id"]) for i in injuries if i.get("player_id") is not None}
    injured_names = {_player_name_key(str(i.get("player_name") or "")) for i in injuries}
    missing: List[Dict[str, Any]] = []
    total_importance = sum(float(p.get("importance") or 0.0) for p in expected)
    missing_importance = 0.0
    for player in expected:
      pid = player.get("player_id")
      pname_key = _player_name_key(str(player.get("player_name") or ""))
      is_missing = bool((pid is not None and int(pid) in injured_ids) or (pname_key and pname_key in injured_names))
      if is_missing:
        missing.append(player)
        missing_importance += float(player.get("importance") or 0.0)
    ratio = (missing_importance / max(0.01, total_importance)) if total_importance > 0 else 0.0
    return min(1.0, max(0.0, ratio)), missing

  home_missing_ratio, home_missing_players = _missing_weight(home_expected_starters, home_injuries)
  away_missing_ratio, away_missing_players = _missing_weight(away_expected_starters, away_injuries)

  ratio_to_penalty_scale = 0.45 + (injury_weight * 8.0)
  home_penalty = min(0.30, max(0.0, home_missing_ratio * ratio_to_penalty_scale))
  away_penalty = min(0.30, max(0.0, away_missing_ratio * ratio_to_penalty_scale))

  home_multiplier = max(0.70, 1.0 - home_penalty) * (1.0 + (away_penalty * 0.5))
  away_multiplier = max(0.70, 1.0 - away_penalty) * (1.0 + (home_penalty * 0.5))

  return (
    expected_home_goals * home_multiplier,
    expected_away_goals * away_multiplier,
    {
      "home_missing_count": len(home_missing_players),
      "away_missing_count": len(away_missing_players),
      "home_missing_importance_ratio": round(home_missing_ratio, 4),
      "away_missing_importance_ratio": round(away_missing_ratio, 4),
      "home_missing_players": home_missing_players,
      "away_missing_players": away_missing_players,
      "injury_weight": injury_weight,
      "home_multiplier": round(home_multiplier, 4),
      "away_multiplier": round(away_multiplier, 4),
    },
  )


def _apply_xi_strength_boost(
  expected_home_goals: float,
  expected_away_goals: float,
  home_expected_starters: List[Dict[str, Any]],
  away_expected_starters: List[Dict[str, Any]],
  xi_boost_weight: float,
) -> Tuple[float, float, Dict[str, Any]]:
  def _team_strength(expected: List[Dict[str, Any]]) -> float:
    if not expected:
      return 0.0
    top_players = expected[:11]
    total_importance = sum(float(player.get("importance") or 0.0) for player in top_players)
    return total_importance / max(1, len(top_players))

  home_strength = _team_strength(home_expected_starters)
  away_strength = _team_strength(away_expected_starters)
  average_strength = max(0.05, (home_strength + away_strength) / 2.0)
  relative_delta = (home_strength - away_strength) / average_strength
  boost_scale = min(0.08, max(-0.08, relative_delta * xi_boost_weight))
  home_multiplier = min(1.08, max(0.92, 1.0 + boost_scale))
  away_multiplier = min(1.08, max(0.92, 1.0 - boost_scale))

  return (
    expected_home_goals * home_multiplier,
    expected_away_goals * away_multiplier,
    {
      "enabled": True,
      "xi_boost_weight": round(xi_boost_weight, 4),
      "home_strength": round(home_strength, 4),
      "away_strength": round(away_strength, 4),
      "strength_delta": round(home_strength - away_strength, 4),
      "relative_strength_delta": round(relative_delta, 4),
      "home_multiplier": round(home_multiplier, 4),
      "away_multiplier": round(away_multiplier, 4),
    },
  )


def predict_match_outcome(
  db: DBConfig,
  home_team_name: str,
  away_team_name: str,
  league_code: str,
  season_year: Optional[int] = None,
  as_of_utc: Optional[str] = None,
  head_to_head_weight: float = 0.2,
  draw_probability_floor: float = 0.0,
  draw_margin: float = 0.06,
  home_win_bias: float = 0.01,
  include_injuries: bool = False,
  injury_weight: float = 0.005,
  include_xi_boost: bool = False,
  xi_boost_weight: float = 0.05,
) -> Dict[str, Any]:
  """Predict a match result from local historical and current-season data only.

  The model is intentionally lightweight and fully local:
  - historical head-to-head from match_game
  - current-season form from event_fixture
  - current-season league scoring averages from event_fixture
  - a Poisson score grid on top of blended expected goals

  Args:
    db: DB connection config.
    home_team_name: Home team as stored in local tables.
    away_team_name: Away team as stored in local tables.
    league_code: Historical league code such as "I1".
    season_year: API/event_fixture season year. If omitted, uses the latest local season_year
      available for the selected fixture pair in the selected league.
    as_of_utc: Optional UTC cutoff like "2026-04-26 16:00:00". Only matches before this
      timestamp contribute to current-season form and averages.
    head_to_head_weight: Blend weight for venue-specific head-to-head scoring rates.

  Returns:
    Dict with prediction probabilities, expected goals, and the supporting local statistics.
  """
  if not 0.0 <= head_to_head_weight <= 1.0:
    raise ValueError("head_to_head_weight must be between 0 and 1")
  if not 0.0 <= draw_probability_floor <= 1.0:
    raise ValueError("draw_probability_floor must be between 0 and 1")
  if not 0.0 <= draw_margin <= 1.0:
    raise ValueError("draw_margin must be between 0 and 1")
  if not -0.2 <= home_win_bias <= 0.2:
    raise ValueError("home_win_bias must be between -0.2 and 0.2")
  if not 0.0 <= injury_weight <= 0.2:
    raise ValueError("injury_weight must be between 0 and 0.2")
  if not 0.0 <= xi_boost_weight <= 0.2:
    raise ValueError("xi_boost_weight must be between 0 and 0.2")

  cutoff_utc = as_of_utc or datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

  conn = _connect(db)
  cur = conn.cursor(dictionary=True)
  try:
    cur.execute(
      """
      SELECT league_id, league_name
      FROM league
      WHERE league_code = %s
      LIMIT 1
      """,
      (league_code,),
    )
    league_row = cur.fetchone()
    if not league_row:
      raise ValueError(f"League code not found in local historical data: {league_code!r}")

    historical_league_name = str(league_row["league_name"])
    home_team = _resolve_team(cur, home_team_name)
    away_team = _resolve_team(cur, away_team_name)
    home_team_id = int(home_team["team_id"])
    away_team_id = int(away_team["team_id"])
    resolved_home_name = str(home_team["team_name"])
    resolved_away_name = str(away_team["team_name"])
    normalized_league_name = historical_league_name.replace("_", " ").strip()
    normalized_league_parts = normalized_league_name.split()
    short_league_name = " ".join(normalized_league_parts[1:]) if len(normalized_league_parts) > 1 else normalized_league_name

    if season_year is None:
      cur.execute(
        """
        SELECT MAX(season_year) AS season_year
        FROM event_fixture
        WHERE league_name IN (%s, %s)
          AND (
          home_team_name IN (%s, %s) OR away_team_name IN (%s, %s)
          OR home_team_name IN (%s, %s) OR away_team_name IN (%s, %s)
          )
        """,
        (
          normalized_league_name,
          short_league_name,
          home_team_name,
          resolved_home_name,
          home_team_name,
          resolved_home_name,
          away_team_name,
          resolved_away_name,
          away_team_name,
          resolved_away_name,
        ),
      )
      season_row = cur.fetchone() or {}
      if season_row.get("season_year") is None:
        raise ValueError("Could not infer season_year from local event_fixture data")
      season_year = int(season_row["season_year"])

    cur.execute(
      """
      SELECT league_name
      FROM event_fixture
      WHERE season_year = %s
        AND (
        (home_team_name IN (%s, %s) AND away_team_name IN (%s, %s))
        OR (home_team_name IN (%s, %s) AND away_team_name IN (%s, %s))
        )
        AND league_name IS NOT NULL
        AND league_name <> ''
      ORDER BY fixture_date_utc DESC
      LIMIT 1
      """,
      (
        season_year,
        home_team_name,
        resolved_home_name,
        away_team_name,
        resolved_away_name,
        away_team_name,
        resolved_away_name,
        home_team_name,
        resolved_home_name,
      ),
    )
    event_league_row = cur.fetchone() or {}
    if event_league_row.get("league_name"):
      event_league_name = str(event_league_row["league_name"])
    else:
      event_league_name = short_league_name

    fixture_context = _find_fixture_context(
      cur,
      int(season_year),
      event_league_name,
      (home_team_name, resolved_home_name),
      (away_team_name, resolved_away_name),
      cutoff_utc,
    )
    provider_fixture_id = int(fixture_context.get("provider_fixture_id")) if fixture_context and fixture_context.get("provider_fixture_id") is not None else None
    event_league_id = int(fixture_context.get("league_id")) if fixture_context and fixture_context.get("league_id") is not None else None
    event_home_team_id = int(fixture_context.get("home_team_id")) if fixture_context and fixture_context.get("home_team_id") is not None else None
    event_away_team_id = int(fixture_context.get("away_team_id")) if fixture_context and fixture_context.get("away_team_id") is not None else None

    cutoff_date = cutoff_utc[:10]

    cur.execute(
      f"""
      SELECT
        COUNT(*) AS matches,
        SUM(CASE
          WHEN (mg.home_team_id = %s AND mg.ft_home_goals > mg.ft_away_goals)
            OR (mg.away_team_id = %s AND mg.ft_away_goals > mg.ft_home_goals)
          THEN 1 ELSE 0 END) AS home_team_wins,
        SUM(CASE
          WHEN (mg.home_team_id = %s AND mg.ft_home_goals > mg.ft_away_goals)
            OR (mg.away_team_id = %s AND mg.ft_away_goals > mg.ft_home_goals)
          THEN 1 ELSE 0 END) AS away_team_wins,
        SUM(CASE WHEN mg.ft_home_goals = mg.ft_away_goals THEN 1 ELSE 0 END) AS draws,
        SUM(CASE WHEN mg.home_team_id = %s THEN mg.ft_home_goals ELSE mg.ft_away_goals END) AS home_team_goals,
        SUM(CASE WHEN mg.home_team_id = %s THEN mg.ft_away_goals ELSE mg.ft_home_goals END) AS away_team_goals
      FROM match_game mg
      JOIN league l ON l.league_id = mg.league_id
      WHERE l.league_code = %s
        AND mg.ft_home_goals IS NOT NULL
        AND mg.ft_away_goals IS NOT NULL
        AND mg.match_date < %s
        AND (
          (mg.home_team_id = %s AND mg.away_team_id = %s)
          OR (mg.home_team_id = %s AND mg.away_team_id = %s)
        )
      """,
      (
        home_team_id,
        home_team_id,
        away_team_id,
        away_team_id,
        home_team_id,
        home_team_id,
        league_code,
        cutoff_date,
        home_team_id,
        away_team_id,
        away_team_id,
        home_team_id,
      ),
    )
    h2h_overall = cur.fetchone() or {}

    cur.execute(
      """
      SELECT
        COUNT(*) AS matches,
        SUM(CASE WHEN mg.ft_home_goals > mg.ft_away_goals THEN 1 ELSE 0 END) AS home_team_wins,
        SUM(CASE WHEN mg.ft_home_goals = mg.ft_away_goals THEN 1 ELSE 0 END) AS draws,
        SUM(CASE WHEN mg.ft_home_goals < mg.ft_away_goals THEN 1 ELSE 0 END) AS away_team_wins,
        SUM(mg.ft_home_goals) AS home_team_goals,
        SUM(mg.ft_away_goals) AS away_team_goals
      FROM match_game mg
      JOIN league l ON l.league_id = mg.league_id
      WHERE l.league_code = %s
        AND mg.ft_home_goals IS NOT NULL
        AND mg.ft_away_goals IS NOT NULL
        AND mg.match_date < %s
        AND mg.home_team_id = %s
        AND mg.away_team_id = %s
      """,
      (league_code, cutoff_date, home_team_id, away_team_id),
    )
    h2h_venue = cur.fetchone() or {}

    cur.execute(
      f"""
      SELECT
        fixture_date_utc,
        goals_home AS goals_for,
        goals_away AS goals_against,
        CASE WHEN goals_home > goals_away THEN 1 ELSE 0 END AS win,
        CASE WHEN goals_home = goals_away THEN 1 ELSE 0 END AS draw_flag,
        CASE WHEN goals_home < goals_away THEN 1 ELSE 0 END AS loss
      FROM event_fixture
      WHERE league_name = %s
        AND season_year = %s
        AND home_team_name IN (%s, %s)
        AND status_short IN ({FINISHED_EVENT_STATUS_SQL})
        AND fixture_date_utc < %s
      ORDER BY fixture_date_utc
      """,
      (event_league_name, season_year, home_team_name, resolved_home_name, cutoff_utc),
    )
    home_matches = cur.fetchall()
    home_form = {
      "played": len(home_matches),
      "wins": sum(int(m.get("win") or 0) for m in home_matches),
      "draws": sum(int(m.get("draw_flag") or 0) for m in home_matches),
      "losses": sum(int(m.get("loss") or 0) for m in home_matches),
      "goals_for": sum(int(m.get("goals_for") or 0) for m in home_matches),
      "goals_against": sum(int(m.get("goals_against") or 0) for m in home_matches),
    }

    cur.execute(
      f"""
      SELECT
        fixture_date_utc,
        goals_away AS goals_for,
        goals_home AS goals_against,
        CASE WHEN goals_away > goals_home THEN 1 ELSE 0 END AS win,
        CASE WHEN goals_away = goals_home THEN 1 ELSE 0 END AS draw_flag,
        CASE WHEN goals_away < goals_home THEN 1 ELSE 0 END AS loss
      FROM event_fixture
      WHERE league_name = %s
        AND season_year = %s
        AND away_team_name IN (%s, %s)
        AND status_short IN ({FINISHED_EVENT_STATUS_SQL})
        AND fixture_date_utc < %s
      ORDER BY fixture_date_utc
      """,
      (event_league_name, season_year, away_team_name, resolved_away_name, cutoff_utc),
    )
    away_matches = cur.fetchall()
    away_form = {
      "played": len(away_matches),
      "wins": sum(int(m.get("win") or 0) for m in away_matches),
      "draws": sum(int(m.get("draw_flag") or 0) for m in away_matches),
      "losses": sum(int(m.get("loss") or 0) for m in away_matches),
      "goals_for": sum(int(m.get("goals_for") or 0) for m in away_matches),
      "goals_against": sum(int(m.get("goals_against") or 0) for m in away_matches),
    }

    cur.execute(
      f"""
      SELECT
        COUNT(*) AS matches,
        AVG(goals_home) AS avg_home_goals,
        AVG(goals_away) AS avg_away_goals
      FROM event_fixture
      WHERE league_name = %s
        AND season_year = %s
        AND status_short IN ({FINISHED_EVENT_STATUS_SQL})
        AND fixture_date_utc < %s
      """,
      (event_league_name, season_year, cutoff_utc),
    )
    league_averages = cur.fetchone() or {}

    cur.execute(
      f"""
      SELECT
        home_team_name,
        away_team_name,
        goals_home,
        goals_away,
        fixture_date_utc
      FROM event_fixture
      WHERE league_name = %s
        AND season_year = %s
        AND status_short IN ({FINISHED_EVENT_STATUS_SQL})
        AND goals_home IS NOT NULL
        AND goals_away IS NOT NULL
        AND fixture_date_utc < %s
      ORDER BY fixture_date_utc
      """,
      (event_league_name, season_year, cutoff_utc),
    )
    all_season_fixtures = cur.fetchall()

    cur.execute(
      """
      SELECT
        mg.match_date,
        ht.team_name AS home_team,
        at.team_name AS away_team,
        mg.ft_home_goals,
        mg.ft_away_goals,
        mg.ft_result
      FROM match_game mg
      JOIN league l ON l.league_id = mg.league_id
      JOIN team ht ON ht.team_id = mg.home_team_id
      JOIN team at ON at.team_id = mg.away_team_id
      WHERE l.league_code = %s
        AND mg.match_date < %s
        AND (
          (mg.home_team_id = %s AND mg.away_team_id = %s)
          OR (mg.home_team_id = %s AND mg.away_team_id = %s)
        )
      ORDER BY mg.match_date DESC
      LIMIT 10
      """,
            (league_code, cutoff_date, home_team_id, away_team_id, away_team_id, home_team_id),
    )
    recent_head_to_head = cur.fetchall()

    home_played = int(home_form.get("played") or 0)
    away_played = int(away_form.get("played") or 0)
    avg_home_goals = float(league_averages.get("avg_home_goals") or 1.0)
    avg_away_goals = float(league_averages.get("avg_away_goals") or 1.0)
    cutoff_dt_naive = datetime.fromisoformat(cutoff_utc[:19])

    # --- Maher + Dixon-Coles model fit (time-decay weighted MLE over all season fixtures) ---
    league_model = _fit_league_model(all_season_fixtures, cutoff_dt_naive)
    home_event_key = next(
      (n for n in (resolved_home_name, home_team_name) if league_model and n in league_model["attack"]),
      None,
    )
    away_event_key = next(
      (n for n in (resolved_away_name, away_team_name) if league_model and n in league_model["attack"]),
      None,
    )
    fitted_rho = league_model["rho"] if league_model else 0.0
    model_source: str

    if league_model and home_event_key and away_event_key:
      # Primary path: Maher team strength parameters fitted via MLE
      expected_home_goals = (
        league_model["attack"][home_event_key]
        * league_model["defense"][away_event_key]
        * league_model["home_adv"]
      )
      expected_away_goals = (
        league_model["attack"][away_event_key]
        * league_model["defense"][home_event_key]
      )
      model_source = "maher_dc"
    else:
      # Fallback: time-decay weighted form ratios (same as classic Maher without full-league fitting)
      home_decay_for = _weighted_per_game(home_matches, "goals_for", cutoff_dt_naive)
      home_decay_against = _weighted_per_game(home_matches, "goals_against", cutoff_dt_naive)
      away_decay_for = _weighted_per_game(away_matches, "goals_for", cutoff_dt_naive)
      away_decay_against = _weighted_per_game(away_matches, "goals_against", cutoff_dt_naive)
      home_attack_strength = (home_decay_for if home_played else _safe_per_game(home_form.get("goals_for"), home_played)) / max(avg_home_goals, 0.01)
      away_defense_weakness = (away_decay_against if away_played else _safe_per_game(away_form.get("goals_against"), away_played)) / max(avg_home_goals, 0.01)
      away_attack_strength = (away_decay_for if away_played else _safe_per_game(away_form.get("goals_for"), away_played)) / max(avg_away_goals, 0.01)
      home_defense_weakness = (home_decay_against if home_played else _safe_per_game(home_form.get("goals_against"), home_played)) / max(avg_away_goals, 0.01)
      expected_home_goals = avg_home_goals * home_attack_strength * away_defense_weakness
      expected_away_goals = avg_away_goals * away_attack_strength * home_defense_weakness
      model_source = "form_decay"

    venue_matches = int(h2h_venue.get("matches") or 0)
    if venue_matches > 0 and head_to_head_weight > 0:
      venue_home_rate = _safe_per_game(h2h_venue.get("home_team_goals"), venue_matches)
      venue_away_rate = _safe_per_game(h2h_venue.get("away_team_goals"), venue_matches)
      expected_home_goals = ((1.0 - head_to_head_weight) * expected_home_goals) + (head_to_head_weight * venue_home_rate)
      expected_away_goals = ((1.0 - head_to_head_weight) * expected_away_goals) + (head_to_head_weight * venue_away_rate)

    lineups = _fetch_fixture_lineups(
      cur,
      provider_fixture_id,
      event_home_team_id,
      event_away_team_id,
      resolved_home_name,
      resolved_away_name,
    )
    injuries = {"home": [], "away": []}
    home_expected_starters: List[Dict[str, Any]] = []
    away_expected_starters: List[Dict[str, Any]] = []
    xi_impact = {
      "enabled": include_xi_boost,
      "xi_boost_weight": round(xi_boost_weight, 4),
      "home_strength": 0.0,
      "away_strength": 0.0,
      "strength_delta": 0.0,
      "relative_strength_delta": 0.0,
      "home_multiplier": 1.0,
      "away_multiplier": 1.0,
    }
    injury_impact = {
      "home_missing_count": 0,
      "away_missing_count": 0,
      "home_missing_importance_ratio": 0.0,
      "away_missing_importance_ratio": 0.0,
      "home_missing_players": [],
      "away_missing_players": [],
      "injury_weight": injury_weight,
      "home_multiplier": 1.0,
      "away_multiplier": 1.0,
    }
    if include_xi_boost or include_injuries:
      home_expected_starters = _fetch_team_expected_starter_importance(
        cur,
        event_home_team_id,
        event_league_id,
        int(season_year),
        cutoff_utc,
      )
      away_expected_starters = _fetch_team_expected_starter_importance(
        cur,
        event_away_team_id,
        event_league_id,
        int(season_year),
        cutoff_utc,
      )

    if include_xi_boost:
      expected_home_goals, expected_away_goals, xi_impact = _apply_xi_strength_boost(
        expected_home_goals,
        expected_away_goals,
        home_expected_starters,
        away_expected_starters,
        xi_boost_weight,
      )

    if include_injuries:
      injuries = _fetch_active_injuries(
        cur,
        provider_fixture_id,
        event_league_id,
        int(season_year),
        cutoff_date,
        event_home_team_id,
        event_away_team_id,
        resolved_home_name,
        resolved_away_name,
      )
      expected_home_goals, expected_away_goals, injury_impact = _apply_weighted_injury_adjustment(
        expected_home_goals,
        expected_away_goals,
        home_expected_starters,
        away_expected_starters,
        injuries.get("home") or [],
        injuries.get("away") or [],
        injury_weight,
      )

    expected_home_goals = max(0.05, round(expected_home_goals, 3))
    expected_away_goals = max(0.05, round(expected_away_goals, 3))
    raw_probability_summary = _score_probability_grid(expected_home_goals, expected_away_goals, rho=fitted_rho)
    probability_summary = _calibrate_outcome_probabilities(
      raw_probability_summary,
      draw_probability_floor=draw_probability_floor,
      draw_margin=draw_margin,
      home_win_bias=home_win_bias,
    )

    return {
      "home_team": resolved_home_name,
      "away_team": resolved_away_name,
      "league_code": league_code,
      "league_name": event_league_name,
      "historical_league_name": historical_league_name,
      "season_year": season_year,
      "as_of_utc": cutoff_utc,
      "fixture_context": {
        "provider_fixture_id": provider_fixture_id,
        "league_id": event_league_id,
        "fixture_date_utc": fixture_context.get("fixture_date_utc").isoformat() if fixture_context and hasattr(fixture_context.get("fixture_date_utc"), "isoformat") else (fixture_context.get("fixture_date_utc") if fixture_context else None),
        "status_short": fixture_context.get("status_short") if fixture_context else None,
      },
      "model_params": {
        "source": model_source,
        "rho": round(fitted_rho, 4),
        "home_adv": round(league_model["home_adv"], 4) if league_model else None,
        "n_fixtures_fitted": league_model["n_fixtures"] if league_model else 0,
      },
      "xi_impact": xi_impact,
      "injury_impact": injury_impact,
      "player_importance": {
        "home_expected_starters": home_expected_starters,
        "away_expected_starters": away_expected_starters,
      },
      "lineups": lineups,
      "injuries": injuries,
      "expected_goals": {
        "home": expected_home_goals,
        "away": expected_away_goals,
      },
      "prediction": probability_summary,
      "raw_prediction": raw_probability_summary,
      "team_ids": {
        "home_team_id": home_team_id,
        "away_team_id": away_team_id,
      },
      "league_averages": {
        "matches": int(league_averages.get("matches") or 0),
        "avg_home_goals": round(avg_home_goals, 3),
        "avg_away_goals": round(avg_away_goals, 3),
      },
      "current_form": {
        "home_team_home_split": {
          "played": home_played,
          "wins": int(home_form.get("wins") or 0),
          "draws": int(home_form.get("draws") or 0),
          "losses": int(home_form.get("losses") or 0),
          "goals_for": int(home_form.get("goals_for") or 0),
          "goals_against": int(home_form.get("goals_against") or 0),
          "goals_for_per_game": round(_safe_per_game(home_form.get("goals_for"), home_played), 3),
          "goals_against_per_game": round(_safe_per_game(home_form.get("goals_against"), home_played), 3),
        },
        "away_team_away_split": {
          "played": away_played,
          "wins": int(away_form.get("wins") or 0),
          "draws": int(away_form.get("draws") or 0),
          "losses": int(away_form.get("losses") or 0),
          "goals_for": int(away_form.get("goals_for") or 0),
          "goals_against": int(away_form.get("goals_against") or 0),
          "goals_for_per_game": round(_safe_per_game(away_form.get("goals_for"), away_played), 3),
          "goals_against_per_game": round(_safe_per_game(away_form.get("goals_against"), away_played), 3),
        },
      },
      "head_to_head": {
        "overall": {
          "matches": int(h2h_overall.get("matches") or 0),
          "home_team_wins": int(h2h_overall.get("home_team_wins") or 0),
          "away_team_wins": int(h2h_overall.get("away_team_wins") or 0),
          "draws": int(h2h_overall.get("draws") or 0),
          "home_team_goals": int(h2h_overall.get("home_team_goals") or 0),
          "away_team_goals": int(h2h_overall.get("away_team_goals") or 0),
        },
        "venue_specific": {
          "matches": venue_matches,
          "home_team_wins": int(h2h_venue.get("home_team_wins") or 0),
          "away_team_wins": int(h2h_venue.get("away_team_wins") or 0),
          "draws": int(h2h_venue.get("draws") or 0),
          "home_team_goals": int(h2h_venue.get("home_team_goals") or 0),
          "away_team_goals": int(h2h_venue.get("away_team_goals") or 0),
        },
        "recent_matches": [
          {
            "match_date": row["match_date"].isoformat() if hasattr(row.get("match_date"), "isoformat") else str(row.get("match_date")),
            "home_team": row.get("home_team"),
            "away_team": row.get("away_team"),
            "ft_home_goals": int(row.get("ft_home_goals") or 0),
            "ft_away_goals": int(row.get("ft_away_goals") or 0),
            "ft_result": row.get("ft_result"),
          }
          for row in recent_head_to_head
        ],
      },
    }
  finally:
    cur.close()
    conn.close()


def get_most_goals_in_season(
    db: DBConfig,
    league_code: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Return top season goals total per team (record rows; includes ties).

    Args:
        db: DB connection config.
        league_code: Optional league code filter (e.g. "I1", "E0").

    Returns:
        List of rows with keys:
        league_code, league_name, season_label, team_name, goals_scored
    """
    league_sql, params = _league_filter_sql(league_code)

    query = f"""
        WITH team_goals AS (
          SELECT
            m.league_id,
            m.season_id,
            m.home_team_id AS team_id,
            COALESCE(m.ft_home_goals, 0) AS goals
          FROM match_game m
          JOIN league l ON l.league_id = m.league_id
          {league_sql}

          UNION ALL

          SELECT
            m.league_id,
            m.season_id,
            m.away_team_id AS team_id,
            COALESCE(m.ft_away_goals, 0) AS goals
          FROM match_game m
          JOIN league l ON l.league_id = m.league_id
          {league_sql}
        ),
        season_totals AS (
          SELECT league_id, season_id, team_id, SUM(goals) AS goals_scored
          FROM team_goals
          GROUP BY league_id, season_id, team_id
        ),
        mx AS (
          SELECT MAX(goals_scored) AS m FROM season_totals
        )
        SELECT
          l.league_code,
          l.league_name,
          CONCAT(s.start_year, '-', RIGHT(s.end_year, 2)) AS season_label,
          t.team_name,
          st.goals_scored
        FROM season_totals st
        JOIN mx ON st.goals_scored = mx.m
        JOIN league l ON l.league_id = st.league_id
        JOIN season s ON s.season_id = st.season_id
        JOIN team t ON t.team_id = st.team_id
        ORDER BY season_label, t.team_name
    """

    # league filter appears twice in the UNION branches.
    all_params = params + params

    conn = _connect(db)
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(query, all_params)
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()


def get_most_points_in_season(
    db: DBConfig,
    league_code: Optional[str] = None,
    points_for_win: int = 3,
    points_for_draw: int = 1,
) -> List[Dict[str, Any]]:
    """Return top final points total per team-season (record rows; includes ties)."""
    league_sql, params = _league_filter_sql(league_code)

    query = f"""
        WITH team_rows AS (
          SELECT
            m.league_id,
            m.season_id,
            m.home_team_id AS team_id,
            CASE
              WHEN m.ft_home_goals > m.ft_away_goals THEN %s
              WHEN m.ft_home_goals = m.ft_away_goals THEN %s
              ELSE 0
            END AS pts
          FROM match_game m
          JOIN league l ON l.league_id = m.league_id
          {league_sql}
            {'AND' if league_sql else 'WHERE'} m.ft_home_goals IS NOT NULL
            AND m.ft_away_goals IS NOT NULL

          UNION ALL

          SELECT
            m.league_id,
            m.season_id,
            m.away_team_id AS team_id,
            CASE
              WHEN m.ft_away_goals > m.ft_home_goals THEN %s
              WHEN m.ft_away_goals = m.ft_home_goals THEN %s
              ELSE 0
            END AS pts
          FROM match_game m
          JOIN league l ON l.league_id = m.league_id
          {league_sql}
            {'AND' if league_sql else 'WHERE'} m.ft_home_goals IS NOT NULL
            AND m.ft_away_goals IS NOT NULL
        ),
        season_points AS (
          SELECT league_id, season_id, team_id, SUM(pts) AS points
          FROM team_rows
          GROUP BY league_id, season_id, team_id
        ),
        mx AS (
          SELECT MAX(points) AS m FROM season_points
        )
        SELECT
          l.league_code,
          l.league_name,
          CONCAT(s.start_year, '-', RIGHT(s.end_year, 2)) AS season_label,
          t.team_name,
          sp.points
        FROM season_points sp
        JOIN mx ON sp.points = mx.m
        JOIN league l ON l.league_id = sp.league_id
        JOIN season s ON s.season_id = sp.season_id
        JOIN team t ON t.team_id = sp.team_id
        ORDER BY season_label, t.team_name
    """

    all_params: Tuple[Any, ...] = (
        points_for_win,
        points_for_draw,
    ) + params + (
        points_for_win,
        points_for_draw,
    ) + params

    conn = _connect(db)
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(query, all_params)
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()


def get_lowest_points_in_season(
    db: DBConfig,
    league_code: Optional[str] = None,
    points_for_win: int = 3,
    points_for_draw: int = 1,
) -> List[Dict[str, Any]]:
    """Return lowest final points total per team-season (record rows; includes ties)."""
    league_sql, params = _league_filter_sql(league_code)

    query = f"""
        WITH team_rows AS (
          SELECT
            m.league_id,
            m.season_id,
            m.home_team_id AS team_id,
            CASE
              WHEN m.ft_home_goals > m.ft_away_goals THEN %s
              WHEN m.ft_home_goals = m.ft_away_goals THEN %s
              ELSE 0
            END AS pts
          FROM match_game m
          JOIN league l ON l.league_id = m.league_id
          {league_sql}
            {'AND' if league_sql else 'WHERE'} m.ft_home_goals IS NOT NULL
            AND m.ft_away_goals IS NOT NULL

          UNION ALL

          SELECT
            m.league_id,
            m.season_id,
            m.away_team_id AS team_id,
            CASE
              WHEN m.ft_away_goals > m.ft_home_goals THEN %s
              WHEN m.ft_away_goals = m.ft_home_goals THEN %s
              ELSE 0
            END AS pts
          FROM match_game m
          JOIN league l ON l.league_id = m.league_id
          {league_sql}
            {'AND' if league_sql else 'WHERE'} m.ft_home_goals IS NOT NULL
            AND m.ft_away_goals IS NOT NULL
        ),
        season_points AS (
          SELECT league_id, season_id, team_id, SUM(pts) AS points
          FROM team_rows
          GROUP BY league_id, season_id, team_id
        ),
        mn AS (
          SELECT MIN(points) AS m FROM season_points
        )
        SELECT
          l.league_code,
          l.league_name,
          CONCAT(s.start_year, '-', RIGHT(s.end_year, 2)) AS season_label,
          t.team_name,
          sp.points
        FROM season_points sp
        JOIN mn ON sp.points = mn.m
        JOIN league l ON l.league_id = sp.league_id
        JOIN season s ON s.season_id = sp.season_id
        JOIN team t ON t.team_id = sp.team_id
        ORDER BY season_label, t.team_name
    """

    all_params: Tuple[Any, ...] = (
        points_for_win,
        points_for_draw,
    ) + params + (
        points_for_win,
        points_for_draw,
    ) + params

    conn = _connect(db)
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(query, all_params)
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()


def get_longest_title_streak(
    db: DBConfig,
    league_code: str,
    points_for_win: int = 3,
    points_for_draw: int = 1,
    tie_breakers: Sequence[str] = ("points", "goal_diff", "goals_for", "team_id"),
) -> List[Dict[str, Any]]:
    """Return longest consecutive-title streak(s) for a league.

    Args:
        db: DB connection config.
        league_code: League code, e.g. "E0" (Premier League), "I1" (Serie A).
        points_for_win: Win points for ranking model.
        points_for_draw: Draw points for ranking model.
        tie_breakers: Ordered ranking tie breakers from:
            points, goal_diff, goals_for, team_id

    Returns:
        List of rows with keys:
        league_code, league_name, team_name, titles_in_row, from_season, to_season
        (multiple rows only if tied streak length)
    """
    order_by = _order_clause(tie_breakers)

    query = f"""
        WITH team_rows AS (
          SELECT
            m.league_id,
            m.season_id,
            m.home_team_id AS team_id,
            CASE
              WHEN m.ft_home_goals > m.ft_away_goals THEN %s
              WHEN m.ft_home_goals = m.ft_away_goals THEN %s
              ELSE 0
            END AS pts,
            COALESCE(m.ft_home_goals, 0) AS gf,
            COALESCE(m.ft_away_goals, 0) AS ga
          FROM match_game m
          JOIN league l ON l.league_id = m.league_id
          WHERE l.league_code = %s
            AND m.ft_home_goals IS NOT NULL
            AND m.ft_away_goals IS NOT NULL

          UNION ALL

          SELECT
            m.league_id,
            m.season_id,
            m.away_team_id AS team_id,
            CASE
              WHEN m.ft_away_goals > m.ft_home_goals THEN %s
              WHEN m.ft_away_goals = m.ft_home_goals THEN %s
              ELSE 0
            END AS pts,
            COALESCE(m.ft_away_goals, 0) AS gf,
            COALESCE(m.ft_home_goals, 0) AS ga
          FROM match_game m
          JOIN league l ON l.league_id = m.league_id
          WHERE l.league_code = %s
            AND m.ft_home_goals IS NOT NULL
            AND m.ft_away_goals IS NOT NULL
        ),
        standings AS (
          SELECT
            league_id,
            season_id,
            team_id,
            SUM(pts) AS points,
            SUM(gf) AS goals_for,
            SUM(ga) AS goals_against,
            SUM(gf) - SUM(ga) AS goal_diff
          FROM team_rows
          GROUP BY league_id, season_id, team_id
        ),
        champions AS (
          SELECT league_id, season_id, team_id
          FROM (
            SELECT
              s.*,
              ROW_NUMBER() OVER (
                PARTITION BY s.league_id, s.season_id
                ORDER BY {order_by}
              ) AS rn
            FROM standings s
          ) ranked
          WHERE rn = 1
        ),
        champs_with_year AS (
          SELECT
            c.league_id,
            c.team_id,
            se.start_year,
            se.end_year
          FROM champions c
          JOIN season se ON se.season_id = c.season_id
        ),
        seq AS (
          SELECT
            cwy.*,
            (cwy.start_year - ROW_NUMBER() OVER (
              PARTITION BY cwy.team_id
              ORDER BY cwy.start_year
            )) AS grp
          FROM champs_with_year cwy
        ),
        streaks AS (
          SELECT
            league_id,
            team_id,
            MIN(start_year) AS streak_start,
            MAX(start_year) AS streak_end_startyear,
            COUNT(*) AS titles_in_row
          FROM seq
          GROUP BY league_id, team_id, grp
        ),
        mx AS (
          SELECT MAX(titles_in_row) AS m FROM streaks
        )
        SELECT
          l.league_code,
          l.league_name,
          t.team_name,
          s.titles_in_row,
          CONCAT(s.streak_start, '-', RIGHT(s.streak_start + 1, 2)) AS from_season,
          CONCAT(s.streak_end_startyear, '-', RIGHT(s.streak_end_startyear + 1, 2)) AS to_season
        FROM streaks s
        JOIN mx ON s.titles_in_row = mx.m
        JOIN team t ON t.team_id = s.team_id
        JOIN league l ON l.league_id = s.league_id
        ORDER BY s.streak_start, t.team_name
    """

    params: Tuple[Any, ...] = (
        points_for_win,
        points_for_draw,
        league_code,
        points_for_win,
        points_for_draw,
        league_code,
    )

    conn = _connect(db)
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(query, params)
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()


def get_best_away_record(
    db: DBConfig,
    league_code: str,
    seasons_back: Optional[int] = None,
    points_for_win: int = 3,
    points_for_draw: int = 1,
) -> List[Dict[str, Any]]:
    """Return best away season record for a league (includes ties).

    The record is defined by maximum away points in a season.
    Tie-breakers: away goal difference, away goals scored, team_id.

    Args:
        db: DB connection config.
        league_code: League code, e.g. "E0", "I1".
        seasons_back: Optional rolling window by season start_year.
            Example: 15 means evaluate only the most recent 15 season start years
            available in the selected league.
        points_for_win: Win points in away standings model.
        points_for_draw: Draw points in away standings model.

    Returns:
        List of rows with keys:
        league_code, league_name, season_label, team_name,
        away_points, away_wins, away_draws, away_losses,
        away_goals_for, away_goals_against, away_goal_diff
    """
    params: List[Any] = [league_code]
    window_sql = ""

    if seasons_back is not None:
        if seasons_back < 1:
            raise ValueError("seasons_back must be >= 1")
        window_sql = """
          AND se.start_year >= (
            SELECT MAX(se2.start_year) - %s + 1
            FROM match_game m2
            JOIN season se2 ON se2.season_id = m2.season_id
            JOIN league l2 ON l2.league_id = m2.league_id
            WHERE l2.league_code = %s
          )
        """
        params.extend([seasons_back, league_code])

    query = f"""
        WITH away_rows AS (
          SELECT
            m.league_id,
            m.season_id,
            m.away_team_id AS team_id,
            CASE
              WHEN m.ft_away_goals > m.ft_home_goals THEN %s
              WHEN m.ft_away_goals = m.ft_home_goals THEN %s
              ELSE 0
            END AS pts,
            CASE WHEN m.ft_away_goals > m.ft_home_goals THEN 1 ELSE 0 END AS w,
            CASE WHEN m.ft_away_goals = m.ft_home_goals THEN 1 ELSE 0 END AS d,
            CASE WHEN m.ft_away_goals < m.ft_home_goals THEN 1 ELSE 0 END AS l,
            COALESCE(m.ft_away_goals, 0) AS gf,
            COALESCE(m.ft_home_goals, 0) AS ga
          FROM match_game m
          JOIN league l ON l.league_id = m.league_id
          JOIN season se ON se.season_id = m.season_id
          WHERE l.league_code = %s
            AND m.ft_home_goals IS NOT NULL
            AND m.ft_away_goals IS NOT NULL
            {window_sql}
        ),
        away_totals AS (
          SELECT
            league_id,
            season_id,
            team_id,
            SUM(pts) AS away_points,
            SUM(w) AS away_wins,
            SUM(d) AS away_draws,
            SUM(l) AS away_losses,
            SUM(gf) AS away_goals_for,
            SUM(ga) AS away_goals_against,
            SUM(gf) - SUM(ga) AS away_goal_diff
          FROM away_rows
          GROUP BY league_id, season_id, team_id
        ),
        mx AS (
          SELECT MAX(away_points) AS m FROM away_totals
        )
        SELECT
          lg.league_code,
          lg.league_name,
          CONCAT(se.start_year, '-', RIGHT(se.end_year, 2)) AS season_label,
          tm.team_name,
          at.away_points,
          at.away_wins,
          at.away_draws,
          at.away_losses,
          at.away_goals_for,
          at.away_goals_against,
          at.away_goal_diff
        FROM away_totals at
        JOIN mx ON at.away_points = mx.m
        JOIN league lg ON lg.league_id = at.league_id
        JOIN season se ON se.season_id = at.season_id
        JOIN team tm ON tm.team_id = at.team_id
        ORDER BY at.away_goal_diff DESC, at.away_goals_for DESC, tm.team_name ASC
    """

    all_params: Tuple[Any, ...] = (
        points_for_win,
        points_for_draw,
        *params,
    )

    conn = _connect(db)
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(query, all_params)
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()


def get_premier_league_longest_title_streak(db: DBConfig) -> List[Dict[str, Any]]:
    """Convenience wrapper for Premier League (league_code='E0')."""
    return get_longest_title_streak(db=db, league_code="E0")
