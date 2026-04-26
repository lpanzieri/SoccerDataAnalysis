from __future__ import annotations

import base64
import hashlib
import os
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import matplotlib
import matplotlib.pyplot as plt
import mysql.connector

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
