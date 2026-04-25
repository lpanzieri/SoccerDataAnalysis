from __future__ import annotations

import base64
import os
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import matplotlib
import matplotlib.pyplot as plt
import mysql.connector

matplotlib.use("Agg")  # For headless environments

EXPORT_WIDTH_INCHES = 16.0
EXPORT_HEIGHT_INCHES = 9.0
EXPORT_DPI = 240

# Mapping from Bet365-style league codes to API-Football provider league IDs.
# These are the IDs used in event_fixture, player_match_stats, event_goal, etc.
LEAGUE_CODE_TO_PROVIDER_ID: Dict[str, int] = {
    "E0": 39,    # Premier League
    "I1": 135,   # Serie A
    "SP1": 140,  # La Liga
    "D1": 78,    # Bundesliga
    "F1": 61,    # Ligue 1
    "N1": 88,    # Eredivisie
}

PROVIDER_ID_TO_LEAGUE_CODE: Dict[int, str] = {
    v: k for k, v in LEAGUE_CODE_TO_PROVIDER_ID.items()
}

# Finished-match status codes used in event_fixture.status_short
FINISHED_STATUSES = ("FT", "AET", "PEN", "FT_PEN", "AWD", "WO")


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

    cur.execute(
      """
      SELECT COUNT(*) AS cnt
      FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA = DATABASE()
        AND TABLE_NAME = 'team'
        AND COLUMN_NAME = 'badge_path'
      """
    )
    schema_row = cur.fetchone() or {}
    has_team_badge_path = bool(schema_row.get("cnt", 0))

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

    team_goals = {name: [] for name in team_names}
    for season in seasons:
      season_id = season["season_id"]
      for name in team_names:
        tid = team_ids.get(name)
        if not tid:
          team_goals[name].append(0)
          continue
        cur.execute(
          """
          SELECT SUM(
            CASE
              WHEN m.home_team_id = %s THEN COALESCE(m.ft_home_goals, 0)
              WHEN m.away_team_id = %s THEN COALESCE(m.ft_away_goals, 0)
              ELSE 0
            END
          ) AS goals
          FROM match_game m
          JOIN league l ON l.league_id = m.league_id
          WHERE m.season_id = %s
            AND l.league_code = %s
            AND (m.home_team_id = %s OR m.away_team_id = %s)
          """,
          (tid, tid, season_id, league_code, tid, tid),
        )
        row = cur.fetchone()
        team_goals[name].append(row["goals"] if row and row["goals"] is not None else 0)

    plt.figure(figsize=(EXPORT_WIDTH_INCHES, EXPORT_HEIGHT_INCHES))
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
      try:
        if isinstance(badge_raw, (bytes, bytearray, memoryview)):
          arr_img = plt.imread(BytesIO(bytes(badge_raw)))
        else:
          badge_path = _resolve_badge_path(str(badge_raw) if badge_raw is not None else None)
          if not badge_path:
            continue
          arr_img = plt.imread(badge_path)
      except Exception:
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
    plt.savefig(image_path, dpi=EXPORT_DPI)

    buf = BytesIO()
    plt.savefig(buf, format="png", dpi=EXPORT_DPI)
    buf.seek(0)
    base64_image = base64.b64encode(buf.read()).decode("utf-8")
    plt.close()
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


# ---------------------------------------------------------------------------
# event_fixture-backed helpers (use API-Football data synced by
# sync_api_football_events.py: event_fixture, player_match_stats, event_goal)
# ---------------------------------------------------------------------------

def _ef_league_filter(league_code: Optional[str]) -> Tuple[str, Tuple[Any, ...]]:
    """Return a WHERE fragment and params for filtering event_fixture by league_code."""
    if league_code and league_code in LEAGUE_CODE_TO_PROVIDER_ID:
        provider_id = LEAGUE_CODE_TO_PROVIDER_ID[league_code]
        return "AND ef.league_id = %s", (provider_id,)
    return "", ()


def get_top_scorers(
    db: DBConfig,
    league_code: Optional[str] = None,
    seasons_back: int = 3,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    """Return top goal scorers from player_match_stats across recent seasons.

    Uses event_fixture + player_match_stats (API-Football data).

    Args:
        db: DB connection config.
        league_code: Optional Bet365-style code (e.g. "I1", "E0"). None = all leagues.
        seasons_back: How many recent completed seasons to include.
        limit: Maximum rows to return.

    Returns:
        List of rows: player_name, total_goals, total_assists, matches_played,
        goals_per_match, league_ids.
    """
    league_fragment, league_params = _ef_league_filter(league_code)

    query = f"""
        SELECT
            pd.player_name,
            SUM(pms.goals_scored)  AS total_goals,
            SUM(pms.assists)       AS total_assists,
            COUNT(DISTINCT ef.provider_fixture_id) AS matches_played,
            ROUND(SUM(pms.goals_scored) /
                  NULLIF(COUNT(DISTINCT ef.provider_fixture_id), 0), 2) AS goals_per_match,
            GROUP_CONCAT(DISTINCT ef.league_id ORDER BY ef.league_id SEPARATOR ',') AS league_ids
        FROM player_match_stats pms
        JOIN event_fixture ef ON ef.provider_fixture_id = pms.provider_fixture_id
        JOIN player_dim pd ON pd.provider_player_id = pms.provider_player_id
        WHERE ef.status_short IN ('FT','AET','PEN','FT_PEN','AWD','WO')
          AND ef.season_year >= YEAR(CURDATE()) - %s
          AND pms.goals_scored > 0
          {league_fragment}
        GROUP BY pms.provider_player_id, pd.player_name
        ORDER BY total_goals DESC
        LIMIT %s
    """

    params = (seasons_back,) + league_params + (limit,)
    conn = _connect(db)
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(query, params)
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()


def get_top_assisters(
    db: DBConfig,
    league_code: Optional[str] = None,
    seasons_back: int = 3,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    """Return top assist providers from player_match_stats across recent seasons."""
    league_fragment, league_params = _ef_league_filter(league_code)

    query = f"""
        SELECT
            pd.player_name,
            SUM(pms.assists)       AS total_assists,
            SUM(pms.goals_scored)  AS total_goals,
            COUNT(DISTINCT ef.provider_fixture_id) AS matches_played,
            ROUND(SUM(pms.assists) /
                  NULLIF(COUNT(DISTINCT ef.provider_fixture_id), 0), 2) AS assists_per_match
        FROM player_match_stats pms
        JOIN event_fixture ef ON ef.provider_fixture_id = pms.provider_fixture_id
        JOIN player_dim pd ON pd.provider_player_id = pms.provider_player_id
        WHERE ef.status_short IN ('FT','AET','PEN','FT_PEN','AWD','WO')
          AND ef.season_year >= YEAR(CURDATE()) - %s
          AND pms.assists > 0
          {league_fragment}
        GROUP BY pms.provider_player_id, pd.player_name
        ORDER BY total_assists DESC
        LIMIT %s
    """

    params = (seasons_back,) + league_params + (limit,)
    conn = _connect(db)
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(query, params)
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()


def get_goals_per_match_leaders(
    db: DBConfig,
    league_code: Optional[str] = None,
    seasons_back: int = 3,
    min_matches: int = 10,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    """Return players with the best goals-per-match rate (min_matches threshold)."""
    league_fragment, league_params = _ef_league_filter(league_code)

    query = f"""
        SELECT
            pd.player_name,
            SUM(pms.goals_scored)  AS total_goals,
            COUNT(DISTINCT ef.provider_fixture_id) AS matches_played,
            ROUND(SUM(pms.goals_scored) /
                  NULLIF(COUNT(DISTINCT ef.provider_fixture_id), 0), 3) AS goals_per_match
        FROM player_match_stats pms
        JOIN event_fixture ef ON ef.provider_fixture_id = pms.provider_fixture_id
        JOIN player_dim pd ON pd.provider_player_id = pms.provider_player_id
        WHERE ef.status_short IN ('FT','AET','PEN','FT_PEN','AWD','WO')
          AND ef.season_year >= YEAR(CURDATE()) - %s
          {league_fragment}
        GROUP BY pms.provider_player_id, pd.player_name
        HAVING matches_played >= %s
        ORDER BY goals_per_match DESC
        LIMIT %s
    """

    params = (seasons_back,) + league_params + (min_matches, limit)
    conn = _connect(db)
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(query, params)
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()


def get_goals_per_match_by_league(
    db: DBConfig,
    league_code: Optional[str] = None,
    seasons_back: int = 3,
) -> List[Dict[str, Any]]:
    """Return average goals per match per league and season (from event_goal)."""
    league_fragment, league_params = _ef_league_filter(league_code)

    query = f"""
        SELECT
            ef.league_id,
            ef.season_year,
            COUNT(DISTINCT ef.provider_fixture_id) AS matches,
            COUNT(eg.goal_id) AS total_goals,
            ROUND(COUNT(eg.goal_id) /
                  NULLIF(COUNT(DISTINCT ef.provider_fixture_id), 0), 2) AS goals_per_match
        FROM event_fixture ef
        LEFT JOIN event_goal eg
            ON eg.provider_fixture_id = ef.provider_fixture_id
            AND eg.event_type = 'Goal'
            AND eg.event_detail != 'Own Goal'
        WHERE ef.status_short IN ('FT','AET','PEN','FT_PEN','AWD','WO')
          AND ef.season_year >= YEAR(CURDATE()) - %s
          {league_fragment}
        GROUP BY ef.league_id, ef.season_year
        ORDER BY ef.league_id, ef.season_year
    """

    params = (seasons_back,) + league_params
    conn = _connect(db)
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(query, params)
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()


def get_most_goals_in_season_ef(
    db: DBConfig,
    league_code: Optional[str] = None,
    seasons_back: int = 10,
) -> List[Dict[str, Any]]:
    """Return the season with the highest total goals scored (from event_goal).

    Uses event_fixture pipeline data instead of legacy match_game.
    """
    league_fragment, league_params = _ef_league_filter(league_code)

    query = f"""
        WITH season_totals AS (
            SELECT
                ef.league_id,
                ef.season_year,
                COUNT(eg.goal_id) AS total_goals,
                COUNT(DISTINCT ef.provider_fixture_id) AS matches
            FROM event_fixture ef
            LEFT JOIN event_goal eg
                ON eg.provider_fixture_id = ef.provider_fixture_id
                AND eg.event_type = 'Goal'
                AND eg.event_detail != 'Own Goal'
            WHERE ef.status_short IN ('FT','AET','PEN','FT_PEN','AWD','WO')
              AND ef.season_year >= YEAR(CURDATE()) - %s
              {league_fragment}
            GROUP BY ef.league_id, ef.season_year
        ),
        mx AS (SELECT MAX(total_goals) AS m FROM season_totals)
        SELECT st.league_id, st.season_year, st.total_goals, st.matches,
               ROUND(st.total_goals / NULLIF(st.matches, 0), 2) AS goals_per_match
        FROM season_totals st
        JOIN mx ON st.total_goals = mx.m
        ORDER BY st.season_year
    """

    params = (seasons_back,) + league_params
    conn = _connect(db)
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(query, params)
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()
