#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import matplotlib

matplotlib.use("Agg")

import matplotlib.colors as mcolors
import matplotlib.offsetbox as offsetbox
import matplotlib.pyplot as plt
import mysql.connector
import numpy as np

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.helpers.api_football_guard import ApiGuard, api_get_json

EXPORT_WIDTH_INCHES = 16.0
EXPORT_DPI = 240
API_BASE = "https://v3.football.api-sports.io"


@dataclass
class DBConfig:
    host: str
    port: int
    user: str
    password: str
    database: str


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate local top-scorers graphical report")
    p.add_argument("--host", default=os.getenv("MYSQL_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.getenv("MYSQL_PORT", "3306")))
    p.add_argument("--user", default=os.getenv("MYSQL_USER", "root"))
    p.add_argument("--database", default=os.getenv("MYSQL_DATABASE", "historic_football_data"))
    p.add_argument("--mysql-password-env", default="MYSQL_PASSWORD")
    p.add_argument("--top-n", type=int, default=10)
    p.add_argument("--image-dir", default="generated_graphs")
    p.add_argument("--badge-zoom", type=float, default=0.085)
    p.add_argument("--api-key-env", default="APIFOOTBALL_KEY")
    return p.parse_args()


def connect_db(cfg: DBConfig):
    return mysql.connector.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database,
        charset="utf8mb4",
        use_unicode=True,
        use_pure=True,
        autocommit=True,
    )


def fetch_top_rows(cur, top_n: int) -> List[Tuple[str, int, str, str, int, int, int]]:
    cur.execute(
        """
        SELECT
            COALESCE(MAX(eg.player_name), 'UNKNOWN') AS player_name,
            ef.season_year,
            COALESCE(MAX(eg.team_name), 'UNKNOWN') AS team_name,
            COALESCE(MAX(ef.league_name), CONCAT('League ', ef.league_id)) AS league_name,
            ef.league_id,
            COUNT(*) AS goals_including_penalties,
            MAX(eg.team_id) AS provider_team_id
        FROM event_goal eg
        JOIN event_fixture ef ON ef.provider_fixture_id = eg.provider_fixture_id
        WHERE eg.player_id IS NOT NULL
          AND (eg.event_detail IS NULL OR LOWER(eg.event_detail) NOT IN ('own goal', 'missed penalty'))
        GROUP BY eg.player_id, ef.season_year, ef.league_id, eg.team_id
        ORDER BY goals_including_penalties DESC, player_name ASC, ef.season_year DESC
        LIMIT %s
        """,
        (top_n,),
    )
    out: List[Tuple[str, int, str, str, int, int, int]] = []
    for player_name, season_year, team_name, league_name, league_id, goals, provider_team_id in cur.fetchall():
        out.append(
            (
                str(player_name),
                int(season_year),
                str(team_name),
                str(league_name),
                int(league_id),
                int(goals),
                int(provider_team_id) if provider_team_id is not None else -1,
            )
        )
    return out


def fetch_badges(cur, team_ids: List[int]) -> Dict[int, Optional[bytes]]:
    valid_ids = [tid for tid in team_ids if tid > 0]
    if not valid_ids:
        return {}

    placeholders = ", ".join(["%s"] * len(valid_ids))
    cur.execute(
        f"""
        SELECT provider_team_id, badge_image
        FROM (
            SELECT
                tb.provider_team_id,
                tb.badge_image,
                ROW_NUMBER() OVER (
                    PARTITION BY tb.provider_team_id
                    ORDER BY tb.season_year DESC, tb.updated_at DESC
                ) AS rn
            FROM team_badge tb
            WHERE tb.provider_team_id IN ({placeholders})
        ) ranked
        WHERE rn = 1
        """,
        valid_ids,
    )
    return {
        int(team_id): bytes(badge_image) if badge_image is not None else None
        for team_id, badge_image in cur.fetchall()
    }


def api_get_team_badge(guard: ApiGuard, api_key: str, provider_team_id: int) -> Optional[Tuple[str, bytes, str, str]]:
    code, payload, headers = api_get_json(
        guard=guard,
        api_key=api_key,
        path="/teams",
        params={"id": str(provider_team_id)},
        timeout_seconds=30,
    )
    if code == 429:
        waited = guard.sleep_retry_window(headers)
        guard.log_call(path="/teams", code=code, headers=headers, action=f"backoff-{waited}s")
        print(f"WARN: rate limited while fetching badge for team {provider_team_id}. Waited {waited}s.")
        return None
    if code != 200:
        return None
    if guard.should_stop_from_remaining(headers):
        guard.log_call(path="/teams", code=code, headers=headers, action="stop-reserve")
        print("WARN: stopping badge API pulls due to low remaining quota reserve.")
        return None

    response = payload.get("response") or []
    if not response:
        return None

    team = response[0].get("team") or {}
    badge_url = team.get("logo")
    team_name = team.get("name") or ""
    if not badge_url:
        return None

    img_req = urllib.request.Request(
        str(badge_url),
        headers={"User-Agent": "top-scorers-report/1.0"},
        method="GET",
    )
    with urllib.request.urlopen(img_req, timeout=30) as img_resp:
        image = img_resp.read()
        content_type = img_resp.headers.get("Content-Type", "application/octet-stream")
    return str(badge_url), image, str(content_type), str(team_name)


def upsert_badge(
    conn,
    provider_team_id: int,
    team_name: str,
    league_id: int,
    league_name: str,
    season_year: int,
    badge_url: str,
    badge_image: bytes,
    content_type: str,
) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO team_badge (
                provider_team_id, team_name, league_id, league_name, season_year,
                badge_url, badge_image, content_type, image_size_bytes, image_sha256
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,SHA2(%s, 256))
            ON DUPLICATE KEY UPDATE
                team_name = VALUES(team_name),
                league_name = VALUES(league_name),
                badge_url = VALUES(badge_url),
                badge_image = VALUES(badge_image),
                content_type = VALUES(content_type),
                image_size_bytes = VALUES(image_size_bytes),
                image_sha256 = VALUES(image_sha256)
            """,
            (
                provider_team_id,
                team_name,
                league_id,
                league_name,
                season_year,
                badge_url,
                badge_image,
                content_type,
                len(badge_image),
                badge_image,
            ),
        )
    finally:
        cur.close()


def backfill_missing_badges_from_api(
    conn,
    rows: List[Tuple[str, int, str, str, int, int, int]],
    existing_badges: Dict[int, Optional[bytes]],
    api_key: str,
) -> Dict[int, Optional[bytes]]:
    if not api_key:
        return existing_badges

    guard = ApiGuard.from_env(user_agent="top-scorers-report/1.0")

    for _, season_year, team_name, league_name, league_id, _, provider_team_id in rows:
        if provider_team_id <= 0 or existing_badges.get(provider_team_id):
            continue
        try:
            resolved = api_get_team_badge(guard, api_key, provider_team_id)
            if not resolved:
                continue
            badge_url, badge_image, content_type, api_team_name = resolved
            if not badge_image:
                continue
            upsert_badge(
                conn=conn,
                provider_team_id=provider_team_id,
                team_name=api_team_name or team_name,
                league_id=league_id,
                league_name=league_name,
                season_year=season_year,
                badge_url=badge_url,
                badge_image=badge_image,
                content_type=content_type,
            )
            existing_badges[provider_team_id] = badge_image
            time.sleep(0.05)
        except Exception as exc:
            print(f"WARN: badge API backfill failed for {team_name} ({provider_team_id}): {exc}")
    return existing_badges


def _team_initials(team_name: str) -> str:
    parts = [p for p in team_name.replace("-", " ").split() if p]
    if not parts:
        return "?"
    if len(parts) == 1:
        return parts[0][:2].upper()
    return (parts[0][0] + parts[-1][0]).upper()


def render_report(
    rows: List[Tuple[str, int, str, str, int, int, int]],
    badges: Dict[int, Optional[bytes]],
    badge_zoom: float,
    output_path: str,
) -> None:
    if not rows:
        raise RuntimeError("No scorer rows found")

    labels = [f"{name} ({season})\n{team} - {league}" for name, season, team, league, _, _, _ in rows]
    values = np.array([goals for _, _, _, _, _, goals, _ in rows], dtype=float)
    team_ids = [team_id for _, _, _, _, _, _, team_id in rows]
    team_names = [team for _, _, team, _, _, _, _ in rows]

    fig_height = max(9.0, 0.85 * len(rows))
    fig, ax = plt.subplots(figsize=(EXPORT_WIDTH_INCHES, fig_height))
    fig.patch.set_facecolor("#f6f2eb")
    ax.set_facecolor("#f6f2eb")

    cmap = mcolors.LinearSegmentedColormap.from_list(
        "teal_gold",
        ["#1d8f8d", "#65b7a5", "#e7be62"],
        N=256,
    )
    bar_colors = [cmap(idx / max(1, len(rows) - 1)) for idx in range(len(rows))]

    y = np.arange(len(rows))
    bars = ax.barh(y, values, color=bar_colors, edgecolor="#2f2b30", linewidth=0.5)

    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=10, color="#2b2430")
    ax.tick_params(axis="y", pad=24)
    ax.invert_yaxis()

    ax.set_xlabel("Goals (including penalties)", fontsize=12, color="#2b2430")
    ax.set_title("Top 10 Scorer Seasons (Local DB)", fontsize=18, fontweight="bold", color="#1f1725", pad=16)

    max_v = max(values) if len(values) else 0
    ax.set_xlim(0, max_v + 5)
    ax.grid(axis="x", color="#ddd3c9", linewidth=0.8, alpha=0.8)

    for bar, v in zip(bars, values):
        ax.text(
            v + 0.4,
            bar.get_y() + bar.get_height() / 2,
            f"{int(v)}",
            va="center",
            ha="left",
            fontsize=10.5,
            color="#1f1725",
            fontweight="bold",
        )

    fig.canvas.draw()
    for row_idx, team_id in enumerate(team_ids):
        badge_blob = badges.get(team_id)
        if badge_blob:
            try:
                badge_img = plt.imread(BytesIO(badge_blob))
                badge_artist = offsetbox.AnnotationBbox(
                    offsetbox.OffsetImage(badge_img, zoom=badge_zoom),
                    (-0.03, row_idx),
                    xycoords=ax.get_yaxis_transform(),
                    frameon=False,
                    box_alignment=(0.5, 0.5),
                    pad=0,
                    annotation_clip=False,
                    zorder=8,
                )
                ax.add_artist(badge_artist)
                continue
            except Exception:
                pass

        # Fallback marker when no team badge blob is available in local DB.
        initials = _team_initials(team_names[row_idx])
        ax.text(
            -0.03,
            row_idx,
            initials,
            transform=ax.get_yaxis_transform(),
            ha="center",
            va="center",
            fontsize=8.5,
            color="#2b2430",
            fontweight="bold",
            bbox={
                "boxstyle": "circle,pad=0.28",
                "facecolor": "#dac6a9",
                "edgecolor": "#8b7a6b",
                "linewidth": 1.0,
            },
            clip_on=False,
            zorder=9,
        )

    fig.text(
        0.13,
        0.94,
        "Computed from local event_goal + event_fixture only; excludes own goals and missed penalties.",
        ha="left",
        va="center",
        fontsize=10,
        color="#584a5f",
    )

    plt.subplots_adjust(left=0.33, right=0.95, top=0.89, bottom=0.08)
    fig.savefig(output_path, dpi=EXPORT_DPI, facecolor=fig.get_facecolor())
    plt.close(fig)


def main() -> None:
    args = parse_args()
    password = os.getenv(args.mysql_password_env, "")
    if not password:
        raise SystemExit(f"{args.mysql_password_env} is required")

    conn = connect_db(
        DBConfig(
            host=args.host,
            port=args.port,
            user=args.user,
            password=password,
            database=args.database,
        )
    )
    cur = conn.cursor()
    try:
        rows = fetch_top_rows(cur, args.top_n)
        badges = fetch_badges(cur, [row[6] for row in rows])
        badges = backfill_missing_badges_from_api(
            conn=conn,
            rows=rows,
            existing_badges=badges,
            api_key=os.getenv(args.api_key_env, ""),
        )

        os.makedirs(args.image_dir, exist_ok=True)
        output_path = os.path.join(args.image_dir, "top10_scorers_report_local.png")
        render_report(rows=rows, badges=badges, badge_zoom=args.badge_zoom, output_path=output_path)
        print(output_path)
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
