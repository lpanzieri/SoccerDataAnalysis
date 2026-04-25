#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
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
from scripts.helpers.cuda_runtime import resolve_compute_backend

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
    parser = argparse.ArgumentParser(description="Generate a 5-minute goal heatmap by team")
    parser.add_argument("--host", default=os.getenv("MYSQL_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("MYSQL_PORT", "3306")))
    parser.add_argument("--user", default=os.getenv("MYSQL_USER", "root"))
    parser.add_argument("--database", default=os.getenv("MYSQL_DATABASE", "historic_football_data"))
    parser.add_argument("--mysql-password-env", default="MYSQL_PASSWORD")
    parser.add_argument("--league-id", type=int, default=135)
    parser.add_argument("--season-year", type=int, default=0)
    parser.add_argument("--image-dir", default="generated_graphs")
    parser.add_argument("--badge-zoom", type=float, default=0.085)
    parser.add_argument("--api-key-env", default="APIFOOTBALL_KEY")
    parser.add_argument("--disable-live-badge-fallback", action="store_true")
    parser.add_argument(
        "--compute-backend",
        choices=("auto", "cpu", "cuda"),
        default=os.getenv("COMPUTE_BACKEND", "auto"),
        help="Requested compute backend. Phase 1 keeps execution on CPU with optional CUDA detection.",
    )
    return parser.parse_args()


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


def slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_") or "heatmap"


def resolve_season_year(cur, league_id: int, requested_season: int) -> int:
    if requested_season > 0:
        return requested_season
    cur.execute(
        "SELECT MAX(season_year) FROM event_fixture WHERE league_id = %s",
        (league_id,),
    )
    row = cur.fetchone()
    if not row or row[0] is None:
        raise RuntimeError(f"No fixtures found for league_id={league_id}")
    return int(row[0])


def get_league_name(cur, league_id: int, season_year: int) -> str:
    cur.execute(
        """
        SELECT COALESCE(MAX(league_name), CONCAT('League ', %s))
        FROM event_fixture
        WHERE league_id = %s AND season_year = %s
        """,
        (league_id, league_id, season_year),
    )
    row = cur.fetchone()
    return str(row[0]) if row and row[0] else f"League {league_id}"


def fetch_team_rows(cur, league_id: int, season_year: int) -> List[Tuple[int, str]]:
    cur.execute(
        """
        SELECT provider_team_id, canonical_team_name
        FROM team_provider_dim
        WHERE provider_team_id IN (
            SELECT home_team_id FROM event_fixture WHERE league_id = %s AND season_year = %s
            UNION
            SELECT away_team_id FROM event_fixture WHERE league_id = %s AND season_year = %s
        )
        ORDER BY canonical_team_name ASC
        """,
        (league_id, season_year, league_id, season_year),
    )
    return [(int(provider_team_id), str(team_name)) for provider_team_id, team_name in cur.fetchall()]


def fetch_goal_bins(cur, league_id: int, season_year: int) -> Dict[int, Dict[int, int]]:
    cur.execute(
        """
        SELECT
            eg.team_id,
            CASE
                WHEN COALESCE(eg.elapsed_minute, 0) >= 90 THEN 18
                ELSE FLOOR(COALESCE(eg.elapsed_minute, 0) / 5)
            END AS bin_idx,
            COUNT(*) AS goals
        FROM event_goal eg
        JOIN event_fixture ef ON ef.provider_fixture_id = eg.provider_fixture_id
        WHERE ef.league_id = %s
          AND ef.season_year = %s
          AND eg.team_id IS NOT NULL
        GROUP BY eg.team_id, bin_idx
        """,
        (league_id, season_year),
    )
    by_team: Dict[int, Dict[int, int]] = {}
    for team_id, bin_idx, goals in cur.fetchall():
        team_bucket = by_team.setdefault(int(team_id), {})
        team_bucket[int(bin_idx)] = int(goals)
    return by_team


def fetch_badges(cur, league_id: int, season_year: int, team_ids: List[int]) -> Dict[int, Optional[bytes]]:
    if not team_ids:
        return {}
    placeholders = ", ".join(["%s"] * len(team_ids))
    cur.execute(
        f"""
        SELECT provider_team_id, badge_image
        FROM (
            SELECT
                tb.provider_team_id,
                tb.badge_image,
                ROW_NUMBER() OVER (
                    PARTITION BY tb.provider_team_id
                    ORDER BY
                        CASE WHEN tb.league_id = %s AND tb.season_year = %s THEN 0 ELSE 1 END,
                        tb.season_year DESC,
                        tb.updated_at DESC
                ) AS rn
            FROM team_badge tb
            WHERE tb.provider_team_id IN ({placeholders})
        ) ranked
        WHERE rn = 1
        """,
        [league_id, season_year, *team_ids],
    )
    return {int(team_id): bytes(badge_image) if badge_image is not None else None for team_id, badge_image in cur.fetchall()}


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
        headers={"User-Agent": "goal-heatmap/1.0"},
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
    league_id: int,
    league_name: str,
    season_year: int,
    labels: List[str],
    team_ids: List[int],
    existing_badges: Dict[int, Optional[bytes]],
    api_key: str,
) -> Dict[int, Optional[bytes]]:
    if not api_key:
        return existing_badges

    guard = ApiGuard.from_env(user_agent="goal-heatmap/1.0")

    name_by_team_id = {team_id: labels[idx] for idx, team_id in enumerate(team_ids)}
    missing_ids = [team_id for team_id in team_ids if not existing_badges.get(team_id)]
    if not missing_ids:
        return existing_badges

    for provider_team_id in missing_ids:
        try:
            resolved = api_get_team_badge(guard, api_key, provider_team_id)
            if not resolved:
                continue
            badge_url, badge_image, content_type, api_team_name = resolved
            if not badge_image:
                continue
            team_name = api_team_name or name_by_team_id.get(provider_team_id, str(provider_team_id))
            upsert_badge(
                conn=conn,
                provider_team_id=provider_team_id,
                team_name=team_name,
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
            team_name = name_by_team_id.get(provider_team_id, str(provider_team_id))
            print(f"WARN: live badge fallback failed for {team_name} ({provider_team_id}): {exc}")
    return existing_badges


def build_heatmap_matrix(
    team_rows: List[Tuple[int, str]],
    goal_bins: Dict[int, Dict[int, int]],
) -> Tuple[List[str], List[int], np.ndarray]:
    ordered_rows: List[Tuple[str, int, List[int]]] = []
    for team_id, team_name in team_rows:
        counts = [goal_bins.get(team_id, {}).get(bin_idx, 0) for bin_idx in range(19)]
        ordered_rows.append((team_name, team_id, counts))

    ordered_rows.sort(key=lambda row: (sum(row[2]), row[0]), reverse=True)
    labels = [row[0] for row in ordered_rows]
    team_ids = [row[1] for row in ordered_rows]
    matrix = np.array([row[2] for row in ordered_rows], dtype=float)
    return labels, team_ids, matrix


def render_heatmap(
    league_name: str,
    season_year: int,
    labels: List[str],
    team_ids: List[int],
    matrix: np.ndarray,
    badges: Dict[int, Optional[bytes]],
    badge_zoom: float,
    output_path: str,
) -> None:
    if matrix.size == 0:
        raise RuntimeError("No matrix values available for heatmap")

    minute_labels = [f"{start}-{start + 4}" for start in range(0, 90, 5)]
    minute_labels[-1] = "85-89"
    minute_labels.append("90+")

    fig_height = max(9.0, 0.46 * len(labels))
    fig, ax = plt.subplots(figsize=(EXPORT_WIDTH_INCHES, fig_height))
    fig.patch.set_facecolor("#f8f4ef")
    ax.set_facecolor("#f8f4ef")

    cmap = mcolors.LinearSegmentedColormap.from_list(
        "teal_purple_gold",
        ["#169c96", "#6b4fa3", "#edc15b"],
        N=256,
    )
    norm = mcolors.PowerNorm(gamma=0.75, vmin=0.0, vmax=max(1.0, float(matrix.max())))

    image = ax.imshow(matrix, aspect="auto", cmap=cmap, norm=norm)

    ax.set_xticks(np.arange(len(minute_labels)))
    ax.set_xticklabels(minute_labels, fontsize=10, color="#2d2233")
    ax.set_yticks(np.arange(len(labels)))
    ax.set_yticklabels(labels, fontsize=10.5, color="#2d2233")
    ax.tick_params(axis="y", pad=22)

    ax.set_xlabel("5-minute scoring interval", fontsize=12, color="#2d2233", labelpad=12)
    ax.set_ylabel("Teams", fontsize=12, color="#2d2233", labelpad=12)
    fig.suptitle(
        f"{league_name} {season_year} Team Goal Heatmap",
        fontsize=18,
        color="#1f1725",
        fontweight="bold",
        y=0.98,
    )
    fig.text(
        0.23,
        0.952,
        "Local DB only. Goal events grouped by scoring team and minute bucket.",
        ha="left",
        va="center",
        fontsize=10.5,
        color="#534663",
    )

    ax.set_xticks(np.arange(-0.5, matrix.shape[1], 1), minor=True)
    ax.set_yticks(np.arange(-0.5, matrix.shape[0], 1), minor=True)
    ax.grid(which="minor", color="#f8f4ef", linewidth=1.2)
    ax.tick_params(which="minor", bottom=False, left=False)

    for row_idx in range(matrix.shape[0]):
        for col_idx in range(matrix.shape[1]):
            value = int(matrix[row_idx, col_idx])
            if value <= 0:
                continue
            text_color = "#fcfaf8" if norm(value) > 0.45 else "#261b2d"
            ax.text(
                col_idx,
                row_idx,
                str(value),
                ha="center",
                va="center",
                fontsize=8.5,
                color=text_color,
                fontweight="bold",
            )

    fig.canvas.draw()
    for row_idx, team_id in enumerate(team_ids):
        badge_blob = badges.get(team_id)
        if not badge_blob:
            continue
        try:
            badge_img = plt.imread(BytesIO(badge_blob))
        except Exception:
            continue
        badge_artist = offsetbox.AnnotationBbox(
            offsetbox.OffsetImage(badge_img, zoom=badge_zoom),
            (-0.026, row_idx),
            xycoords=ax.get_yaxis_transform(),
            frameon=False,
            box_alignment=(0.5, 0.5),
            pad=0,
            annotation_clip=False,
            zorder=8,
        )
        ax.add_artist(badge_artist)

    colorbar = fig.colorbar(image, ax=ax, pad=0.02, fraction=0.03)
    colorbar.set_label("Goals", fontsize=11, color="#2d2233")
    colorbar.ax.tick_params(labelsize=9, colors="#2d2233")

    plt.subplots_adjust(left=0.23, right=0.93, top=0.90, bottom=0.08)
    fig.savefig(output_path, dpi=EXPORT_DPI, facecolor=fig.get_facecolor())
    plt.close(fig)


def main() -> None:
    args = parse_args()
    backend = resolve_compute_backend(args.compute_backend, allow_cuda_execution=False)
    print(
        "INFO: compute backend requested=%s selected=%s reason=%s cuda_enabled=%s cupy_available=%s cuda_devices=%s"
        % (
            backend.requested_backend,
            backend.selected_backend,
            backend.reason,
            backend.cuda_enabled,
            backend.cupy_available,
            backend.cuda_device_count,
        )
    )
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
        season_year = resolve_season_year(cur, args.league_id, args.season_year)
        league_name = get_league_name(cur, args.league_id, season_year)
        team_rows = fetch_team_rows(cur, args.league_id, season_year)
        goal_bins = fetch_goal_bins(cur, args.league_id, season_year)
        labels, team_ids, matrix = build_heatmap_matrix(team_rows, goal_bins)
        badges = fetch_badges(cur, args.league_id, season_year, team_ids)
        if not args.disable_live_badge_fallback:
            api_key = os.getenv(args.api_key_env, "")
            badges = backfill_missing_badges_from_api(
                conn=conn,
                league_id=args.league_id,
                league_name=league_name,
                season_year=season_year,
                labels=labels,
                team_ids=team_ids,
                existing_badges=badges,
                api_key=api_key,
            )

        os.makedirs(args.image_dir, exist_ok=True)
        output_path = os.path.join(
            args.image_dir,
            f"goal_heatmap_{slugify(league_name)}_{season_year}_5min.png",
        )
        render_heatmap(
            league_name=league_name,
            season_year=season_year,
            labels=labels,
            team_ids=team_ids,
            matrix=matrix,
            badges=badges,
            badge_zoom=args.badge_zoom,
            output_path=output_path,
        )
        print(output_path)
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()