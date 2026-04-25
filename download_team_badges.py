#!/usr/bin/env python3
"""Download and store team badges for top 5 European leagues from API-Football."""

import argparse
import hashlib
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from typing import Dict, List, Tuple

import mysql.connector

API_BASE = "https://v3.football.api-sports.io"
TOP5_LEAGUES: List[Tuple[int, str]] = [
    (39, "Premier League"),
    (140, "La Liga"),
    (135, "Serie A"),
    (78, "Bundesliga"),
    (61, "Ligue 1"),
]


def guard_unsafe_secret_flags():
    blocked = ("--password", "--api-key")
    for arg in sys.argv[1:]:
        if any(arg == flag or arg.startswith(flag + "=") for flag in blocked):
            raise SystemExit(
                "Unsafe secret flags detected. Use environment variables MYSQL_PASSWORD and APIFOOTBALL_KEY."
            )


def parse_args():
    guard_unsafe_secret_flags()
    p = argparse.ArgumentParser(description="Download top-5 league team badges")
    p.add_argument("--host", default=os.getenv("MYSQL_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.getenv("MYSQL_PORT", "3306")))
    p.add_argument("--user", default=os.getenv("MYSQL_USER", "football_admin"))
    p.add_argument("--database", default=os.getenv("MYSQL_DATABASE", "historic_football_data"))
    p.add_argument("--mysql-password-env", default="MYSQL_PASSWORD")
    p.add_argument("--api-key-env", default="APIFOOTBALL_KEY")
    p.add_argument("--season-year", type=int, default=2024)
    p.add_argument("--sleep-seconds", type=float, default=0.2)
    p.add_argument("--image-timeout", type=int, default=30)
    return p.parse_args()


def connect_db(args):
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
        autocommit=False,
    )


def ensure_badges_table_exists(conn):
    cur = conn.cursor()
    try:
        cur.execute("SHOW TABLES LIKE 'team_badge'")
        if cur.fetchone() is None:
            raise RuntimeError("Missing table team_badge. Run setup_schema.py first.")
    finally:
        cur.close()


def api_get_teams(api_key: str, league_id: int, season_year: int) -> Dict:
    params = {"league": str(league_id), "season": str(season_year)}
    url = f"{API_BASE}/teams?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(
        url,
        headers={
            "x-apisports-key": api_key,
            "Accept": "application/json",
            "User-Agent": "badge-downloader/1.0",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def download_image(url: str, timeout: int) -> Tuple[bytes, str]:
    req = urllib.request.Request(url, headers={"User-Agent": "badge-downloader/1.0"}, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = resp.read()
        content_type = resp.headers.get("Content-Type", "application/octet-stream")
        return data, content_type


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
):
    sha = hashlib.sha256(badge_image).hexdigest()
    size = len(badge_image)

    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO team_badge (
                provider_team_id, team_name, league_id, league_name, season_year,
                badge_url, badge_image, content_type, image_size_bytes, image_sha256
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
                size,
                sha,
            ),
        )
    finally:
        cur.close()


def main():
    args = parse_args()
    api_key = os.getenv(args.api_key_env, "")
    if not api_key:
        raise SystemExit("APIFOOTBALL_KEY missing. Set environment variable APIFOOTBALL_KEY.")

    conn = connect_db(args)
    try:
        ensure_badges_table_exists(conn)

        total_teams = 0
        total_downloaded = 0

        for league_id, league_name in TOP5_LEAGUES:
            payload = api_get_teams(api_key, league_id, args.season_year)
            errors = payload.get("errors")
            if errors:
                print(f"WARN: teams endpoint error for {league_name} ({league_id}): {errors}")
                continue

            response = payload.get("response", [])
            print(f"League {league_name} ({league_id}) teams returned: {len(response)}")

            for row in response:
                team = row.get("team") or {}
                team_id = team.get("id")
                team_name = team.get("name")
                badge_url = team.get("logo")
                if not team_id or not team_name or not badge_url:
                    continue

                try:
                    img, ctype = download_image(badge_url, args.image_timeout)
                    upsert_badge(
                        conn,
                        provider_team_id=int(team_id),
                        team_name=str(team_name),
                        league_id=league_id,
                        league_name=league_name,
                        season_year=args.season_year,
                        badge_url=str(badge_url),
                        badge_image=img,
                        content_type=ctype,
                    )
                    total_downloaded += 1
                except Exception as exc:
                    print(f"WARN: failed badge download for {team_name} ({team_id}): {exc}")

                total_teams += 1
                if args.sleep_seconds:
                    time.sleep(args.sleep_seconds)

            conn.commit()

        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT league_name, COUNT(*)
                FROM team_badge
                WHERE season_year = %s
                GROUP BY league_name
                ORDER BY league_name
                """,
                (args.season_year,),
            )
            league_counts = cur.fetchall()
        finally:
            cur.close()

        print(f"Teams processed: {total_teams}")
        print(f"Badges downloaded/upserted: {total_downloaded}")
        print("Rows in team_badge by league:")
        for league_name, cnt in league_counts:
            print(f"  {league_name}: {cnt}")

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
