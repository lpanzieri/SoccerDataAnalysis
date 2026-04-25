
from __future__ import annotations

def fetch_and_insert_missing_data_from_api(db: DBConfig, league_code: str, from_date: str, to_date: str) -> None:
    """
    Fetch missing matches/events from the API for the given league and date range, and insert into the DB.
    Uses sync_fixtures from sync_api_football_events.py and API-Football.
    """
    import os
    import mysql.connector
    from datetime import datetime
    from scripts.sync_api_football_events import sync_fixtures, connect_db

    api_key = os.getenv("APIFOOTBALL_KEY", "")
    if not api_key:
        print("[API BRIDGE] APIFOOTBALL_KEY missing, skipping API fetch.")
        return

    # Find league_id and all relevant season_years for the date range
    conn = mysql.connector.connect(
        host=db.host,
        port=db.port,
        user=db.user,
        password=db.password,
        database=db.database,
        charset="utf8mb4",
        use_unicode=True,
        use_pure=True,
        autocommit=False,
    )
    cur = conn.cursor()
    try:
        # Get league_id from league_code
        cur.execute("SELECT league_id FROM league WHERE league_code = %s", (league_code,))
        row = cur.fetchone()
        if not row:
            print(f"[API BRIDGE] League code {league_code} not found in DB.")
            return
        league_id = row[0]

        # Get all season_years in the date range
        cur.execute("SELECT DISTINCT start_year FROM season WHERE start_year >= %s AND start_year <= %s ORDER BY start_year", (from_date[:4], to_date[:4]))
        season_years = [r[0] for r in cur.fetchall()]
        if not season_years:
            print(f"[API BRIDGE] No seasons found for years {from_date[:4]} to {to_date[:4]}.")
            return

        # For each season, call sync_fixtures
        for season_year in season_years:
            print(f"[API BRIDGE] Syncing fixtures for league_id={league_id}, season_year={season_year}")
            # Use a reasonable call budget and sleep
            sync_fixtures(conn, api_key, league_id, season_year, calls_left=10, sleep_seconds=1.5)
            conn.commit()
    finally:
        cur.close()
        conn.close()
#!/usr/bin/env python3
"""Dynamic helper management for NL football questions.

Workflow:
1) Normalize and classify a question.
2) Check registry for an existing generic helper key.
3) If missing, generate a reusable helper file and register it.
4) Execute helper and return JSON-like rows.

This module is website-ready: it can be called by any chat/controller layer.
"""

import decimal
import hashlib
import importlib.util
import json
import os
import re
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import mysql.connector

from scripts.helpers.league_records import DBConfig


HELPERS_ROOT = Path(__file__).resolve().parent
GENERATED_DIR = HELPERS_ROOT / "generated"
REGISTRY_PATH = HELPERS_ROOT / "helper_registry.json"
INTENT_TEMPLATES_PATH = HELPERS_ROOT / "intent_templates.json"
LEAGUE_ALIASES_PATH = HELPERS_ROOT / "league_aliases.json"
UNKNOWN_QUESTIONS_LOG = HELPERS_ROOT / "unknown_questions.log"
CACHE_TABLE_NAME = "helper_response_cache"


DEFAULT_TEMPLATES = [
    {
        "intent": "best_away_record",
        "match_phrases": [
            "best away record",
            "strongest away record",
            "best away performance",
        ],
        "helper_function": "get_best_away_record",
        "requires_league": True,
        "pass_league_code": True,
        "kwargs": {
            "points_for_win": 3,
            "points_for_draw": 1,
        },
        "dynamic_kwargs": ["seasons_back_from_question"],
    },
    {
        "intent": "longest_title_streak",
        "match_phrases": [
            "most titles in a row",
            "most times in a row",
            "consecutive titles",
            "won in a row",
            "titles in a row",
        ],
        "helper_function": "get_longest_title_streak",
        "requires_league": True,
        "pass_league_code": True,
        "kwargs": {
            "points_for_win": 3,
            "points_for_draw": 1,
        },
    },
    {
        "intent": "most_goals_in_season",
        "match_phrases": [
            "most goals in a season",
            "record for most goals",
            "highest goals in a season",
        ],
        "helper_function": "get_most_goals_in_season",
        "requires_league": False,
        "pass_league_code": True,
        "kwargs": {},
    },
    {
        "intent": "most_points_in_season",
        "match_phrases": [
            "most points in a season",
            "record for final points",
            "highest points total",
            "best points total",
            "final points in the leaderboard",
        ],
        "helper_function": "get_most_points_in_season",
        "requires_league": False,
        "pass_league_code": True,
        "kwargs": {
            "points_for_win": 3,
            "points_for_draw": 1,
        },
    },
]

DEFAULT_LEAGUE_ALIASES = {
    "premier league": "E0",
    "english premier league": "E0",
    "serie a": "I1",
    "italian serie a": "I1",
    "la liga": "SP1",
    "bundesliga": "D1",
    "ligue 1": "F1",
    "eredivisie": "N1",
}


@dataclass
class HelperResolution:
    helper_key: str
    intent: str
    league_code: Optional[str]
    helper_file: Path
    created: bool


@dataclass
class IntentResolution:
    intent: str
    template: Optional[Dict[str, Any]]
    league_code: Optional[str]


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off", ""}


def _ensure_storage() -> None:
    GENERATED_DIR.mkdir(parents=True, exist_ok=True)
    if not REGISTRY_PATH.exists():
        REGISTRY_PATH.write_text("{}\n", encoding="utf-8")
    if not INTENT_TEMPLATES_PATH.exists():
        INTENT_TEMPLATES_PATH.write_text(
            json.dumps(DEFAULT_TEMPLATES, indent=2, ensure_ascii=True) + "\n",
            encoding="utf-8",
        )
    if not LEAGUE_ALIASES_PATH.exists():
        LEAGUE_ALIASES_PATH.write_text(
            json.dumps(DEFAULT_LEAGUE_ALIASES, indent=2, ensure_ascii=True) + "\n",
            encoding="utf-8",
        )


def _load_registry() -> Dict[str, Dict[str, Any]]:
    _ensure_storage()
    try:
        return json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _save_registry(registry: Dict[str, Dict[str, Any]]) -> None:
    REGISTRY_PATH.write_text(json.dumps(registry, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def _load_templates() -> List[Dict[str, Any]]:
    _ensure_storage()
    try:
        data = json.loads(INTENT_TEMPLATES_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        data = DEFAULT_TEMPLATES
    if not isinstance(data, list):
        return DEFAULT_TEMPLATES
    return data


def _load_league_aliases() -> Dict[str, str]:
    _ensure_storage()
    try:
        data = json.loads(LEAGUE_ALIASES_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        data = DEFAULT_LEAGUE_ALIASES
    if not isinstance(data, dict):
        return DEFAULT_LEAGUE_ALIASES
    return {str(k).lower(): str(v).upper() for k, v in data.items()}


def normalize_question(question: str) -> str:
    q = question.strip().lower()
    q = re.sub(r"[^a-z0-9\s]", " ", q)
    q = re.sub(r"\s+", " ", q)
    return q


def infer_league_code(question: str) -> Optional[str]:
    q = normalize_question(question)

    aliases = _load_league_aliases()
    for label, code in aliases.items():
        if label in q:
            return code

    # Direct league code mention, e.g. "league E0" or explicit known alias value.
    # Avoid false positives from common words like "of".
    known_codes = set(aliases.values())
    for token in re.findall(r"\b([a-z]{1,3}[0-9]{0,2})\b", q):
        candidate = token.upper()
        if candidate in known_codes:
            return candidate
        if re.match(r"^[A-Z]{1,3}[0-9]{1,2}$", candidate):
            return candidate

    return None


def infer_intent(question: str) -> IntentResolution:
    q = normalize_question(question)

    templates = _load_templates()
    best_template: Optional[Dict[str, Any]] = None
    best_score = -1

    for tmpl in templates:
        phrases = tmpl.get("match_phrases", [])
        if not isinstance(phrases, list):
            continue

        for phrase in phrases:
            if not phrase:
                continue
            p = normalize_question(str(phrase))
            if p and p in q:
                score = len(p)
                if score > best_score:
                    best_score = score
                    best_template = tmpl

    # Use league_code from template if present, else infer from question
    template_league_code = best_template.get("league_code") if best_template else None
    league_code = template_league_code or infer_league_code(question)

    if best_template is None:
        return IntentResolution(intent="unknown", template=None, league_code=league_code)

    intent = str(best_template.get("intent", "unknown"))
    return IntentResolution(intent=intent, template=best_template, league_code=league_code)


def _helper_key(intent: str, league_code: Optional[str], helper_kwargs: Optional[Dict[str, Any]] = None) -> str:
    if intent == "unknown":
        return f"unknown:{league_code or 'UNSCOPED'}"
    base = f"{intent}:{league_code or 'ALL'}"
    if not helper_kwargs:
        return base
    sig = ",".join(f"{k}={helper_kwargs[k]}" for k in sorted(helper_kwargs.keys()))
    return f"{base}:{sig}"


def _generated_filename(intent: str, league_code: Optional[str], helper_kwargs: Optional[Dict[str, Any]] = None) -> str:
    safe_intent = re.sub(r"[^a-z0-9_]+", "_", intent.lower())
    safe_league = (league_code or "all").lower()
    if not helper_kwargs:
        return f"helper_{safe_intent}_{safe_league}.py"
    raw_sig = "_".join(f"{k}_{helper_kwargs[k]}" for k in sorted(helper_kwargs.keys()))
    safe_sig = re.sub(r"[^a-z0-9_]+", "_", raw_sig.lower())
    return f"helper_{safe_intent}_{safe_league}_{safe_sig}.py"


def _extract_seasons_back(question: str) -> Optional[int]:
    q = normalize_question(question)
    patterns = [
        r"(?:last|past)\s+(\d+)\s+(?:years?|seasons?)",
        r"over\s+(?:the\s+)?(?:last\s+)?(\d+)\s+(?:years?|seasons?)",
    ]
    for pattern in patterns:
        m = re.search(pattern, q)
        if m:
            n = int(m.group(1))
            if n >= 1:
                return n
    return None


def _resolve_helper_kwargs(question: str, template: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    static_kwargs = template.get("kwargs", {})
    if isinstance(static_kwargs, dict):
        out.update(static_kwargs)

    dynamic = template.get("dynamic_kwargs", [])
    if not isinstance(dynamic, list):
        dynamic = []

    if "seasons_back_from_question" in dynamic:
        seasons_back = _extract_seasons_back(question)
        if seasons_back is not None:
            out["seasons_back"] = seasons_back

    return out


def _unknown_intent_message(question: str) -> str:
    return (
        "No generic helper template matched this question yet. "
        "Question was logged for future helper promotion. "
        f"question={question!r}"
    )


def _render_helper_file(
    *,
    intent: str,
    template: Optional[Dict[str, Any]],
    league_code: Optional[str],
    helper_kwargs: Optional[Dict[str, Any]],
    question: str,
) -> str:
    if template is None:
        msg = _unknown_intent_message(question)
        return f'''#!/usr/bin/env python3
def answer(db):
    return [{{"intent": "unknown", "message": {msg!r}}}]
'''

    helper_function = str(template.get("helper_function", "")).strip()
    if not helper_function:
        return '''#!/usr/bin/env python3
def answer(db):
    return [{"error": "Template is missing helper_function"}]
'''

    requires_league = bool(template.get("requires_league", False))
    pass_league_code = bool(template.get("pass_league_code", False))
    kwargs = helper_kwargs or {}

    if requires_league and not league_code:
        return '''#!/usr/bin/env python3
def answer(db):
    return [{"error": "This helper requires a league, but none was inferred from the question."}]
'''

    args = ["db=db"]
    if pass_league_code:
        args.append(f"league_code={league_code!r}")
    for k, v in kwargs.items():
        args.append(f"{k}={v!r}")
    args_str = ", ".join(args)

    return f'''#!/usr/bin/env python3
from scripts.helpers.league_records import {helper_function}


def answer(db):
    return {helper_function}({args_str})
'''


def _append_unknown_question(question: str, league_code: Optional[str]) -> None:
    ts = datetime.now(timezone.utc).isoformat()
    line = json.dumps(
        {
            "timestamp_utc": ts,
            "question": question,
            "league_code": league_code,
        },
        ensure_ascii=True,
    )
    with UNKNOWN_QUESTIONS_LOG.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def _connect_db(db: DBConfig):
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


def _ensure_response_cache_table(db: DBConfig) -> None:
    conn = _connect_db(db)
    cur = conn.cursor()
    try:
                cur.execute(
                        f"""
                        CREATE TABLE IF NOT EXISTS {CACHE_TABLE_NAME} (
                            cache_key VARCHAR(64) PRIMARY KEY,
                            normalized_question VARCHAR(1024) NOT NULL,
                            helper_key VARCHAR(512) NOT NULL,
                            response_json LONGTEXT NOT NULL,
                            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            expires_at DATETIME NULL,
                            hit_count BIGINT NOT NULL DEFAULT 0,
                            last_hit_at DATETIME NULL,
                            latest_data_timestamp DATETIME NULL,
                            INDEX idx_{CACHE_TABLE_NAME}_expires_at (expires_at),
                            INDEX idx_{CACHE_TABLE_NAME}_helper_key (helper_key),
                            INDEX idx_{CACHE_TABLE_NAME}_latest_data_timestamp (latest_data_timestamp)
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                        """
                )
    finally:
        cur.close()
        conn.close()


def _build_cache_key(normalized_question: str, helper_key: str) -> str:
    payload = f"v1|{normalized_question}|{helper_key}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _prune_expired_cache(db: DBConfig, limit: int = 200) -> None:
    conn = _connect_db(db)
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            DELETE FROM {CACHE_TABLE_NAME}
            WHERE expires_at IS NOT NULL AND expires_at < UTC_TIMESTAMP()
            LIMIT %s
            """,
            (limit,),
        )
    finally:
        cur.close()
        conn.close()


def _get_cached_rows(db: DBConfig, cache_key: str) -> Optional[List[Dict[str, Any]]]:
    conn = _connect_db(db)
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            f"""
            SELECT response_json, expires_at, latest_data_timestamp
            FROM {CACHE_TABLE_NAME}
            WHERE cache_key = %s
            LIMIT 1
            """,
            (cache_key,),
        )
        row = cur.fetchone()
        if not row:
            return None

        expires_at = row.get("expires_at")
        if expires_at is not None and expires_at < datetime.utcnow():
            cur.execute(
                f"DELETE FROM {CACHE_TABLE_NAME} WHERE cache_key = %s",
                (cache_key,),
            )
            return None

        cur.execute(
            f"""
            UPDATE {CACHE_TABLE_NAME}
            SET hit_count = hit_count + 1,
                last_hit_at = UTC_TIMESTAMP()
            WHERE cache_key = %s
            """,
            (cache_key,),
        )

        # Return both rows and latest_data_timestamp
        return {
            "rows": json.loads(row["response_json"]),
            "latest_data_timestamp": row.get("latest_data_timestamp"),
        }
    finally:
        cur.close()
        conn.close()


def _set_cached_rows(
    db: DBConfig,
    cache_key: str,
    normalized_question: str,
    helper_key: str,
    rows: List[Dict[str, Any]],
    ttl_seconds: int,
) -> None:
    now = datetime.utcnow()
    expires_at = None
    if ttl_seconds > 0:
        expires_at = datetime.utcfromtimestamp(now.timestamp() + ttl_seconds)

        payload = json.dumps(rows, ensure_ascii=True)


        # Normalize rows to always be a list of dicts
        def normalize_rows(rows):
            if isinstance(rows, str):
                return [{"message": rows}]
            if isinstance(rows, list):
                if all(isinstance(r, str) for r in rows):
                    return [{"message": r} for r in rows]
                if all(isinstance(r, dict) for r in rows):
                    return rows
            return [{"message": str(rows)}]

        norm_rows = normalize_rows(rows)

        def extract_latest_match_date(rows):
            candidates = []
            for row in rows:
                for key in ("last_match_date", "latest_match_date", "match_date", "date", "end_date", "to_season"):
                    val = row.get(key)
                    if val:
                        try:
                            if isinstance(val, str) and len(val) >= 10:
                                candidates.append(val[:10])
                        except Exception:
                            pass
            if candidates:
                return max(candidates)
            return None

        latest_data_timestamp = extract_latest_match_date(norm_rows)
        payload = json.dumps(norm_rows, ensure_ascii=True)

        conn = _connect_db(db)
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                INSERT INTO {CACHE_TABLE_NAME}
                    (cache_key, normalized_question, helper_key, response_json, created_at, expires_at, hit_count, last_hit_at, latest_data_timestamp)
                VALUES
                    (%s, %s, %s, %s, UTC_TIMESTAMP(), %s, 0, NULL, %s)
                ON DUPLICATE KEY UPDATE
                    normalized_question = VALUES(normalized_question),
                    helper_key = VALUES(helper_key),
                    response_json = VALUES(response_json),
                    created_at = UTC_TIMESTAMP(),
                    expires_at = VALUES(expires_at),
                    latest_data_timestamp = VALUES(latest_data_timestamp)
                """,
                (cache_key, normalized_question, helper_key, payload, expires_at, latest_data_timestamp),
            )
        finally:
            cur.close()
            conn.close()


def ensure_helper_for_question(question: str) -> HelperResolution:
    intent_info = infer_intent(question)
    intent = intent_info.intent
    template = intent_info.template
    league_code = intent_info.league_code
    helper_kwargs = _resolve_helper_kwargs(question, template) if template else {}
    key = _helper_key(intent, league_code, helper_kwargs)

    registry = _load_registry()
    entry = registry.get(key)

    if entry:
        helper_file = Path(entry["helper_file"])
        return HelperResolution(
            helper_key=key,
            intent=intent,
            league_code=league_code,
            helper_file=helper_file,
            created=False,
        )

    helper_file = GENERATED_DIR / _generated_filename(intent, league_code, helper_kwargs)
    helper_file.write_text(
        _render_helper_file(
            intent=intent,
            template=template,
            league_code=league_code,
            helper_kwargs=helper_kwargs,
            question=question,
        ),
        encoding="utf-8",
    )

    if intent == "unknown":
        _append_unknown_question(question, league_code)

    registry[key] = {
        "intent": intent,
        "league_code": league_code,
        "helper_file": str(helper_file),
        "source_question_example": question,
        "template_present": template is not None,
        "helper_kwargs": helper_kwargs,
    }
    _save_registry(registry)

    return HelperResolution(
        helper_key=key,
        intent=intent,
        league_code=league_code,
        helper_file=helper_file,
        created=True,
    )


def _load_answer_callable(helper_file: Path):
    module_name = f"generated_helper_{helper_file.stem}"
    spec = importlib.util.spec_from_file_location(module_name, helper_file)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load helper module from {helper_file}")

    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    if not hasattr(mod, "answer"):
        raise RuntimeError(f"Generated helper has no answer(db) function: {helper_file}")
    return mod.answer


def _to_json_safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, bool, int, float)):
        return value
    if isinstance(value, decimal.Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    if isinstance(value, dict):
        return {str(k): _to_json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_json_safe(v) for v in value]
    # Fallback for date/time and uncommon mysql types.
    return str(value)


def answer_question_with_helpers(
    question: str,
    db: DBConfig,
    use_cache: Optional[bool] = None,
    cache_ttl_seconds: Optional[int] = None,
) -> Dict[str, Any]:
    if use_cache is None:
        use_cache = _env_bool("HELPER_RESPONSE_CACHE_ENABLED", True)
    if cache_ttl_seconds is None:
        cache_ttl_seconds = int(os.getenv("HELPER_RESPONSE_CACHE_TTL_SECONDS", "86400"))
    if cache_ttl_seconds < 0:
        cache_ttl_seconds = 0

    resolved = ensure_helper_for_question(question)
    normalized_question = normalize_question(question)
    cache_key = _build_cache_key(normalized_question, resolved.helper_key)


    if use_cache:
        _ensure_response_cache_table(db)
        _prune_expired_cache(db)
        cached = _get_cached_rows(db, cache_key)
        if cached is not None:
            cached_rows = cached["rows"]
            cached_latest_ts = cached.get("latest_data_timestamp")

            # Query the DB for the latest match date for this intent/league
            def get_latest_db_match_date(db, league_code=None):
                conn = _connect_db(db)
                cur = conn.cursor()
                try:
                    if league_code:
                        cur.execute(
                            "SELECT MAX(match_date) FROM match_game m JOIN league l ON l.league_id = m.league_id WHERE l.league_code = %s",
                            (league_code,)
                        )
                    else:
                        cur.execute("SELECT MAX(match_date) FROM match_game")
                    row = cur.fetchone()
                    return row[0].strftime("%Y-%m-%d") if row and row[0] else None
                finally:
                    cur.close()
                    conn.close()

            db_latest_ts = get_latest_db_match_date(db, resolved.league_code)
            today_str = datetime.now().strftime("%Y-%m-%d")

            # If cache is up to date (latest data in DB is today), return as usual
            if cached_latest_ts and db_latest_ts and cached_latest_ts >= today_str and db_latest_ts >= today_str:
                # If this is a graphical answer, return image data at top-level.
                if resolved.intent.startswith("graphical_"):
                    return {
                        "helper_key": resolved.helper_key,
                        "intent": resolved.intent,
                        "league_code": resolved.league_code,
                        "helper_file": str(resolved.helper_file),
                        "created": resolved.created,
                        "cache": {
                            "enabled": True,
                            "hit": True,
                            "ttl_seconds": cache_ttl_seconds,
                            "cache_key": cache_key,
                            "fresh": True,
                            "latest_data_timestamp": cached_latest_ts,
                        },
                        "image": cached_rows[0]["image_path"] if isinstance(cached_rows, list) and cached_rows and "image_path" in cached_rows[0] else (cached_rows["image_path"] if isinstance(cached_rows, dict) and "image_path" in cached_rows else None),
                        "base64_image": cached_rows[0]["base64_image"] if isinstance(cached_rows, list) and cached_rows and "base64_image" in cached_rows[0] else (cached_rows["base64_image"] if isinstance(cached_rows, dict) and "base64_image" in cached_rows else None),
                        "meta": cached_rows[0] if isinstance(cached_rows, list) and cached_rows else (cached_rows if isinstance(cached_rows, dict) else {}),
                    }
                # Otherwise, return as usual
                return {
                    "helper_key": resolved.helper_key,
                    "intent": resolved.intent,
                    "league_code": resolved.league_code,
                    "helper_file": str(resolved.helper_file),
                    "created": resolved.created,
                    "cache": {
                        "enabled": True,
                        "hit": True,
                        "ttl_seconds": cache_ttl_seconds,
                        "cache_key": cache_key,
                        "fresh": True,
                        "latest_data_timestamp": cached_latest_ts,
                    },
                    "rows": cached_rows,
                }
            # Otherwise, fetch missing data from API, update DB, then refresh cache
            fetch_and_insert_missing_data_from_api(db, resolved.league_code, db_latest_ts or "2000-01-01", today_str)
            # After API update, re-run helper and update cache
            answer_fn = _load_answer_callable(resolved.helper_file)
            fresh_rows = _to_json_safe(answer_fn(db))
            _set_cached_rows(
                db=db,
                cache_key=cache_key,
                normalized_question=normalized_question,
                helper_key=resolved.helper_key,
                rows=fresh_rows,
                ttl_seconds=cache_ttl_seconds,
            )
            if resolved.intent.startswith("graphical_"):
                return {
                    "helper_key": resolved.helper_key,
                    "intent": resolved.intent,
                    "league_code": resolved.league_code,
                    "helper_file": str(resolved.helper_file),
                    "created": resolved.created,
                    "cache": {
                        "enabled": True,
                        "hit": True,
                        "ttl_seconds": cache_ttl_seconds,
                        "cache_key": cache_key,
                        "fresh": False,
                        "latest_data_timestamp": today_str,
                    },
                    "image": fresh_rows["image_path"] if isinstance(fresh_rows, dict) and "image_path" in fresh_rows else None,
                    "base64_image": fresh_rows["base64_image"] if isinstance(fresh_rows, dict) and "base64_image" in fresh_rows else None,
                    "meta": fresh_rows,
                }
            return {
                "helper_key": resolved.helper_key,
                "intent": resolved.intent,
                "league_code": resolved.league_code,
                "helper_file": str(resolved.helper_file),
                "created": resolved.created,
                "cache": {
                    "enabled": True,
                    "hit": True,
                    "ttl_seconds": cache_ttl_seconds,
                    "cache_key": cache_key,
                    "fresh": False,
                    "latest_data_timestamp": today_str,
                },
                "rows": fresh_rows,
            }

    answer_fn = _load_answer_callable(resolved.helper_file)
    rows = _to_json_safe(answer_fn(db))

    if use_cache:
        _set_cached_rows(
            db=db,
            cache_key=cache_key,
            normalized_question=normalized_question,
            helper_key=resolved.helper_key,
            rows=rows,
            ttl_seconds=cache_ttl_seconds,
        )

    if resolved.intent.startswith("graphical_"):
        return {
            "helper_key": resolved.helper_key,
            "intent": resolved.intent,
            "league_code": resolved.league_code,
            "helper_file": str(resolved.helper_file),
            "created": resolved.created,
            "cache": {
                "enabled": bool(use_cache),
                "hit": False,
                "ttl_seconds": cache_ttl_seconds,
                "cache_key": cache_key,
            },
            "image": rows["image_path"] if isinstance(rows, dict) and "image_path" in rows else None,
            "base64_image": rows["base64_image"] if isinstance(rows, dict) and "base64_image" in rows else None,
            "meta": rows if isinstance(rows, dict) else {},
        }

    return {
        "helper_key": resolved.helper_key,
        "intent": resolved.intent,
        "league_code": resolved.league_code,
        "helper_file": str(resolved.helper_file),
        "created": resolved.created,
        "cache": {
            "enabled": bool(use_cache),
            "hit": False,
            "ttl_seconds": cache_ttl_seconds,
            "cache_key": cache_key,
        },
        "rows": rows,
    }
