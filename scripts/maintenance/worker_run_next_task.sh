#!/usr/bin/env bash
set -euo pipefail

ROOT="/home/lpanzieri/Data-Analysis"
ENV_FILE="$ROOT/.cron.env"
LOG_DIR="$ROOT/logs"
PYTHON_BIN="$ROOT/.conda/bin/python"
ERROR_LOG="$LOG_DIR/worker_errors_$(date +%F).log"
STALE_IN_PROGRESS_MINUTES="${STALE_IN_PROGRESS_MINUTES:-90}"
BACKFILL_SCOPE="${BACKFILL_SCOPE:-all}"
BACKFILL_MIN_START_YEAR="${BACKFILL_MIN_START_YEAR:-0}"

TOP5_CODES="E0,SP1,I1,D1,F1"
TOP5_API_IDS="39,140,135,78,61"

mkdir -p "$LOG_DIR"

log() {
  echo "$(date -u '+%F %T') | $*"
}

log_error_event() {
  local phase="$1"
  local reason="$2"
  local details_file="${3:-}"
  local prefix
  prefix="$(date -u '+%F %T') | task_id=${TASK_ID:-na} day=${DAY_NO:-na} league=${LEAGUE_CODE:-na} year=${START_YEAR:-na} phase=${phase}"

  echo "${prefix} | reason=${reason}" >> "$ERROR_LOG"
  if [[ -n "$details_file" && -f "$details_file" ]]; then
    {
      echo "${prefix} | last_output_begin"
      tail -n 25 "$details_file"
      echo "${prefix} | last_output_end"
    } >> "$ERROR_LOG"
  fi
}

summarize_sync_error() {
  local file="$1"
  local text
  text="$(tail -n 120 "$file" 2>/dev/null | tr '\n' ' ')"

  if echo "$text" | grep -Eqi 'rateLimit|Too many requests|daily request limit'; then
    echo "api_rate_limit_reached"
    return
  fi
  if echo "$text" | grep -Eqi 'plan restriction|errors.*plan'; then
    echo "api_plan_restriction"
    return
  fi
  if echo "$text" | grep -Eqi 'No current season found|No league response found|season.*not.*available|/fixtures failed \(4'; then
    echo "season_not_available_or_invalid"
    return
  fi
  if echo "$text" | grep -Eqi '/fixtures/events failed|/fixtures failed'; then
    echo "api_fixtures_request_failed"
    return
  fi
  if echo "$text" | grep -Eqi 'APIFOOTBALL_KEY missing|Unsafe secret flags|Missing required tables|Missing required columns'; then
    echo "configuration_or_schema_error"
    return
  fi
  echo "sync_failed_unknown"
}

mysql_q() {
  MYSQL_PWD="$MYSQL_PASSWORD" mysql -N -B -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -D "$DB_NAME" -e "$1"
}

update_day_status() {
  local day_no="$1"

  local day_counts
  day_counts="$(mysql_q "
    SELECT
      SUM(status='pending'),
      SUM(status='in_progress'),
      SUM(status='blocked')
    FROM backfill_task
    WHERE day_no = ${day_no};
  ")"

  local day_pending day_inprog day_blocked
  IFS=$'\t' read -r day_pending day_inprog day_blocked <<< "$day_counts"
  day_pending="${day_pending:-0}"
  day_inprog="${day_inprog:-0}"
  day_blocked="${day_blocked:-0}"

  local api_remaining
  api_remaining="$(mysql_q "
    SELECT COALESCE(requests_remaining, -1)
    FROM event_api_call_log
    WHERE requests_remaining IS NOT NULL
    ORDER BY call_id DESC
    LIMIT 1;
  ")"
  api_remaining="${api_remaining:--1}"

  local day_status day_note
  if [[ "$day_pending" -eq 0 && "$day_inprog" -eq 0 && "$day_blocked" -eq 0 ]]; then
    day_status="completed"
    day_note="all tasks completed"
  elif [[ "$day_blocked" -gt 0 ]]; then
    day_status="blocked"
    day_note="one or more blocked tasks"
  else
    day_status="in_progress"
    day_note="work in progress"
  fi

  "$PYTHON_BIN" "$ROOT/scripts/maintenance/backfill_progress_tracker.py" \
    --host "$DB_HOST" --port "$DB_PORT" --user "$DB_USER" --database "$DB_NAME" \
    mark-day --day "$day_no" --status "$day_status" --api-remaining "$api_remaining" --notes "$day_note" || true
}

map_league_code_to_api_id() {
  case "$1" in
    B1) echo 144 ;;
    D1) echo 78 ;;
    D2) echo 79 ;;
    E0) echo 39 ;;
    E1) echo 40 ;;
    E2) echo 41 ;;
    E3) echo 42 ;;
    EC) echo 43 ;;
    F1) echo 61 ;;
    F2) echo 62 ;;
    I1) echo 135 ;;
    I2) echo 136 ;;
    N1) echo 88 ;;
    P1) echo 94 ;;
    SP1) echo 140 ;;
    SP2) echo 141 ;;
    # The following are intentionally unresolved here; keep blocked until verified:
    G1|SC0|SC1|SC2|SC3|T1) echo 0 ;;
    *) echo 0 ;;
  esac
}

is_top5_code() {
  case "$1" in
    E0|SP1|I1|D1|F1) return 0 ;;
    *) return 1 ;;
  esac
}

is_top5_api_id() {
  case "$1" in
    39|140|135|78|61) return 0 ;;
    *) return 1 ;;
  esac
}

if [[ ! -f "$ENV_FILE" ]]; then
  log "Missing env file: $ENV_FILE"
  log_error_event "bootstrap" "missing_env_file"
  exit 1
fi

# shellcheck disable=SC1090
source "$ENV_FILE"

if [[ -z "${MYSQL_PASSWORD:-}" ]]; then
  log "MYSQL_PASSWORD missing in env file"
  log_error_event "bootstrap" "missing_mysql_password"
  exit 1
fi
if [[ -z "${APIFOOTBALL_KEY:-}" ]]; then
  log "APIFOOTBALL_KEY missing in env file"
  log_error_event "bootstrap" "missing_api_key"
  exit 1
fi

export MYSQL_PASSWORD
export APIFOOTBALL_KEY

DB_HOST="${MYSQL_HOST:-127.0.0.1}"
DB_PORT="${MYSQL_PORT:-3306}"
DB_USER="${MYSQL_USER:-football_admin}"
DB_NAME="${MYSQL_DATABASE:-historic_football_data}"

log "Worker start"

# Requeue tasks that were left in_progress (e.g. power off/reboot) and went stale.
if [[ "$STALE_IN_PROGRESS_MINUTES" =~ ^[0-9]+$ ]] && [[ "$STALE_IN_PROGRESS_MINUTES" -gt 0 ]]; then
  REQUEUED_COUNT="$(mysql_q "
    UPDATE backfill_task
    SET status='pending',
        notes=CONCAT('auto-requeued stale in_progress after restart (', ${STALE_IN_PROGRESS_MINUTES}, 'm threshold)'),
        updated_at=CURRENT_TIMESTAMP
    WHERE status='in_progress'
      AND updated_at < (UTC_TIMESTAMP() - INTERVAL ${STALE_IN_PROGRESS_MINUTES} MINUTE);
    SELECT ROW_COUNT();
  " | tail -n 1)"

  if [[ "${REQUEUED_COUNT:-0}" =~ ^[0-9]+$ ]] && [[ "$REQUEUED_COUNT" -gt 0 ]]; then
    log "Recovered stale tasks: requeued=$REQUEUED_COUNT threshold_minutes=$STALE_IN_PROGRESS_MINUTES"
  fi
else
  log "Skipping stale in_progress recovery due to invalid STALE_IN_PROGRESS_MINUTES=$STALE_IN_PROGRESS_MINUTES"
fi

NEXT_TASK="$(mysql_q "
  SELECT
    task_id, day_no, item_type, league_code,
    COALESCE(api_league_id, 0),
    COALESCE(start_year, 0),
    COALESCE(estimated_calls, 0)
  FROM backfill_task
  WHERE status = 'pending'
  ORDER BY day_no ASC, task_id ASC
  LIMIT 1;
")"

if [[ -z "$NEXT_TASK" ]]; then
  log "No pending task. Nothing to do."
  exit 0
fi

IFS=$'\t' read -r TASK_ID DAY_NO ITEM_TYPE LEAGUE_CODE API_LEAGUE_ID START_YEAR EST_CALLS <<< "$NEXT_TASK"

# Claim task safely.
UPDATED="$(mysql_q "
  UPDATE backfill_task
  SET status='in_progress', notes='worker claimed task', updated_at=CURRENT_TIMESTAMP
  WHERE task_id=${TASK_ID} AND status='pending';
  SELECT ROW_COUNT();
" | tail -n 1)"
if [[ "$UPDATED" != "1" ]]; then
  log "Task ${TASK_ID} was claimed by another worker."
  exit 0
fi

if [[ "$API_LEAGUE_ID" -le 0 ]]; then
  API_LEAGUE_ID="$(map_league_code_to_api_id "$LEAGUE_CODE")"
fi

log "Picked task_id=$TASK_ID day=$DAY_NO type=$ITEM_TYPE league=$LEAGUE_CODE api_league_id=$API_LEAGUE_ID year=$START_YEAR est_calls=$EST_CALLS"

if [[ "$BACKFILL_SCOPE" == "top5_2016_plus" || "$BACKFILL_SCOPE" == "top5" ]]; then
  if ! is_top5_code "$LEAGUE_CODE" || ! is_top5_api_id "$API_LEAGUE_ID"; then
    NOTE="skipped by scope guard: BACKFILL_SCOPE=$BACKFILL_SCOPE"
    log "$NOTE"
    "$PYTHON_BIN" "$ROOT/scripts/maintenance/backfill_progress_tracker.py" \
      --host "$DB_HOST" --port "$DB_PORT" --user "$DB_USER" --database "$DB_NAME" \
      mark-task --day "$DAY_NO" --item-type "$ITEM_TYPE" --league-code "$LEAGUE_CODE" \
      --start-year "$START_YEAR" --status skipped --notes "$NOTE"
    update_day_status "$DAY_NO"
    exit 0
  fi
fi

if [[ "$BACKFILL_MIN_START_YEAR" =~ ^[0-9]+$ ]] && [[ "$BACKFILL_MIN_START_YEAR" -gt 0 ]]; then
  if [[ "$START_YEAR" -lt "$BACKFILL_MIN_START_YEAR" ]]; then
    NOTE="skipped by year guard: start_year=$START_YEAR < BACKFILL_MIN_START_YEAR=$BACKFILL_MIN_START_YEAR"
    log "$NOTE"
    "$PYTHON_BIN" "$ROOT/scripts/maintenance/backfill_progress_tracker.py" \
      --host "$DB_HOST" --port "$DB_PORT" --user "$DB_USER" --database "$DB_NAME" \
      mark-task --day "$DAY_NO" --item-type "$ITEM_TYPE" --league-code "$LEAGUE_CODE" \
      --start-year "$START_YEAR" --status skipped --notes "$NOTE"
    update_day_status "$DAY_NO"
    exit 0
  fi
fi

"$PYTHON_BIN" "$ROOT/scripts/maintenance/backfill_progress_tracker.py" \
  --host "$DB_HOST" --port "$DB_PORT" --user "$DB_USER" --database "$DB_NAME" \
  mark-day --day "$DAY_NO" --status in_progress --notes "worker started task_id=$TASK_ID" || true

if [[ "$ITEM_TYPE" == "rosters" ]]; then
  log "Roster task detected. Marking as skipped in mapping-focused worker."
  "$PYTHON_BIN" "$ROOT/scripts/maintenance/backfill_progress_tracker.py" \
    --host "$DB_HOST" --port "$DB_PORT" --user "$DB_USER" --database "$DB_NAME" \
    mark-task --day "$DAY_NO" --item-type "$ITEM_TYPE" --league-code "$LEAGUE_CODE" \
    --start-year "$START_YEAR" --status skipped --notes "roster task skipped by mapping-focused worker"
  update_day_status "$DAY_NO"
  exit 0
fi

if [[ "$API_LEAGUE_ID" -le 0 || "$START_YEAR" -le 0 ]]; then
  NOTE="blocked: missing api_league_id mapping or start_year"
  log "$NOTE"
  log_error_event "precheck" "missing_api_mapping_or_year"
  "$PYTHON_BIN" "$ROOT/scripts/maintenance/backfill_progress_tracker.py" \
    --host "$DB_HOST" --port "$DB_PORT" --user "$DB_USER" --database "$DB_NAME" \
    mark-task --day "$DAY_NO" --item-type "$ITEM_TYPE" --league-code "$LEAGUE_CODE" \
    --start-year "$START_YEAR" --status blocked --notes "$NOTE"
  "$PYTHON_BIN" "$ROOT/scripts/maintenance/backfill_progress_tracker.py" \
    --host "$DB_HOST" --port "$DB_PORT" --user "$DB_USER" --database "$DB_NAME" \
    mark-day --day "$DAY_NO" --status blocked --notes "$NOTE" || true
  exit 0
fi

EVENT_CALLS=$(( EST_CALLS - 1 ))
if [[ "$EVENT_CALLS" -lt 1 ]]; then
  EVENT_CALLS=1
fi

SYNC_EXTRA_ARGS=()
if [[ "$ITEM_TYPE" =~ _part_([0-9]+)$ ]]; then
  PART_NUM="${BASH_REMATCH[1]}"
  if [[ "$PART_NUM" -gt 1 ]]; then
    SYNC_EXTRA_ARGS+=(--skip-fixture-refresh)
  fi
fi

set +e
SYNC_LOG="$LOG_DIR/sync_${LEAGUE_CODE}_${START_YEAR}_$(date +%F_%H%M%S).log"
"$PYTHON_BIN" "$ROOT/sync_api_football_events.py" \
  --host "$DB_HOST" --port "$DB_PORT" --user "$DB_USER" --database "$DB_NAME" \
  --league-id "$API_LEAGUE_ID" --season-year "$START_YEAR" \
  --daily-limit 75000 --reserve 500 \
  --max-event-calls "$EVENT_CALLS" \
  --max-stats-calls "$EVENT_CALLS" \
  --max-lineup-calls "$EVENT_CALLS" \
  --max-player-stats-calls "$EVENT_CALLS" \
  --max-full-event-backfill-calls 0 \
  --sleep-seconds 1.5 \
  "${SYNC_EXTRA_ARGS[@]}" > "$SYNC_LOG" 2>&1
SYNC_RC=$?
set -e

log "Sync log: $SYNC_LOG"

if [[ "$SYNC_RC" -ne 0 ]]; then
  SYNC_REASON="$(summarize_sync_error "$SYNC_LOG")"
  NOTE="sync failed rc=$SYNC_RC reason=$SYNC_REASON"
  log "$NOTE"
  log_error_event "sync" "$SYNC_REASON" "$SYNC_LOG"
  "$PYTHON_BIN" "$ROOT/scripts/maintenance/backfill_progress_tracker.py" \
    --host "$DB_HOST" --port "$DB_PORT" --user "$DB_USER" --database "$DB_NAME" \
    mark-task --day "$DAY_NO" --item-type "$ITEM_TYPE" --league-code "$LEAGUE_CODE" \
    --start-year "$START_YEAR" --status blocked --notes "$NOTE"
  "$PYTHON_BIN" "$ROOT/scripts/maintenance/backfill_progress_tracker.py" \
    --host "$DB_HOST" --port "$DB_PORT" --user "$DB_USER" --database "$DB_NAME" \
    mark-day --day "$DAY_NO" --status blocked --notes "$NOTE" || true
  exit 0
fi

LINK_LOG="$LOG_DIR/link_${LEAGUE_CODE}_${START_YEAR}_$(date +%F_%H%M%S).log"
LINKER_SCOPE_ARGS=()
if [[ "$BACKFILL_SCOPE" == "top5_2016_plus" || "$BACKFILL_SCOPE" == "top5" ]]; then
  LINKER_SCOPE_ARGS+=(--league-codes "$TOP5_CODES" --api-league-ids "$TOP5_API_IDS")
fi
set +e
"$PYTHON_BIN" "$ROOT/scripts/maintenance/link_historical_to_event_fixtures.py" \
  --host "$DB_HOST" --port "$DB_PORT" --user "$DB_USER" --database "$DB_NAME" \
  --min-season-year "$START_YEAR" --max-season-year "$START_YEAR" \
  "${LINKER_SCOPE_ARGS[@]}" \
  --progress-every 200 \
  --log-file "$LINK_LOG"
LINK_RC=$?
set -e

if [[ "$LINK_RC" -ne 0 ]]; then
  NOTE="linker failed rc=$LINK_RC"
  log "$NOTE"
  log_error_event "linker" "linker_failed" "$LINK_LOG"
  "$PYTHON_BIN" "$ROOT/scripts/maintenance/backfill_progress_tracker.py" \
    --host "$DB_HOST" --port "$DB_PORT" --user "$DB_USER" --database "$DB_NAME" \
    mark-task --day "$DAY_NO" --item-type "$ITEM_TYPE" --league-code "$LEAGUE_CODE" \
    --start-year "$START_YEAR" --status blocked --notes "$NOTE"
  "$PYTHON_BIN" "$ROOT/scripts/maintenance/backfill_progress_tracker.py" \
    --host "$DB_HOST" --port "$DB_PORT" --user "$DB_USER" --database "$DB_NAME" \
    mark-day --day "$DAY_NO" --status blocked --notes "$NOTE" || true
  exit 0
fi

"$PYTHON_BIN" "$ROOT/scripts/maintenance/backfill_progress_tracker.py" \
  --host "$DB_HOST" --port "$DB_PORT" --user "$DB_USER" --database "$DB_NAME" \
  mark-task --day "$DAY_NO" --item-type "$ITEM_TYPE" --league-code "$LEAGUE_CODE" \
  --start-year "$START_YEAR" --status completed --notes "sync+link ok"

update_day_status "$DAY_NO"

log "Task completed task_id=$TASK_ID"
log "Worker end"