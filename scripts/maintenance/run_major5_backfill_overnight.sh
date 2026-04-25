#!/usr/bin/env bash
set -euo pipefail

ROOT="/home/lpanzieri/Data-Analysis"
ENV_FILE="$ROOT/.cron.env"
WORKER="$ROOT/scripts/maintenance/worker_run_next_task.sh"
LOG_DIR="$ROOT/logs"

mkdir -p "$LOG_DIR"
RUN_ID="$(date +%F_%H%M%S)"
RUN_LOG="$LOG_DIR/overnight_major5_${RUN_ID}.log"
ARTIFACT_DIR="${ARTIFACT_DIR:-$ROOT/plans/reports}"
mkdir -p "$ARTIFACT_DIR"
PROGRESS_JSONL="$ARTIFACT_DIR/overnight_major5_progress_${RUN_ID}.jsonl"
SUMMARY_JSON="$ARTIFACT_DIR/overnight_major5_summary_${RUN_ID}.json"

log() {
  echo "$(date -u '+%F %T') | $*" | tee -a "$RUN_LOG"
}

emit_progress() {
  local event_type="$1"
  local reason="$2"
  local pending="$3"
  local remaining="$4"
  local worker_rc="$5"
  local runtime_minutes="$6"

  printf '{"time_utc":"%s","run_id":"%s","event":"%s","reason":"%s","loop":%s,"pending_top5":%s,"api_remaining":%s,"worker_rc":%s,"runtime_minutes":%s}\n' \
    "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
    "$RUN_ID" \
    "$event_type" \
    "$reason" \
    "$loop" \
    "$pending" \
    "$remaining" \
    "$worker_rc" \
    "$runtime_minutes" >> "$PROGRESS_JSONL"
}

write_summary() {
  local stop_reason="$1"
  local pending="$2"
  local remaining="$3"
  local runtime_minutes="$4"

  cat > "$SUMMARY_JSON" <<EOF
{
  "run_id": "${RUN_ID}",
  "start_time_utc": "${RUN_START_UTC}",
  "end_time_utc": "$(date -u '+%Y-%m-%dT%H:%M:%SZ')",
  "stop_reason": "${stop_reason}",
  "loops_executed": ${loop},
  "runtime_minutes": ${runtime_minutes},
  "pending_top5_final": ${pending},
  "api_remaining_final": ${remaining},
  "scope": "${BACKFILL_SCOPE}",
  "min_start_year": ${BACKFILL_MIN_START_YEAR},
  "reserve_floor": ${RESERVE_FLOOR},
  "progress_jsonl": "${PROGRESS_JSONL}",
  "run_log": "${RUN_LOG}"
}
EOF
}

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing env file: $ENV_FILE" >&2
  exit 1
fi

# shellcheck disable=SC1090
source "$ENV_FILE"

if [[ -z "${MYSQL_PASSWORD:-}" ]]; then
  echo "MYSQL_PASSWORD missing in $ENV_FILE" >&2
  exit 1
fi

DB_HOST="${MYSQL_HOST:-127.0.0.1}"
DB_PORT="${MYSQL_PORT:-3306}"
DB_USER="${MYSQL_USER:-football_admin}"
DB_NAME="${MYSQL_DATABASE:-historic_football_data}"

# Default safe scope for overnight major-5 backfill.
export BACKFILL_SCOPE="${BACKFILL_SCOPE:-top5_2016_plus}"
export BACKFILL_MIN_START_YEAR="${BACKFILL_MIN_START_YEAR:-2016}"

# Loop controls.
MAX_LOOPS="${MAX_LOOPS:-0}"               # 0 = unlimited
SLEEP_SECONDS="${SLEEP_SECONDS:-60}"      # pause between cycles
MAX_RUNTIME_MINUTES="${MAX_RUNTIME_MINUTES:-0}"  # 0 = unlimited
RESERVE_FLOOR="${RESERVE_FLOOR:-1000}"    # stop when API remaining <= floor

start_epoch="$(date +%s)"
RUN_START_UTC="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
loop=0
STOP_REASON="unknown"
SUMMARY_WRITTEN=0

on_exit() {
  if [[ "$SUMMARY_WRITTEN" -eq 1 ]]; then
    return
  fi
  if [[ "$STOP_REASON" == "unknown" ]]; then
    STOP_REASON="interrupted_or_unexpected_exit"
  fi
  final_pending="$(pending_top5_count || true)"
  final_pending="${final_pending:-0}"
  final_remaining="$(latest_api_remaining || true)"
  final_remaining="${final_remaining:--1}"
  final_runtime_minutes=$(( ($(date +%s) - start_epoch) / 60 ))
  write_summary "$STOP_REASON" "$final_pending" "$final_remaining" "$final_runtime_minutes"
  SUMMARY_WRITTEN=1
}

trap on_exit EXIT

mysql_q() {
  MYSQL_PWD="$MYSQL_PASSWORD" mysql -N -B -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -D "$DB_NAME" -e "$1"
}

pending_top5_count() {
  mysql_q "
    SELECT COUNT(*)
    FROM backfill_task
    WHERE status='pending'
      AND league_code IN ('E0','SP1','I1','D1','F1')
      AND start_year >= ${BACKFILL_MIN_START_YEAR};
  "
}

latest_api_remaining() {
  mysql_q "
    SELECT COALESCE(requests_remaining, -1)
    FROM event_api_call_log
    WHERE requests_remaining IS NOT NULL
    ORDER BY call_id DESC
    LIMIT 1;
  "
}

log "Overnight runner start"
log "scope=$BACKFILL_SCOPE min_start_year=$BACKFILL_MIN_START_YEAR reserve_floor=$RESERVE_FLOOR sleep_seconds=$SLEEP_SECONDS"
emit_progress "start" "runner_started" "-1" "-1" "-1" "0"

while true; do
  loop=$((loop + 1))

  pending="$(pending_top5_count)"
  pending="${pending:-0}"
  now_epoch="$(date +%s)"
  runtime_minutes=$(( (now_epoch - start_epoch) / 60 ))

  if [[ "$pending" -le 0 ]]; then
    log "Stop condition: no pending major-5 tasks"
    STOP_REASON="no_pending_tasks"
    emit_progress "stop" "$STOP_REASON" "$pending" "-1" "-1" "$runtime_minutes"
    break
  fi

  remaining="$(latest_api_remaining || true)"
  remaining="${remaining:--1}"
  if [[ "$remaining" =~ ^[0-9]+$ ]] && [[ "$remaining" -ge 0 ]] && [[ "$remaining" -le "$RESERVE_FLOOR" ]]; then
    log "Stop condition: API reserve floor reached (remaining=$remaining <= floor=$RESERVE_FLOOR)"
    STOP_REASON="reserve_floor_reached"
    emit_progress "stop" "$STOP_REASON" "$pending" "$remaining" "-1" "$runtime_minutes"
    break
  fi

  if [[ "$MAX_RUNTIME_MINUTES" =~ ^[0-9]+$ ]] && [[ "$MAX_RUNTIME_MINUTES" -gt 0 ]]; then
    if [[ "$runtime_minutes" -ge "$MAX_RUNTIME_MINUTES" ]]; then
      log "Stop condition: max runtime reached (${runtime_minutes}m >= ${MAX_RUNTIME_MINUTES}m)"
      STOP_REASON="max_runtime_reached"
      emit_progress "stop" "$STOP_REASON" "$pending" "$remaining" "-1" "$runtime_minutes"
      break
    fi
  fi

  if [[ "$MAX_LOOPS" =~ ^[0-9]+$ ]] && [[ "$MAX_LOOPS" -gt 0 ]] && [[ "$loop" -gt "$MAX_LOOPS" ]]; then
    log "Stop condition: max loops reached (loop=$loop > max_loops=$MAX_LOOPS)"
    STOP_REASON="max_loops_reached"
    emit_progress "stop" "$STOP_REASON" "$pending" "$remaining" "-1" "$runtime_minutes"
    break
  fi

  log "Cycle $loop: pending_top5=$pending api_remaining=$remaining"
  emit_progress "cycle_start" "worker_invocation" "$pending" "$remaining" "-1" "$runtime_minutes"

  set +e
  bash "$WORKER" >> "$RUN_LOG" 2>&1
  rc=$?
  set -e

  if [[ "$rc" -ne 0 ]]; then
    log "Worker returned non-zero rc=$rc; continuing after sleep"
  fi

  emit_progress "cycle_end" "worker_finished" "$pending" "$remaining" "$rc" "$runtime_minutes"

  sleep "$SLEEP_SECONDS"
done

final_pending="$(pending_top5_count || true)"
final_pending="${final_pending:-0}"
final_remaining="$(latest_api_remaining || true)"
final_remaining="${final_remaining:--1}"
final_runtime_minutes=$(( ($(date +%s) - start_epoch) / 60 ))
write_summary "$STOP_REASON" "$final_pending" "$final_remaining" "$final_runtime_minutes"
SUMMARY_WRITTEN=1

log "Overnight runner end"
log "Log file: $RUN_LOG"
log "Progress artifact: $PROGRESS_JSONL"
log "Summary artifact: $SUMMARY_JSON"
