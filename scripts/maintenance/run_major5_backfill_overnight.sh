#!/usr/bin/env bash
set -euo pipefail

ROOT="/home/lpanzieri/Data-Analysis"
ENV_FILE="$ROOT/.cron.env"
WORKER="$ROOT/scripts/maintenance/worker_run_next_task.sh"
LOG_DIR="$ROOT/logs"

mkdir -p "$LOG_DIR"
RUN_LOG="$LOG_DIR/overnight_major5_$(date +%F_%H%M%S).log"

log() {
  echo "$(date -u '+%F %T') | $*" | tee -a "$RUN_LOG"
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
loop=0

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

while true; do
  loop=$((loop + 1))

  pending="$(pending_top5_count)"
  pending="${pending:-0}"

  if [[ "$pending" -le 0 ]]; then
    log "Stop condition: no pending major-5 tasks"
    break
  fi

  remaining="$(latest_api_remaining || true)"
  remaining="${remaining:--1}"
  if [[ "$remaining" =~ ^[0-9]+$ ]] && [[ "$remaining" -ge 0 ]] && [[ "$remaining" -le "$RESERVE_FLOOR" ]]; then
    log "Stop condition: API reserve floor reached (remaining=$remaining <= floor=$RESERVE_FLOOR)"
    break
  fi

  now_epoch="$(date +%s)"
  if [[ "$MAX_RUNTIME_MINUTES" =~ ^[0-9]+$ ]] && [[ "$MAX_RUNTIME_MINUTES" -gt 0 ]]; then
    runtime_minutes=$(( (now_epoch - start_epoch) / 60 ))
    if [[ "$runtime_minutes" -ge "$MAX_RUNTIME_MINUTES" ]]; then
      log "Stop condition: max runtime reached (${runtime_minutes}m >= ${MAX_RUNTIME_MINUTES}m)"
      break
    fi
  fi

  if [[ "$MAX_LOOPS" =~ ^[0-9]+$ ]] && [[ "$MAX_LOOPS" -gt 0 ]] && [[ "$loop" -gt "$MAX_LOOPS" ]]; then
    log "Stop condition: max loops reached (loop=$loop > max_loops=$MAX_LOOPS)"
    break
  fi

  log "Cycle $loop: pending_top5=$pending api_remaining=$remaining"

  set +e
  bash "$WORKER" >> "$RUN_LOG" 2>&1
  rc=$?
  set -e

  if [[ "$rc" -ne 0 ]]; then
    log "Worker returned non-zero rc=$rc; continuing after sleep"
  fi

  sleep "$SLEEP_SECONDS"
done

log "Overnight runner end"
log "Log file: $RUN_LOG"
