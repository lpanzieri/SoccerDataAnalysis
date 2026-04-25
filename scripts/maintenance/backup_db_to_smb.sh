#!/usr/bin/env bash
set -euo pipefail

ROOT="/home/lpanzieri/Data-Analysis"
ENV_FILE="$ROOT/.cron.env"
LOG_DIR="$ROOT/logs"
mkdir -p "$LOG_DIR"

log() {
  echo "$(date -u '+%F %T') | $*"
}

if [[ ! -f "$ENV_FILE" ]]; then
  log "Missing env file: $ENV_FILE"
  exit 1
fi

# shellcheck disable=SC1090
source "$ENV_FILE"

DB_HOST="${MYSQL_HOST:-127.0.0.1}"
DB_PORT="${MYSQL_PORT:-3306}"
DB_USER="${MYSQL_USER:-football_admin}"
DB_NAME="${MYSQL_DATABASE:-historic_football_data}"

if [[ -z "${MYSQL_PASSWORD:-}" ]]; then
  log "MYSQL_PASSWORD missing in env"
  exit 1
fi

# SMB settings: use mounted filesystem path in cron jobs.
SMB_URI="${SMB_BACKUP_URI:-smb://192.168.1.250/Software/mySQL_Backups}"
SMB_MOUNT_POINT="${SMB_BACKUP_MOUNT_POINT:-/mnt/software}"
SMB_SUBDIR="${SMB_BACKUP_SUBDIR:-mySQL_Backups}"
MAX_BACKUPS="${DB_BACKUP_MAX_FILES:-20}"

if [[ ! -d "$SMB_MOUNT_POINT" ]] || ! mountpoint -q "$SMB_MOUNT_POINT"; then
  log "Backup target is not mounted: $SMB_MOUNT_POINT (uri=$SMB_URI)"
  log "Mount the SMB share before cron backup runs."
  exit 1
fi

TARGET_DIR="$SMB_MOUNT_POINT/$SMB_SUBDIR"
mkdir -p "$TARGET_DIR"

TIMESTAMP="$(date -u +%Y%m%d_%H%M%S)"
BASENAME="${DB_NAME}_${TIMESTAMP}.sql.gz"
TMP_FILE="/tmp/${BASENAME}"
TARGET_FILE="$TARGET_DIR/$BASENAME"

log "Starting backup db=$DB_NAME target=$TARGET_FILE"

set +e
MYSQL_PWD="$MYSQL_PASSWORD" mysqldump \
  --single-transaction \
  --quick \
  --no-tablespaces \
  --routines \
  --triggers \
  --events \
  --hex-blob \
  --default-character-set=utf8mb4 \
  -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" "$DB_NAME" | gzip -9 > "$TMP_FILE"
DUMP_RC=$?
set -e

if [[ "$DUMP_RC" -ne 0 ]]; then
  rm -f "$TMP_FILE"
  log "mysqldump failed rc=$DUMP_RC"
  exit 1
fi

mv "$TMP_FILE" "$TARGET_FILE"

if command -v sha256sum >/dev/null 2>&1; then
  SHA="$(sha256sum "$TARGET_FILE" | awk '{print $1}')"
else
  SHA="n/a"
fi

BYTES="$(wc -c < "$TARGET_FILE" | tr -d ' ')"
log "Backup completed file=$TARGET_FILE bytes=$BYTES sha256=$SHA"

if [[ "$MAX_BACKUPS" =~ ^[0-9]+$ ]] && [[ "$MAX_BACKUPS" -ge 1 ]]; then
  mapfile -t backup_files < <(find "$TARGET_DIR" -maxdepth 1 -type f -name "${DB_NAME}_*.sql.gz" -printf "%T@ %p\n" | sort -nr | awk '{ $1=""; sub(/^ /, ""); print }')
  total_backups="${#backup_files[@]}"
  if [[ "$total_backups" -gt "$MAX_BACKUPS" ]]; then
    for ((i=MAX_BACKUPS; i<total_backups; i++)); do
      oldf="${backup_files[$i]}"
      rm -f "$oldf"
      log "Pruned old backup: $oldf"
    done
  fi
else
  log "Skipping count pruning due to invalid DB_BACKUP_MAX_FILES=$MAX_BACKUPS"
fi

log "Backup job finished"
