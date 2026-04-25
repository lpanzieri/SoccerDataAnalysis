#!/usr/bin/env bash
set -euo pipefail

ROOT="/home/lpanzieri/Data-Analysis"
LOG_DIR="$ROOT/logs"
CRON_TMP="$(mktemp)"
NEW_CRON_TMP="$(mktemp)"

mkdir -p "$LOG_DIR"

# Capture existing crontab if available; ignore when empty/unset.
if crontab -l >"$CRON_TMP" 2>/dev/null; then
  :
else
  : >"$CRON_TMP"
fi

# Remove any existing managed block.
sed '/# BEGIN DATA-ANALYSIS-AUTO/,/# END DATA-ANALYSIS-AUTO/d' "$CRON_TMP" > "$NEW_CRON_TMP"

cat >>"$NEW_CRON_TMP" <<'CRONBLOCK'
# BEGIN DATA-ANALYSIS-AUTO
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
MAILTO=""

*/5 * * * * flock -n /tmp/football_worker.lock bash -lc '/home/lpanzieri/Data-Analysis/scripts/maintenance/worker_run_next_task.sh >> /home/lpanzieri/Data-Analysis/logs/worker_$(date +\%F).log 2>&1'
*/30 * * * * bash -lc 'source /home/lpanzieri/Data-Analysis/.cron.env; /home/lpanzieri/Data-Analysis/.conda/bin/python /home/lpanzieri/Data-Analysis/scripts/maintenance/backfill_progress_tracker.py --host ${MYSQL_HOST:-127.0.0.1} --port ${MYSQL_PORT:-3306} --user ${MYSQL_USER:-football_admin} --database ${MYSQL_DATABASE:-historic_football_data} status >> /home/lpanzieri/Data-Analysis/logs/status_$(date +\%F).log 2>&1'
15 * * * * bash -lc 'source /home/lpanzieri/Data-Analysis/.cron.env; /home/lpanzieri/Data-Analysis/.conda/bin/python /home/lpanzieri/Data-Analysis/scripts/maintenance/summarize_orchestrator_health.py --host ${MYSQL_HOST:-127.0.0.1} --port ${MYSQL_PORT:-3306} --user ${MYSQL_USER:-football_admin} --database ${MYSQL_DATABASE:-historic_football_data} --date $(date +\%F) >> /home/lpanzieri/Data-Analysis/logs/orchestrator_health_$(date +\%F).log 2>&1'
20 3 * * * flock -n /tmp/db_backup.lock bash -lc 'source /home/lpanzieri/Data-Analysis/.cron.env; /home/lpanzieri/Data-Analysis/scripts/maintenance/backup_db_to_smb.sh >> /home/lpanzieri/Data-Analysis/logs/db_backup_$(date +\%F).log 2>&1'
# END DATA-ANALYSIS-AUTO
CRONBLOCK

if crontab "$NEW_CRON_TMP" 2>/tmp/data_analysis_cron_install.err; then
  echo "Cron installed successfully with managed DATA-ANALYSIS-AUTO block."
  crontab -l
  rm -f "$NEW_CRON_TMP" /tmp/data_analysis_cron_install.err
else
  echo "Could not install crontab automatically in this environment."
  echo "Generated cron file: $NEW_CRON_TMP"
  echo "Installer error:"
  cat /tmp/data_analysis_cron_install.err
  echo
  echo "Apply manually on your machine with:"
  echo "  crontab $NEW_CRON_TMP"
  echo "Canonical checked-in template: $ROOT/scripts/maintenance/data_analysis_auto.cron"
  rm -f /tmp/data_analysis_cron_install.err
fi

rm -f "$CRON_TMP"
