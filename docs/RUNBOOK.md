# Data-Analysis Project Runbook

## 1) Purpose
This project ingests football fixtures/events from API-Football, links them to historical match rows, and runs as an unattended queue via cron.

Primary goal right now:
- personal-use historical-to-event mapping at scale
- resilient automation with clear operational logging

## 2) Current Pipeline
High-level flow:
1. Queue tasks are stored in `backfill_task` and grouped in `backfill_day_log`.
2. Worker picks one pending task, runs fixture/event sync, then runs historical linker.
3. Tracker status is updated to completed/blocked/skipped.
4. Logs and health summaries are written continuously.

Core scripts:
- sync script: [sync_api_football_events.py](../sync_api_football_events.py)
- planner: [scripts/maintenance/build_daily_pull_schedule.py](../scripts/maintenance/build_daily_pull_schedule.py)
- tracker: [scripts/maintenance/backfill_progress_tracker.py](../scripts/maintenance/backfill_progress_tracker.py)
- worker/orchestrator: [scripts/maintenance/worker_run_next_task.sh](../scripts/maintenance/worker_run_next_task.sh)
- linker: [scripts/maintenance/link_historical_to_event_fixtures.py](../scripts/maintenance/link_historical_to_event_fixtures.py)
- health summary: [scripts/maintenance/summarize_orchestrator_health.py](../scripts/maintenance/summarize_orchestrator_health.py)
- cron installer: [scripts/maintenance/install_cron_worker.sh](../scripts/maintenance/install_cron_worker.sh)
- managed cron block: [scripts/maintenance/data_analysis_auto.cron](../scripts/maintenance/data_analysis_auto.cron)

## 3) Data Model (Operational)
Main operational tables:
- `event_fixture`: fixture-level API records.
- `event_timeline`: all timeline events.
- `event_goal`: goal subset for convenience queries.
- `event_fixture_match_map`: link between provider fixture and historical `match_game.match_id`.
- `backfill_task`: task queue.
- `backfill_day_log`: day-level run state.
- `team_badge`: stored team badge images (BLOBs).

Schema file:
- [schema.sql](../schema.sql)

## 4) Automation Setup
Environment file:
- [ .cron.env](../.cron.env)
- template: [ .cron.env.example](../.cron.env.example)

Managed cron jobs:
- Worker every 5 minutes (with lockfile).
- Tracker status snapshot every 30 minutes.
- Orchestrator health summary every hour.
- DB backup to SMB every night at 03:20.

Cron definition:
- [scripts/maintenance/data_analysis_auto.cron](../scripts/maintenance/data_analysis_auto.cron)

Apply cron block:
```bash
cd /home/lpanzieri/Data-Analysis
crontab scripts/maintenance/data_analysis_auto.cron
crontab -l
```

## 5) Key Reliability Features
- Idempotent upserts in sync/linking path.
- Queue state persisted in DB.
- Stale `in_progress` auto-recovery in worker (requeues old tasks after threshold).
- Error classification and dedicated error logs.
- Health summary script for fast diagnosis.

## 6) Logging and Monitoring
Log directory:
- `/home/lpanzieri/Data-Analysis/logs`

Primary logs:
- worker stream: `worker_YYYY-MM-DD.log`
- structured worker errors: `worker_errors_YYYY-MM-DD.log`
- tracker snapshots: `status_YYYY-MM-DD.log`
- health snapshots: `orchestrator_health_YYYY-MM-DD.log`
- per-task sync logs: `sync_<league>_<year>_<timestamp>.log`
- per-task linker logs: `link_<league>_<year>_<timestamp>.log`

Quick checks:
```bash
tail -f /home/lpanzieri/Data-Analysis/logs/worker_$(date +%F).log
tail -f /home/lpanzieri/Data-Analysis/logs/worker_errors_$(date +%F).log
tail -f /home/lpanzieri/Data-Analysis/logs/orchestrator_health_$(date +%F).log
tail -f /home/lpanzieri/Data-Analysis/logs/db_backup_$(date +%F).log
```

## 6.1) SMB Backup Setup
Backup script:
- [scripts/maintenance/backup_db_to_smb.sh](../scripts/maintenance/backup_db_to_smb.sh)

It expects the SMB share to be mounted locally (cron-safe approach), then writes compressed dumps.

Default target settings:
- URI reference: `smb://192.168.1.250/Software/mySQL_Backups`
- local mount point: `/mnt/mysql_backups`
- backup subdir: `mySQL_Backups`

Tune in env file:
- [ .cron.env](../.cron.env)
- [ .cron.env.example](../.cron.env.example)

Manual test:
```bash
source /home/lpanzieri/Data-Analysis/.cron.env
bash /home/lpanzieri/Data-Analysis/scripts/maintenance/backup_db_to_smb.sh
```

## 7) Restart/Power-Off Behavior
- After reboot, cron resumes worker execution.
- Worker requeues stale `in_progress` tasks automatically (threshold configured in env).
- Queue resumes from DB state, avoiding manual recovery in normal cases.

## 8) Common Failure Modes
- `api_plan_restriction`: plan does not allow target season.
- `api_rate_limit_reached`: daily/free limit exhausted.
- `season_not_available_or_invalid`: season inaccessible or invalid for league.
- `missing_api_mapping_or_year`: task has unresolved league mapping.

Blocked reasons are reflected in both:
- `backfill_task.notes`
- `worker_errors_YYYY-MM-DD.log`

## 9) Useful Commands
Initialize queue from schedule:
```bash
MYSQL_PASSWORD='***' conda run -p /home/lpanzieri/Data-Analysis/.conda --no-capture-output \
  python /home/lpanzieri/Data-Analysis/scripts/maintenance/backfill_progress_tracker.py \
  --host 127.0.0.1 --port 3306 --user football_admin --database historic_football_data \
  init --csv /home/lpanzieri/Data-Analysis/scripts/maintenance/all_leagues_2008_2025_mapping_only_75k.csv
```

Show queue status:
```bash
MYSQL_PASSWORD='***' conda run -p /home/lpanzieri/Data-Analysis/.conda --no-capture-output \
  python /home/lpanzieri/Data-Analysis/scripts/maintenance/backfill_progress_tracker.py \
  --host 127.0.0.1 --port 3306 --user football_admin --database historic_football_data status
```

Run one worker cycle manually:
```bash
bash /home/lpanzieri/Data-Analysis/scripts/maintenance/worker_run_next_task.sh
```

Run health summary manually:
```bash
MYSQL_PASSWORD='***' conda run -p /home/lpanzieri/Data-Analysis/.conda --no-capture-output \
  python /home/lpanzieri/Data-Analysis/scripts/maintenance/summarize_orchestrator_health.py \
  --host 127.0.0.1 --port 3306 --user football_admin --database historic_football_data --date $(date +%F)
```

## 10) Assets
Generated badge-based SVG examples:
- [assets/inter_logo_name_from_db.svg](../assets/inter_logo_name_from_db.svg)
- [assets/juventus_logo_name_from_db.svg](../assets/juventus_logo_name_from_db.svg)

## 11) Next Steps
- Upgrade the API-Football plan and verify expanded season access.
- Rebuild the mapping-only queue for the new allowed season window and re-initialize the tracker.
- Complete or verify league-code to API league-id mappings to reduce blocked tasks.
- Run a full health snapshot after the first upgraded-plan day and compare the blocked-reason mix.
- Keep stale-task recovery enabled and tune `STALE_IN_PROGRESS_MINUTES` if needed.
- Build an API layer for frontend integration if this moves beyond personal-use operations.
