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
- [.cron.env](../.cron.env)
- template: [.cron.env.example](../.cron.env.example)

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

## 4.1) Optional CUDA Runtime (Phase 3)
CUDA support is optional and CPU remains the default execution mode.

Current phase behavior:
- runtime capability detection is enabled for analysis scripts
- CUDA execution is enabled for heatmap matrix aggregation in `scripts/analysis/generate_goal_heatmap.py`
- CUDA execution is enabled for ranking/array preparation in `scripts/analysis/generate_top_scorers_report.py`

Supported controls:
- `ENABLE_CUDA=0|1` (default `1`)
- `COMPUTE_BACKEND=auto|cpu|cuda` (default `auto`)
- per-script override: `--compute-backend auto|cpu|cuda`

Notes:
- `--compute-backend cuda` attempts CUDA execution in heatmap matrix aggregation and falls back to CPU if runtime path fails.
- `--compute-backend cuda` attempts CUDA execution in top-scorers ranking/array preparation and falls back to CPU if runtime path fails.
- missing CUDA libraries/devices never break default `auto` mode.

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
- [.cron.env](../.cron.env)
- [.cron.env.example](../.cron.env.example)

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

Major-5 2016-to-current baseline snapshot (read-only):
```bash
cd /home/lpanzieri/Data-Analysis
set -a && source ./.cron.env && set +a
conda run -p /home/lpanzieri/Data-Analysis/.conda --no-capture-output \
  python /home/lpanzieri/Data-Analysis/scripts/maintenance/snapshot_major5_backfill_baseline.py \
  --min-start-year 2016 \
  --max-start-year 0 \
  --out-prefix plans/major5_backfill_baseline
```

Major-5 2016-to-current schedule generation (no API calls):
```bash
cd /home/lpanzieri/Data-Analysis
set -a && source ./.cron.env && set +a
conda run -p /home/lpanzieri/Data-Analysis/.conda --no-capture-output \
  python /home/lpanzieri/Data-Analysis/scripts/maintenance/build_daily_pull_schedule.py \
  --scope top5 \
  --min-start-year 2016 \
  --max-start-year 0 \
  --daily-limit 75000 \
  --max-batch-calls 1000 \
  --include-player-stats \
  --players-pages-per-team-season 2.0 \
  --csv-out plans/major5_2016_current_schedule.csv
```

Initialize queue tracker from major-5 schedule:
```bash
cd /home/lpanzieri/Data-Analysis
set -a && source ./.cron.env && set +a
conda run -p /home/lpanzieri/Data-Analysis/.conda --no-capture-output \
  python /home/lpanzieri/Data-Analysis/scripts/maintenance/backfill_progress_tracker.py \
  --host ${MYSQL_HOST:-127.0.0.1} \
  --port ${MYSQL_PORT:-3306} \
  --user ${MYSQL_USER} \
  --database ${MYSQL_DATABASE:-historic_football_data} \
  init --csv /home/lpanzieri/Data-Analysis/plans/major5_2016_current_schedule.csv
```

Major-5 historical linking pass (2016-to-current, unmapped fixtures only):
```bash
cd /home/lpanzieri/Data-Analysis
set -a && source ./.cron.env && set +a
conda run -p /home/lpanzieri/Data-Analysis/.conda --no-capture-output \
  python /home/lpanzieri/Data-Analysis/scripts/maintenance/link_historical_to_event_fixtures.py \
  --min-season-year 2016 \
  --max-season-year 2025 \
  --league-codes E0,SP1,I1,D1,F1 \
  --api-league-ids 39,140,135,78,61 \
  --min-confidence 72.0 \
  --only-unmapped
```

Optional worker guardrails for major-5 API gap-fill runs:
```bash
cd /home/lpanzieri/Data-Analysis
set -a && source ./.cron.env && set +a
export BACKFILL_SCOPE=top5_2016_plus
export BACKFILL_MIN_START_YEAR=2016
bash /home/lpanzieri/Data-Analysis/scripts/maintenance/worker_run_next_task.sh
```

Guardrail behavior:
- tasks outside major-5 (E0, SP1, I1, D1, F1) are skipped
- tasks with `start_year < BACKFILL_MIN_START_YEAR` are skipped
- linker is auto-scoped to major-5 only when the top5 guard is enabled

Build targeted major-5 retry schedule from current DB gaps:
```bash
cd /home/lpanzieri/Data-Analysis
set -a && source ./.cron.env && set +a
conda run -p /home/lpanzieri/Data-Analysis/.conda --no-capture-output \
  python /home/lpanzieri/Data-Analysis/scripts/maintenance/build_major5_retry_schedule.py \
  --min-start-year 2016 \
  --max-start-year 0 \
  --retry-mode both \
  --daily-limit 75000 \
  --max-batch-calls 1000 \
  --csv-out plans/major5_retry_schedule.csv
```

Load retry schedule into tracker queue:
```bash
cd /home/lpanzieri/Data-Analysis
set -a && source ./.cron.env && set +a
conda run -p /home/lpanzieri/Data-Analysis/.conda --no-capture-output \
  python /home/lpanzieri/Data-Analysis/scripts/maintenance/backfill_progress_tracker.py \
  --host ${MYSQL_HOST:-127.0.0.1} \
  --port ${MYSQL_PORT:-3306} \
  --user ${MYSQL_USER} \
  --database ${MYSQL_DATABASE:-historic_football_data} \
  init --csv /home/lpanzieri/Data-Analysis/plans/major5_retry_schedule.csv
```

Retry task behavior in worker:
- `item_type` containing `retry` triggers `--skip-fixture-refresh`.
- retry tasks enable `--max-full-event-backfill-calls` from task budget to re-poll already-known fixtures missing events.
- stats/lineups/player-stats remain enabled to preserve result->player lockstep.

Overnight major-5 unattended runner:
```bash
cd /home/lpanzieri/Data-Analysis
set -a && source ./.cron.env && set +a
export BACKFILL_SCOPE=top5_2016_plus
export BACKFILL_MIN_START_YEAR=2016
export RESERVE_FLOOR=1000
export SLEEP_SECONDS=60
export MAX_RUNTIME_MINUTES=480
bash /home/lpanzieri/Data-Analysis/scripts/maintenance/run_major5_backfill_overnight.sh
```

Stop conditions in overnight runner:
- no pending major-5 tasks in queue
- API remaining calls <= `RESERVE_FLOOR`
- optional `MAX_RUNTIME_MINUTES` reached
- optional `MAX_LOOPS` reached

Step-7 artifacts emitted by overnight runner:
- progress stream (JSONL): `plans/reports/overnight_major5_progress_<run_id>.jsonl`
- final summary (JSON): `plans/reports/overnight_major5_summary_<run_id>.json`
- runner log: `logs/overnight_major5_<run_id>.log`

Artifact notes:
- a progress line is emitted at start, each cycle start/end, and stop condition
- summary includes stop reason, final pending count, final API remaining, runtime, and links to the progress/log files

Step-8 final validation report (matrix + replay-ready gap list):
```bash
cd /home/lpanzieri/Data-Analysis
set -a && source ./.cron.env && set +a
conda run -p /home/lpanzieri/Data-Analysis/.conda --no-capture-output \
  python /home/lpanzieri/Data-Analysis/scripts/maintenance/generate_major5_backfill_report.py \
  --min-start-year 2016 \
  --max-start-year 0 \
  --max-gap-rows 0 \
  --out-prefix plans/reports/major5_backfill_report
```

Step-8 outputs:
- `<prefix>_matrix.csv`: league-season completeness matrix
- `<prefix>_gaps.csv`: fixture-level replay-ready gaps (missing mapping/timeline/player stats)
- `<prefix>_summary.json`: totals + artifact paths

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

## 12) Handoff Docs
- Start here for any future agent session:
  [docs/NEXT_AGENT_START_HERE.md](NEXT_AGENT_START_HERE.md)
- Windows restart handoff for current API-Football ingestion work:
  [docs/WINDOWS_AGENT_HANDOFF_20260425.md](WINDOWS_AGENT_HANDOFF_20260425.md)
- Linux restart handoff for current API-Football ingestion work:
  [docs/LINUX_AGENT_HANDOFF_20260425.md](LINUX_AGENT_HANDOFF_20260425.md)
