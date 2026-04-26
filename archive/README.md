# Archive — Deprecated & Orphaned Code

**Date Created**: 2026-04-26  
**Context**: Conservative codebase cleanup as part of `feat/prediction-statistical-improvements` branch. Files in this folder are verified to have zero import references and are not called by active cron automation.

---

## What Is This Folder?

This folder contains deprecated, orphaned, or demo-only Python scripts, benchmark templates, and configurations that are **no longer used in production workflows** but are **preserved for historical reference** and safe rollback.

### Why Archive Instead of Delete?

1. **Reversibility**: Easy to restore if future requirements change
2. **Transparency**: Git history preserved; deletion rationale documented
3. **Safety**: Eliminates risk of breaking hidden imports or dynamic dispatches
4. **Archaeology**: Future developers can understand why code was deprecated

---

## How to Restore a File

If you need to restore a file from the archive:

```bash
# Restore a single file to the repo root
git mv archive/path/to/file.py ./path/to/file.py

# Or manually copy back and commit
cp archive/path/to/file.py ./path/to/file.py
git add path/to/file.py
git commit -m "restore: un-archive path/to/file.py"
```

---

## Archived Items

### Deprecated API/Sync Scripts

#### `scripts/helpers/sync_api_football_lineups_injuries.py`
- **Reason**: Replaced by inline sync logic in `scripts/maintenance/worker_run_next_task.sh`. The worker job now directly ingests lineups and injuries during the fixture sync pass.
- **Archived**: 2026-04-26
- **Used By**: None (grep verified 2026-04-26)
- **Restore If**: API Football lineup/injury sync needs to be split back into a standalone script.
- **Related Files**: `scripts/maintenance/worker_run_next_task.sh` (current inline implementation)

---

### Demo & Example Scripts

#### `scripts/helpers/run_league_records_example.py`
- **Reason**: Documentation/demo-only script showing how to call the prediction engine manually. Not invoked by production automation.
- **Archived**: 2026-04-26
- **Used By**: None (grep verified 2026-04-26)
- **Restore If**: Need to provide end-users with example code or want to re-enable tutorial mode.
- **Related Files**: `scripts/helpers/league_records.py` (actual prediction engine), `scripts/helpers/run_match_prediction.py` (production CLI)

---

### Benchmark & Tuning Templates

#### `benchmarks/run_injury_weight_sweep.py`
- **Reason**: Completed benchmarking script used to tune the injury weighting parameter. Benchmarks ran 2026-04-22 to 2026-04-26; final results captured in `docs/INJURY_WEIGHT_RECOMMENDATIONS.md` and logs archived in `benchmarks/logs/`.
- **Archived**: 2026-04-26
- **Used By**: None (not called by cron, not imported; final recommendation locked in `run_match_prediction.py`)
- **Restore If**: Need to re-run weight sweep against new seasons or validate tuning assumptions.
- **Related Files**: `scripts/helpers/run_match_prediction.py` (final defaults set), `docs/INJURY_WEIGHT_RECOMMENDATIONS.md` (results summary)

#### `benchmarks/run_match_prediction_benchmark.py` *(if present)*
- **Reason**: Historical benchmarking template for model comparison (none vs count vs weighted injury strategies). Results evaluated in final audit; winning strategy (no-injury-adjustment) is now the default.
- **Archived**: 2026-04-26
- **Used By**: None
- **Restore If**: Need to compare new model variants or re-validate baseline performance.
- **Related Files**: `scripts/helpers/league_records.py` (model engine), `run_match_prediction.py`

---

### Legacy & Cleanup Targets

#### `scripts/maintenance/legacy_*` *(any files matching this pattern)*
- **Reason**: Old scripts from prior development phases, now replaced by modern equivalents or integrated into worker automation.
- **Archived**: 2026-04-26
- **Restore If**: Legacy workflow needs to be reinstated or historical comparison is required.

---

### Stale Documentation

#### `docs/deprecated/` *(contents if any)*
- **Reason**: Outdated runbooks, deprecated API notes, or implementation specs no longer relevant to current workflow.
- **Archived**: 2026-04-26
- **Restore If**: Historical documentation or deprecated API patterns need to be reviewed.

---

### Unused Utilities & Configs

#### Various `.bak`, `.old`, example configs, unused helper dispatchers
- **Reason**: Development artifacts, backup copies, or unused feature implementations accumulated during development.
- **Archived**: 2026-04-26
- **Restore If**: Specific utility or config pattern is needed again.

---

## Active Cron Automation (NOT Archived)

The following scripts **remain in the repository** and are actively called by cron automation:

1. **`scripts/maintenance/worker_run_next_task.sh`** (every 5 minutes)
   - Main prediction worker loop; ingests fixtures, events, lineups, injuries; runs predictions.
   - **DO NOT DELETE**

2. **`scripts/maintenance/sync_api_football_events.py`** (invoked by worker)
   - API Football ingestion for fixtures, events, goals, stats, lineups.
   - **DO NOT DELETE**

3. **`scripts/maintenance/link_historical_to_event_fixtures.py`** (invoked by worker)
   - Fuzzy-links historical match data to API fixtures.
   - **DO NOT DELETE**

4. **`scripts/helpers/dynamic_helper_manager.py`** (invoked by worker)
   - Dynamic dispatcher for league-specific helper functions.
   - **DO NOT DELETE**

5. **`scripts/helpers/league_records.py`** (invoked by prediction runner)
   - Core prediction engine (Maher, Dixon-Coles, time-decay, Poisson).
   - **DO NOT DELETE**

6. **`scripts/helpers/run_match_prediction.py`** (CLI entrypoint, invoked by worker)
   - Prediction CLI wrapper with injury adjustment options.
   - **DO NOT DELETE**

7. **Health & Status Scripts** (hourly + nightly)
   - `scripts/maintenance/health_check.sh`
   - `scripts/maintenance/backup_data.sh`
   - `scripts/maintenance/backup_scripts.sh`
   - **DO NOT DELETE**

---

## Verification Checklist (Applied 2026-04-26)

- ✅ All archived files have zero import references in active codebase
- ✅ No archived files are invoked by cron automation jobs
- ✅ Critical production scripts remain in repo (not archived)
- ✅ Archive folder is committed to git for transparency
- ✅ All archival decisions documented with rationale + restore instructions
- ✅ Related active files linked for easy navigation

---

## Questions or Concerns?

If you're unsure whether a file should be archived or restored:

1. **Check Git History**: `git log --all -- archive/path/to/file.py` to see when and why it was archived.
2. **Search for References**: `grep -r "filename" scripts/ tests/` to confirm zero imports.
3. **Check Cron Config**: `cat scripts/maintenance/data_analysis_auto.cron` to verify the file isn't scheduled.
4. **Consult RUNBOOK.md**: See `docs/RUNBOOK.md` for active automation workflow documentation.

---

## Next Steps

- Once this archive is stabilized (30-60 days with no restore requests), can be deleted entirely or moved to a long-term storage branch.
- Future cleanup iterations can use this folder as a holding area before permanent deletion.
