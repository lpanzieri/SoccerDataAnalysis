# Optimization Execution Checklist (1 Week, Pre-Prod)

## Scope
Performance and code-efficiency improvements only (runtime, query count, caching overhead, throughput).

## Success Metrics
- Helper latency: reduce graphical helper p95 by >= 40%.
- Query efficiency: reduce goals-chart query count from O(seasons * teams) to O(1) batch query.
- Sync throughput: increase fixtures/hour without increasing API error rate.
- Contract safety: no regressions in `intent`, `image`, `base64_image`, `meta`, `team_badges` outputs.

## Current Baseline Snapshot (2026-04-25)

Question benchmarked:
- `graph of the goals scored by inter, milan, juventus and napoli in the last 10 years`

Runs:
- 10 runs, 2 warmups, same environment/DB user (`root`)

Cache ON report:
- File: `benchmarks/helper_benchmark_20260425_163301.json`
- `p50`: 714.41 ms
- `p95`: 741.79 ms
- avg DB execute calls/run: 57
- cache hit rate: 1.0

Cache OFF report:
- File: `benchmarks/helper_benchmark_20260425_163314.json`
- `p50`: 362.36 ms
- `p95`: 365.54 ms
- avg DB execute calls/run: 44
- cache hit rate: 0.0

Initial interpretation:
- Current cache-enabled path is slower than cache-disabled path for this query.
- This aligns with known hot-path overhead in `dynamic_helper_manager.py` (per-request cache ensure/prune/freshness checks and potential inline refresh logic).
- Priority remains Tasks 7, 8, 9 to make caching beneficial.

## Execution Log

### Task 11 - Replace fixed sleep pacing with adaptive throttling

- Branch: `opt/task-11-adaptive-throttle`
- Status: code complete (benchmark complete; throughput delta pending workload)
- Files changed: `sync_api_football_events.py`
- Changes implemented:
  - Added header-aware throttle computation using API rate-limit headers.
  - Added fallback pacing when rate-limit headers are unavailable.
  - Added CLI controls:
    - `--disable-adaptive-throttle`
    - `--adaptive-throttle-max-seconds`
  - Wired adaptive pacing into both `/fixtures` and `/fixtures/events` polling paths.
- Validation completed:
  - Python syntax check passed (`sync_api_football_events.py`).
  - Runtime smoke check passed with adaptive throttle enabled.
- Benchmark status:
  - Report file: `benchmarks/sync_benchmark_task11_20260425_173301.json`
  - Run log: `benchmarks/sync_benchmark_task11_20260425_173301.log`
  - `duration_seconds`: 2.25
  - `calls_left_in_run_budget`: 9 (one API call consumed)
  - `fixtures_missing_events_processing_now`: 0
  - Outcome: live path executed successfully with adaptive throttling enabled; throughput delta remains pending a workload with pending fixtures/events.

### Task 10 - Reduce cursor churn and batch write operations in sync flow

- Branch: `opt/task-10-sync-batching`
- Status: code complete (smoke benchmark complete; throughput delta pending workload)
- Files changed: `sync_api_football_events.py`
- Changes implemented:
  - Batched per-fixture writes to `event_timeline` via `executemany`.
  - Batched per-fixture writes to `event_goal` via `executemany`.
  - Removed redundant success-path `next_retry_after` update cursor by folding reset into `mark_fixture_polled`.
  - Kept idempotency semantics (`event_hash` upsert keys) unchanged.
- Validation completed:
  - Python syntax check passed (`sync_api_football_events.py`).
- Benchmark status:
  - Smoke benchmark report: `benchmarks/sync_benchmark_task10_20260425_172913.json`
  - Run log: `benchmarks/sync_benchmark_task10_20260425_172913.log`
  - `duration_seconds`: 1.43
  - `fixtures_missing_events_processing_now`: 0
  - `calls_left_in_run_budget`: 30
  - Outcome: sync completed successfully, but throughput delta is inconclusive in this environment because no pending completed fixtures were available.

### Task 2 - Replace N+1 goals loop with batched query

- Branch: `opt/task-2-batched-goals-query`
- Status: complete
- Report file: `benchmarks/helper_benchmark_20260425_165746.json`
- Validation mode: cache disabled, 10 runs, 2 warmups, DB user `root`
- Result:
  - `p50`: 310.09 ms
  - `p95`: 318.47 ms
  - avg DB execute calls/run: 9
  - cache hit rate: 0.0
- Delta vs prior cache-off baseline:
  - `p50`: 362.36 ms -> 310.09 ms
  - `p95`: 365.54 ms -> 318.47 ms
  - avg DB execute calls/run: 44 -> 9
- Regression checks passed:
  - `intent == graphical_goals_comparison`
  - `image == True`
  - `base64_image == True`
  - `meta.image_path` present

### Task 4 - Single render image pipeline

- Branch: `opt/task-4-single-render`
- Status: complete
- Report file: `benchmarks/helper_benchmark_20260425_170050.json`
- Validation mode: cache disabled, 10 runs, 2 warmups, DB user `root`
- Result:
  - `p50`: 195.36 ms
  - `p95`: 199.90 ms
  - avg DB execute calls/run: 44
  - cache hit rate: 0.0
- Delta vs prior cache-off baseline:
  - `p50`: 362.36 ms -> 195.36 ms
  - `p95`: 365.54 ms -> 199.90 ms
  - avg DB execute calls/run: unchanged at 44
- Regression checks passed:
  - `intent == graphical_goals_comparison`
  - `image == True`
  - `base64_image == True`
  - `meta.image_path` present

### Task 6 - TTL memory cache for loader files

- Branch: `opt/task-6-loader-ttl-cache`
- Status: complete
- Report file: `benchmarks/helper_benchmark_20260425_170329.json`
- Validation mode: cache disabled, 10 runs, 2 warmups, DB user `root`
- Result:
  - `p50`: 320.70 ms
  - `p95`: 324.38 ms
  - avg DB execute calls/run: 44
  - cache hit rate: 0.0
- Delta vs prior cache-off baseline:
  - `p50`: 362.36 ms -> 320.70 ms
  - `p95`: 365.54 ms -> 324.38 ms
  - avg DB execute calls/run: unchanged at 44
- Regression checks passed:
  - `intent == graphical_goals_comparison`
  - `image == True`
  - `base64_image == True`
  - `meta.image_path` present

### Task 7 - Move cache ensure/prune out of request hot path

- Branch: `opt/task-7-cache-hot-path`
- Status: complete
- Report file: `benchmarks/helper_benchmark_20260425_170638.json`
- Validation mode: cache enabled, 10 runs, 2 warmups, DB user `root`
- Result:
  - `p50`: 436.88 ms
  - `p95`: 449.53 ms
  - avg DB execute calls/run: 51
  - cache hit rate: 1.0
- Delta vs prior cache-on baseline:
  - `p50`: 714.41 ms -> 436.88 ms
  - `p95`: 741.79 ms -> 449.53 ms
  - avg DB execute calls/run: 57 -> 51
- Regression checks passed:
  - `intent == graphical_goals_comparison`
  - `image == True` on cache hits
  - `base64_image == True` on cache hits
  - `meta.image_path` present on cache hits

### Task 8 - Lightweight cache freshness strategy

- Branch: `opt/task-8-light-freshness`
- Status: complete
- Report file: `benchmarks/helper_benchmark_20260425_171237.json`
- Validation mode: cache enabled, 10 runs, 2 warmups, DB user `root`
- Result:
  - `p50`: 51.26 ms
  - `p95`: 52.37 ms
  - avg DB execute calls/run: 6
  - cache hit rate: 1.0
- Delta vs prior cache-on baseline:
  - `p50`: 714.41 ms -> 51.26 ms
  - `p95`: 741.79 ms -> 52.37 ms
  - avg DB execute calls/run: 57 -> 6
- Regression checks passed:
  - `intent == graphical_goals_comparison`
  - `image == True` on cache hits
  - `base64_image == True` on cache hits
  - `meta.image_path` present on cache hits

### Task 9 - Decouple API refresh from synchronous request path

- Branch: `opt/task-9-async-refresh-trigger`
- Status: complete
- Report file: `benchmarks/helper_benchmark_20260425_171742.json`
- Validation mode: cache enabled, 10 runs, 2 warmups, DB user `root`
- Result:
  - `p50`: 71.86 ms
  - `p95`: 83.30 ms
  - avg DB execute calls/run: 8
  - cache hit rate: 1.0
- Delta vs prior cache-on baseline:
  - `p50`: 714.41 ms -> 71.86 ms
  - `p95`: 741.79 ms -> 83.30 ms
  - avg DB execute calls/run: 57 -> 8
- Regression checks passed:
  - `intent == graphical_goals_comparison`
  - `image == True` on cache hits
  - `base64_image == True` on cache hits
  - `meta.image_path` present on cache hits
- Notes:
  - Stale-cache requests now return stale cached payload immediately instead of performing inline API sync.
  - Refresh intent is queued to `scripts/helpers/refresh_queue.jsonl` with `stale_cache` reason.
  - Response cache metadata now includes `stale` and `refresh_queued` flags when stale data is served.
  - Trigger cooldown is configurable via `HELPER_REFRESH_TRIGGER_COOLDOWN_SECONDS`.

## Week Plan

### Day 1 - Baseline + Quick Wins (Helpers)

1. Add baseline measurement script/logging for current helper latency and query count.
- Target files: `scripts/helpers/run_dynamic_helper.py`, `scripts/helpers/dynamic_helper_manager.py`
- Effort: 2-3 hours
- Output: before metrics snapshot (`p50`, `p95`, request query count)

2. Replace N+1 season/team loop with batched goals aggregation query.
- Target file: `scripts/helpers/league_records.py`
- Effort: 3-5 hours
- Risk: Low
- Validation:
  - same `team_goals` values as before
  - fewer DB calls per chart request
- Status: Done on `opt/task-2-batched-goals-query`

3. Cache schema capability checks in-process (avoid repeated `information_schema` reads).
- Target file: `scripts/helpers/league_records.py`
- Effort: 1-2 hours
- Risk: Low

### Day 2 - Image Pipeline Efficiency

4. Remove duplicate Matplotlib rendering (render once, write file + base64 from same bytes).
- Target file: `scripts/helpers/league_records.py`
- Effort: 1-2 hours
- Risk: Low
- Validation:
  - `image_path` exists
  - `base64_image` present and decodable
  - visual output unchanged (including badges)
- Status: Done on `opt/task-4-single-render`

5. Profile and optimize badge decode path for blob-backed badges (minimal decode overhead).
- Target file: `scripts/helpers/league_records.py`
- Effort: 2-3 hours
- Risk: Low-Medium

### Day 3 - Dynamic Helper Manager Request Path

6. Add TTL memory cache for registry/templates/league aliases.
- Target file: `scripts/helpers/dynamic_helper_manager.py`
- Effort: 2-4 hours
- Risk: Low
- Status: Done on `opt/task-6-loader-ttl-cache`

7. Move cache table ensure/prune out of hot request path.
- Target file: `scripts/helpers/dynamic_helper_manager.py`
- Effort: 2-4 hours
- Risk: Medium
- Validation: request latency drop; cache behavior still correct
- Status: Done on `opt/task-7-cache-hot-path`

### Day 4 - Cache Freshness / API Decoupling

8. Replace per-request DB freshness (`MAX(match_date)`) with lighter freshness strategy.
- Target file: `scripts/helpers/dynamic_helper_manager.py`
- Effort: 3-5 hours
- Risk: Medium
- Status: Done on `opt/task-8-light-freshness`

9. Decouple API refresh from synchronous request path (stale-mark + async refresh trigger).
- Target files: `scripts/helpers/dynamic_helper_manager.py`, maintenance worker path if needed
- Effort: 4-6 hours
- Risk: Medium
- Status: Done on `opt/task-9-async-refresh-trigger`

### Day 5 - Sync Throughput Improvements

10. Reduce cursor churn and batch write operations in sync flow.
- Target file: `sync_api_football_events.py`
- Effort: 4-6 hours
- Risk: Medium
- Status: Code complete on `opt/task-10-sync-batching` (smoke benchmark complete; throughput delta pending workload)

11. Replace fixed sleep pacing with adaptive throttling based on quota/headers.
- Target file: `sync_api_football_events.py`
- Effort: 3-5 hours
- Risk: Medium
- Status: Code complete on `opt/task-11-adaptive-throttle` (benchmark complete; throughput delta pending workload)

### Day 6 - DB Tuning (Only Proven Changes)

12. Run EXPLAIN on top helper/sync queries and add composite indexes only where validated.
- Target file: `schema.sql`
- Effort: 3-5 hours
- Risk: Medium
- Validation:
  - EXPLAIN improvements recorded
  - no regression on writes

### Day 7 - Regression + Documentation

13. End-to-end regression pass for graphical and non-graphical intents.
- Effort: 2-4 hours
- Includes:
  - intent matching correctness
  - image/base64 contract
  - badge presence
  - cache hit/miss correctness

14. Update docs with measured gains and final operational notes.
- Target files: `WORKFLOW.md` and this checklist (results section)
- Effort: 1-2 hours

## Suggested Execution Order (if constrained)
1. Task 2 (batched goals query)
2. Task 4 (single render)
3. Task 6 (loader TTL cache)
4. Task 7 (remove ensure/prune from hot path)
5. Task 10 (sync batching)

## Rollback Strategy
- Keep each optimization in separate commits.
- After each task, run the same benchmark script and compare against baseline.
- If p95 worsens or output contract breaks, revert that task only.

## Tracking Template

Completed:
- [x] Task 2
- [x] Task 4
- [x] Task 6
- [x] Task 7
- [x] Task 8
- [x] Task 9

Remaining:
- [ ] Task 1
- [ ] Task 3
- [ ] Task 5
- [~] Task 10 (code complete, smoke benchmark done, throughput delta pending workload)
- [~] Task 11 (code complete, benchmark done, throughput delta pending workload)
- [ ] Task 12
- [ ] Task 13
- [ ] Task 14
