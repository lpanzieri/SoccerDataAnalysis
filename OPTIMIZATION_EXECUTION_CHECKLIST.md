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
  - `image == True`
  - `base64_image == True`
  - cache envelope remains present and structured
- Notes:
  - Cache table setup now runs once per DB target in-process.
  - Expired-row pruning now runs on an interval instead of every request.
  - Tuning env vars: `HELPER_CACHE_PRUNE_INTERVAL_SECONDS`, `HELPER_CACHE_PRUNE_LIMIT`.
  - Freshness/API-refresh behavior is still the next bottleneck and remains scoped to Tasks 8-9.

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

5. Profile and optimize badge decode path for blob-backed badges (minimal decode overhead).
- Target file: `scripts/helpers/league_records.py`
- Effort: 2-3 hours
- Risk: Low-Medium

### Day 3 - Dynamic Helper Manager Request Path

6. Add TTL memory cache for registry/templates/league aliases.
- Target file: `scripts/helpers/dynamic_helper_manager.py`
- Effort: 2-4 hours
- Risk: Low

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

9. Decouple API refresh from synchronous request path (stale-mark + async refresh trigger).
- Target files: `scripts/helpers/dynamic_helper_manager.py`, maintenance worker path if needed
- Effort: 4-6 hours
- Risk: Medium

### Day 5 - Sync Throughput Improvements

10. Reduce cursor churn and batch write operations in sync flow.
- Target file: `sync_api_football_events.py`
- Effort: 4-6 hours
- Risk: Medium

11. Replace fixed sleep pacing with adaptive throttling based on quota/headers.
- Target file: `sync_api_football_events.py`
- Effort: 3-5 hours
- Risk: Medium

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
- [x] Task 7
- [x] Code complete
- [x] Benchmark delta recorded
- [x] Regression checks passed
- [x] Docs updated
