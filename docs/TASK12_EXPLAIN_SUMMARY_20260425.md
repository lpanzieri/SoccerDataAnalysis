# Task 12 EXPLAIN Summary (2026-04-25)

## Scope
- Validate top sync/helper queries with `EXPLAIN FORMAT=JSON`.
- Add composite indexes only when they improve plan quality.

## Query A (sync pending fixtures poll list)
SQL shape:
- `event_fixture` filtered by `league_id`, `season_year`, `status_short`, `events_polled_at`, `last_events_http_code`, `next_retry_after`
- Ordered by `fixture_date_utc` with `LIMIT`

Before index:
- Selected key: `idx_event_fixture_league_season_date`
- Query cost: `46.80`
- Rows examined per scan: `380`

After adding `idx_event_fixture_sync_polling`:
- Selected key: `idx_event_fixture_sync_polling`
- Query cost: `3.87`
- Rows examined per scan: `6`
- Improvement: major reduction in cost and scanned rows

Decision:
- Keep index.

## Query B (sync repoll candidates missing timeline)
SQL shape:
- `event_fixture` filtered by `league_id`, `season_year`, status and polled condition
- `NOT EXISTS` over `event_timeline`

Before index:
- Query cost: `203.48`

After index changes:
- Query cost: `203.48` (no material change)

Decision:
- No additional index accepted for this query from this pass.

## Query C (helper goals aggregation by season/team)
SQL shape:
- Aggregation over `match_game` (home + away union), grouped by team/season

Tested candidates:
- `idx_match_season_league_home_goals`
- `idx_match_season_league_away_goals`

Observed result:
- Candidate indexes caused worse plan behavior in this dataset.

Decision:
- Reverted candidate helper indexes.
- Kept original helper/match indexes.

## Write Safety Check
Post-index runtime smoke run:
- Command path: `sync_api_football_events.py` with explicit season, fixture refresh skipped.
- Outcome: `Sync completed successfully.`
- No write-path regression observed.
