# Linux Agent Handoff - API-Football Ingestion State

## Purpose
This document is a restart guide for a future Copilot session on Linux.
It is optimized for resuming this exact repository state without rebuilding context from chat history.

Use this when another agent needs to:
- continue API-Football ingestion work
- inspect current DB coverage
- resume retries safely after rate limiting
- avoid repeating already-known mistakes

## Current Workstream
The current workstream is ingestion of recent API-Football data into the local MySQL database.

Primary target:
- recent 3-season window for major European leagues

Leagues in scope during this session:
- Serie A (`135`)
- Premier League (`39`)
- La Liga (`140`)
- Bundesliga (`78`)
- Ligue 1 (`61`)

## Key Files
- [sync_api_football_events.py](../sync_api_football_events.py)
- [setup_schema.py](../setup_schema.py)
- [schema.sql](../schema.sql)
- [scripts/maintenance/link_historical_to_event_fixtures.py](../scripts/maintenance/link_historical_to_event_fixtures.py)
- [docs/RUNBOOK.md](RUNBOOK.md)
- [docs/WINDOWS_AGENT_HANDOFF_20260425.md](WINDOWS_AGENT_HANDOFF_20260425.md)
- [scripts/analysis/generate_goal_heatmap.py](../scripts/analysis/generate_goal_heatmap.py)

## Important Current Script Behavior
Current sync behavior already implemented in [sync_api_football_events.py](../sync_api_football_events.py):

- `--season-count` uses API current-season metadata when available
- event polling is idempotent
- stats polling is idempotent
- lineup polling is idempotent
- fixture refresh can be skipped if `/fixtures` is rate limited
- event 429s set `next_retry_after`
- internal cooldown for event 429s is 60 minutes

For charting/reporting tasks with team names:
- badges are required
- prefer `team_badge.badge_image` blobs for local chart rendering
- place badges directly next to team labels whenever the chart layout allows it
- export images in 4K by default so users can safely scale down
- always evaluate whether repeated task logic should be implemented as a reusable helper `.py` function
- unless the user explicitly asks for local-only work, validate local freshness against API data before final answers
- for website integration, route external AI/frontend calls through `scripts/web/agent_api_server.py` with bearer auth and rate limiting enabled

Important already-fixed issues:
- season anchor uses API current season semantics, not naive calendar-year max
- player dimension insert happens before player alias insert to satisfy FK constraints

## Linux Runtime Assumptions
This repo was actively operated on Linux with:

- repo root: `/home/lpanzieri/Data-Analysis`
- env file: `.cron.env`
- Python env: `.conda`
- DB: local MySQL database `historic_football_data`

Typical environment bootstrap on Linux:

```bash
cd /home/lpanzieri/Data-Analysis
set -a
source ./.cron.env
set +a
```

Typical Python invocation form on Linux:

```bash
conda run -p /home/lpanzieri/Data-Analysis/.conda --no-capture-output python ...
```

## Safe Resume Checklist On Linux
When another agent resumes on Linux, do this first:

1. `cd /home/lpanzieri/Data-Analysis`
2. Load env vars from `.cron.env`
3. Validate schema with `setup_schema.py`
4. Query current coverage from MySQL before running any more syncs
5. Check recent 429 timestamps in `event_api_call_log`
6. If rate pressure is recent, wait before retrying

Schema validation command:

```bash
cd /home/lpanzieri/Data-Analysis && \
set -a && source ./.cron.env && set +a && \
conda run -p /home/lpanzieri/Data-Analysis/.conda --no-capture-output python setup_schema.py
```

## Current Data State
This section reflects the last confirmed DB snapshot from this session.

### Serie A
Target window resolved to `[2025, 2024, 2023]`.

Coverage:
- 2025: fixtures `380`, timeline `5344`, goals `806`, lineups polled `332`, stats rows `330`
- 2024: fixtures `380`, timeline `5537`, goals `883`, lineups polled `380`, stats rows `380`
- 2023: fixtures `380`, timeline `6399`, goals `992`, lineups polled `380`, stats rows `380`

Interpretation:
- 2025 has `48` `NS` fixtures, so incomplete lineups/stats there is expected
- 2025 still has `50` unmapped fixtures, so stats cannot yet fully populate for all finished fixtures

### Premier League
- 2025: fixtures `380`, timeline `5120`, goals `933`
- 2024: fixtures `380`, timeline `4059`, goals `763`
- 2023: fixtures `380`, timeline `6099`, goals `1246`

### La Liga
- 2025: fixtures `380`, timeline `5578`, goals `865`
- 2024: fixtures `380`, timeline `6533`, goals `1019`
- 2023: fixtures `380`, timeline `4312`, goals `664`

### Bundesliga
- 2025: fixtures `306`, timeline `4216`, goals `802`
- 2024: fixtures `308`, timeline `1430`, goals `279`
- 2023: fixtures `308`, timeline `4947`, goals `922`

### Ligue 1
- 2025: no rows loaded yet
- 2024: no rows loaded yet
- 2023: no rows loaded yet

## Why Coverage Is Partial
Two main blockers were seen.

### 1. API 429 Rate Limiting
Recent logs showed repeated `429` on:
- `/fixtures`
- `/fixtures/events`
- `/fixtures/statistics`

Important details:
- `event_api_call_log` stores `response_code` and `requests_remaining`
- recent `429` rows had `requests_remaining = NULL`
- the current DB logging does not preserve a usable explicit retry header

Operational conclusion:
- do not keep hammering the API during a 429 burst
- wait about 60 minutes after the last 429 burst before retrying events-heavy passes

### 2. La Liga 2025 Lineup Deadlock
A lineup-enabled La Liga 2025 run hit:
- MySQL `1213 (40001): Deadlock found when trying to get lock`

Observed path:
- `sync_fixture_lineups`
- `capture_players_from_lineups`
- `maybe_capture_player_identity`
- `upsert_player_dim`

Workaround that succeeded:
- run bulk catch-up with `--skip-lineups-sync`
- finish fixtures/events/stats first
- defer lineup enrichment to smaller targeted passes later

## Commands That Worked On Linux
### Full Serie A 3-season sync
```bash
cd /home/lpanzieri/Data-Analysis && \
set -a && source ./.cron.env && set +a && \
conda run -p /home/lpanzieri/Data-Analysis/.conda --no-capture-output \
python sync_api_football_events.py \
  --league-id 135 \
  --season-count 3 \
  --daily-limit 75000 \
  --reserve 1000 \
  --max-event-calls 1500 \
  --max-stats-calls 1500 \
  --max-lineup-calls 1500 \
  --max-full-event-backfill-calls 300 \
  --sleep-seconds 0
```

### Safer bulk retry without lineups
```bash
cd /home/lpanzieri/Data-Analysis && \
set -a && source ./.cron.env && set +a && \
conda run -p /home/lpanzieri/Data-Analysis/.conda --no-capture-output \
python sync_api_football_events.py \
  --league-id <LEAGUE_ID> \
  --season-year <SEASON_YEAR> \
  --daily-limit 75000 \
  --reserve 1000 \
  --max-event-calls 1500 \
  --max-stats-calls 1500 \
  --max-lineup-calls 0 \
  --max-full-event-backfill-calls 300 \
  --sleep-seconds 0.1 \
  --skip-lineups-sync
```

### Conservative retry during active rate pressure
```bash
cd /home/lpanzieri/Data-Analysis && \
set -a && source ./.cron.env && set +a && \
conda run -p /home/lpanzieri/Data-Analysis/.conda --no-capture-output \
python sync_api_football_events.py \
  --league-id <LEAGUE_ID> \
  --season-year <SEASON_YEAR> \
  --daily-limit 75000 \
  --reserve 1000 \
  --max-event-calls 250 \
  --max-stats-calls 250 \
  --max-lineup-calls 0 \
  --max-full-event-backfill-calls 80 \
  --sleep-seconds 0.35 \
  --skip-lineups-sync
```

## Commands That Did Not Work Reliably
### One big multi-league lineup-enabled pass
This was a poor choice operationally because it combined:
- large call volume
- lineup deadlock risk
- multiple leagues competing under current rate pressure

### Immediate retries during confirmed 429 bursts
If `/fixtures` itself is returning `429`, immediate retries are usually wasted.

## Recommended Linux Resume Strategy
Use this exact order.

1. Check the latest `429` timestamps.
2. If the last burst was recent, wait about 60 minutes.
3. Query current coverage.
4. Resume only missing league-season slices.
5. Use `--skip-lineups-sync` first.
6. Defer lineup retries to smaller slices after event coverage stabilizes.

Recommended retry priority from current state:
1. Bundesliga 2025
2. Bundesliga 2024
3. Ligue 1 2025
4. Ligue 1 2024
5. Ligue 1 2023
6. La Liga 2023 completion pass
7. Premier League 2024 completion pass

## Useful Linux Commands
### Coverage snapshot
```bash
cd /home/lpanzieri/Data-Analysis && \
set -a && source ./.cron.env && set +a && \
MYSQL_PWD="$MYSQL_PASSWORD" mysql \
  -h "${MYSQL_HOST:-127.0.0.1}" \
  -P "${MYSQL_PORT:-3306}" \
  -u "${MYSQL_USER:-root}" \
  -D "${MYSQL_DATABASE:-historic_football_data}" \
  -N -B -e "
SELECT
  CASE ef.league_id
    WHEN 39 THEN 'Premier League'
    WHEN 140 THEN 'La Liga'
    WHEN 78 THEN 'Bundesliga'
    WHEN 61 THEN 'Ligue 1'
    ELSE CAST(ef.league_id AS CHAR)
  END AS league_name,
  ef.season_year,
  COUNT(*) AS fixtures,
  COALESCE(t.timeline_rows, 0) AS timeline_rows,
  COALESCE(g.goal_rows, 0) AS goal_rows
FROM event_fixture ef
LEFT JOIN (
  SELECT ef2.league_id, ef2.season_year, COUNT(*) AS timeline_rows
  FROM event_timeline et
  JOIN event_fixture ef2 ON ef2.provider_fixture_id = et.provider_fixture_id
  GROUP BY ef2.league_id, ef2.season_year
) t ON t.league_id = ef.league_id AND t.season_year = ef.season_year
LEFT JOIN (
  SELECT ef3.league_id, ef3.season_year, COUNT(*) AS goal_rows
  FROM event_goal eg
  JOIN event_fixture ef3 ON ef3.provider_fixture_id = eg.provider_fixture_id
  GROUP BY ef3.league_id, ef3.season_year
) g ON g.league_id = ef.league_id AND g.season_year = ef.season_year
WHERE ef.league_id IN (39, 140, 78, 61)
  AND ef.season_year IN (2025, 2024, 2023)
GROUP BY ef.league_id, ef.season_year, t.timeline_rows, g.goal_rows
ORDER BY ef.league_id, ef.season_year DESC;
"
```

### Recent API failures
```bash
cd /home/lpanzieri/Data-Analysis && \
set -a && source ./.cron.env && set +a && \
MYSQL_PWD="$MYSQL_PASSWORD" mysql \
  -h "${MYSQL_HOST:-127.0.0.1}" \
  -P "${MYSQL_PORT:-3306}" \
  -u "${MYSQL_USER:-root}" \
  -D "${MYSQL_DATABASE:-historic_football_data}" \
  -N -B -e "
SELECT created_at, endpoint, response_code, requests_remaining
FROM event_api_call_log
ORDER BY created_at DESC
LIMIT 20;
"
```

### Fixtures locked by event retry cooldown
```bash
cd /home/lpanzieri/Data-Analysis && \
set -a && source ./.cron.env && set +a && \
MYSQL_PWD="$MYSQL_PASSWORD" mysql \
  -h "${MYSQL_HOST:-127.0.0.1}" \
  -P "${MYSQL_PORT:-3306}" \
  -u "${MYSQL_USER:-root}" \
  -D "${MYSQL_DATABASE:-historic_football_data}" \
  -N -B -e "
SELECT provider_fixture_id, season_year, last_events_http_code, next_retry_after
FROM event_fixture
WHERE league_id IN (39, 140, 78, 61)
  AND season_year IN (2025, 2024, 2023)
  AND last_events_http_code = 429
ORDER BY next_retry_after DESC
LIMIT 20;
"
```

## Operator Notes For Another Linux Agent
- On Linux, `.cron.env` was the real runtime source of DB/API values.
- The credentials text file was informational; commands actually used `.cron.env`.
- Prefer single league plus single season retries once rate pressure starts.
- If lineup deadlocks appear again, do not debug them first. Finish non-lineup ingestion first.
- When possible, verify progress from MySQL counts instead of relying only on buffered terminal output.

## Fastest Safe Resume Path
If a future agent has very limited time:

1. Check if last 429 burst is older than 60 minutes.
2. Run targeted non-lineup retries for only missing league-season slices.
3. Re-run the coverage snapshot.
4. Stop there unless the user explicitly wants lineup/player enrichment next.
