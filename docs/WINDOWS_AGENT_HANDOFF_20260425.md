# Windows Agent Handoff - API-Football Ingestion State

## Purpose
This document is a restart guide for a future Copilot session on Windows.
It explains the current ingestion state, the commands that worked, the commands that failed, and the safest next actions.

Scope of this handoff:
- resume event ingestion work from Windows
- avoid re-debugging already-known issues
- preserve the current DB-backed progress

## Current Project Goal
The current workstream is ingestion of recent API-Football data into the local MySQL database for the major European leagues.

Focus so far:
- Serie A: 3-season rolling window, with fixtures, events, stats, and lineup/player enrichment
- Premier League, La Liga, Bundesliga, Ligue 1: same 3-season target window, but with partial completion due API rate limiting and one lineup deadlock issue

## Important Current Behavior
The sync script is:
- [sync_api_football_events.py](../sync_api_football_events.py)

Relevant implemented behavior already in the code:
- `--season-count N` resolves the window from API current season metadata when available
- event polling is idempotent
- stats polling is idempotent
- lineup polling is idempotent
- fixture refresh can be skipped safely when rate limited
- event polling for a fixture sets `next_retry_after`
- 429 on event polling uses an internal cooldown of 60 minutes

For charting/reporting tasks with team names:
- badges are required, not optional
- prefer `team_badge.badge_image` when building local graphics
- place badges next to team labels rather than in a disconnected legend when possible
- export images in 4K by default so users can safely scale down
- always evaluate whether repeated task logic should be implemented as a reusable helper `.py` function
- unless the user explicitly asks for local-only work, validate local freshness against API data before final answers
- for website integration, route external AI/frontend calls through `scripts/web/agent_api_server.py` with bearer auth and rate limiting enabled

Known code fixes already present:
- season window anchor prefers API current season, not calendar year max
- player dimension row is inserted before player alias row to satisfy FK constraints

## Environment Assumptions
On Linux, runtime commands used:
- `.cron.env` for real environment values
- `.conda` as the Python environment
- local MySQL database `historic_football_data`

On Windows, another agent should not assume the same shell commands work unchanged.
The safe approach is:
1. Read [developing_stage_credentials.txt](../developing_stage_credentials.txt) for connection targets and API key source.
2. Prefer environment variables over inline secrets.
3. Confirm Python environment and MySQL client availability before running sync commands.

Minimum required environment variables:
- `MYSQL_HOST`
- `MYSQL_PORT`
- `MYSQL_USER`
- `MYSQL_PASSWORD`
- `MYSQL_DATABASE`
- `APIFOOTBALL_KEY`

## Windows Resume Checklist
If resuming from Windows, do this first:

1. Open the repo root.
2. Confirm the Python environment used for the project.
3. Export DB and API env vars in the active shell.
4. Run schema validation:

```powershell
python .\setup_schema.py
```

5. Check current DB coverage before pulling more data.
6. Only run targeted retries for missing league-season slices.

If Conda is used on Windows, the equivalent command shape is expected to be:

```powershell
conda run -p .\.conda --no-capture-output python .\sync_api_football_events.py ...
```

If that exact path does not exist on Windows, the future agent should adapt to the active Windows environment rather than forcing `.conda`.

## Current Data State
Latest confirmed DB snapshot for requested leagues and seasons:

### Serie A
Current 3-season target window is `[2025, 2024, 2023]`.

Coverage after completed runs:
- 2025: fixtures `380`, timeline `5344`, goals `806`, lineups polled `332`, stats rows `330`
- 2024: fixtures `380`, timeline `5537`, goals `883`, lineups polled `380`, stats rows `380`
- 2023: fixtures `380`, timeline `6399`, goals `992`, lineups polled `380`, stats rows `380`

Interpretation:
- 2025 has `48` `NS` fixtures, so lineup/stat shortfall is expected there
- 2025 still has `50` unmapped fixtures, so stats cannot fully populate for all finished fixtures yet

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

## Why Some Leagues Are Partial
Two separate blockers were observed.

### 1. API 429 Rate Limiting
Recent call logs showed repeated 429 responses on:
- `/fixtures`
- `/fixtures/events`
- `/fixtures/statistics`

Important detail:
- the code logs response codes and `requests_remaining`
- for the latest 429 responses, logged `requests_remaining` was `NULL`
- no explicit retry header was captured in the DB logs

Practical guidance already derived from the code path:
- for event polling, internal cooldown is 60 minutes on 429
- a future agent should wait about 1 hour after the last 429 burst before retrying

### 2. Lineup Deadlock in La Liga 2025
The full sync with lineup ingestion hit a MySQL deadlock while inserting player identity rows during lineup capture.

Observed failing path:
- lineup sync
- `capture_players_from_lineups`
- `maybe_capture_player_identity`
- `upsert_player_dim`
- MySQL `1213 (40001): Deadlock found when trying to get lock`

Practical workaround that succeeded:
- run bulk retries with `--skip-lineups-sync`
- load fixtures, events, and stats first
- do lineup retries later in smaller targeted chunks

## Commands That Worked
### Full Serie A 3-season sync
Linux form used successfully:

```bash
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

### Safer non-lineup retry form for bulk catch-up
This form worked well when lineup deadlocks or rate pressure made full runs unstable:

```bash
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

### Conservative retry form during rate pressure
This helped make some progress while the API was unstable:

```bash
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
### Multi-league full pass with lineups
This caused two issues:
- 429 rate limiting for non-Serie A leagues
- lineup deadlock in La Liga 2025

So a future agent should not start with one big multi-league lineup-enabled pass.

### League-season retries while the API was already rate limited
Bundesliga and Ligue 1 were still blocked when `/fixtures` itself returned 429.
In those moments, retrying immediately is wasted effort.

## Recommended Resume Strategy For a Future Agent
When resuming on Windows, do this in order.

1. Check recent 429 timestamps in `event_api_call_log`.
2. If the last burst was recent, wait about 60 minutes before retrial.
3. Re-check league-season coverage from the DB.
4. Resume only the missing slices.
5. Use `--skip-lineups-sync` first.
6. Only after events are stable, run small targeted lineup passes.

Recommended order of future retries:
1. Bundesliga 2025
2. Bundesliga 2024
3. Ligue 1 2025
4. Ligue 1 2024
5. Ligue 1 2023
6. La Liga 2023 completion pass
7. Premier League 2024 completion pass

## Useful Verification Queries
### Coverage snapshot
```sql
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
```

### Recent API failures
```sql
SELECT created_at, endpoint, response_code, requests_remaining
FROM event_api_call_log
ORDER BY created_at DESC
LIMIT 20;
```

### Event retry lockout window
```sql
SELECT provider_fixture_id, season_year, last_events_http_code, next_retry_after
FROM event_fixture
WHERE league_id IN (39, 140, 78, 61)
  AND season_year IN (2025, 2024, 2023)
  AND last_events_http_code = 429
ORDER BY next_retry_after DESC
LIMIT 20;
```

## Windows-Specific Notes For the Future Agent
- Prefer PowerShell-compatible commands when suggesting or running commands.
- Do not assume `source ./.cron.env` works on Windows.
- If `.cron.env` is not directly consumable, read the variable names and set them in the session manually.
- Prefer targeted `--league-id` plus `--season-year` retries over long multi-league loops.
- If lineup deadlocks reappear, continue using `--skip-lineups-sync` and defer player enrichment.

## If the Future Agent Has Very Little Time
Fastest safe resume path:

1. Check if the last 429 burst is older than 60 minutes.
2. If yes, run targeted non-lineup retries for the missing league-season slices.
3. Re-run the coverage snapshot query.
4. Only then consider targeted lineup passes.

## Relevant Files
- [sync_api_football_events.py](../sync_api_football_events.py)
- [setup_schema.py](../setup_schema.py)
- [schema.sql](../schema.sql)
- [docs/RUNBOOK.md](RUNBOOK.md)
- [scripts/maintenance/link_historical_to_event_fixtures.py](../scripts/maintenance/link_historical_to_event_fixtures.py)
- [scripts/analysis/generate_goal_heatmap.py](../scripts/analysis/generate_goal_heatmap.py)
