# Dynamic Football Q&A System: Robust Workflow & Best Practices

## Purpose
This document provides a step-by-step workflow and best practices to efficiently add new questions, templates, or code changes to the dynamic football Q&A system, minimizing debugging cycles and ensuring robust automation.

## 0. Documentation Maintenance Rule (Mandatory)

- **Always update this file after every behavior-changing code/config change.**
  - If you touch intent routing, helper output format, cache behavior, schema assumptions, CLI usage, or API bridge behavior, reflect it here in the same PR/change.
  - Do not defer docs updates. `WORKFLOW.md` is part of the implementation, not optional commentary.
- **API bridge contract rule (mandatory): keep OpenAPI spec in lockstep with server behavior.**
  - If you modify `scripts/web/agent_api_server.py` request/response shape, endpoint paths, auth, headers, status codes, or rate-limit semantics, update `docs/openapi/agent_api_openapi.yaml` in the same PR/change.
  - Validation command: `grep -n "def do_GET\|def do_POST\|/health\|/v1/question" scripts/web/agent_api_server.py && test -f docs/openapi/agent_api_openapi.yaml`
  - Current API bridge supports request-level compute preference (`compute_backend: auto|cpu|cuda`) and forwards it to helper execution with safe CPU fallback.
- **Optional CUDA rule (mandatory): preserve CPU-first behavior and safe fallback.**
  - CUDA acceleration must remain optional; default behavior must work without CUDA packages or GPU hardware.
  - For new compute-heavy paths, default to implementing optional CUDA acceleration with mandatory CPU fallback unless explicitly rejected for technical reasons.
  - Runtime backend selection must support `auto|cpu|cuda`; `auto` must never fail due to missing CUDA dependencies.
  - Current scope: Phase 3 enables CUDA execution for heatmap matrix aggregation in `scripts/analysis/generate_goal_heatmap.py` and ranking/array preparation in `scripts/analysis/generate_top_scorers_report.py`.
  - Validation command: `python - <<'PY'\nfrom scripts.helpers.cuda_runtime import resolve_compute_backend\nprint(resolve_compute_backend('auto', allow_cuda_execution=True))\nprint(resolve_compute_backend('cpu', allow_cuda_execution=True))\nPY`
  - Runtime parity check: `source ./.cron.env && python scripts/analysis/generate_goal_heatmap.py --compute-backend auto --league-id 135 --season-year 2025 --image-dir generated_graphs && python scripts/analysis/generate_goal_heatmap.py --compute-backend cpu --league-id 135 --season-year 2025 --image-dir generated_graphs`
  - Runtime parity check (top scorers): `source ./.cron.env && python scripts/analysis/generate_top_scorers_report.py --compute-backend auto --top-n 10 --image-dir generated_graphs && python scripts/analysis/generate_top_scorers_report.py --compute-backend cpu --top-n 10 --image-dir generated_graphs`
- **Update both what changed and how to validate it.**
  - Add one short bullet under the relevant section and one validation command/example.
- **Keep canonical operational docs and filenames stable.**
  - The runbook lives at `docs/RUNBOOK.md`.
  - The checked-in cron template lives at `scripts/maintenance/data_analysis_auto.cron`.

---

## 1. Template/Intent Addition

- **Edit `intent_templates.json` as a valid JSON array.**
  - Use a JSON validator before saving.
  - Each new intent/template must include: `intent`, `match_phrases`, `helper_function`, `requires_league`, `pass_league_code`, `kwargs`, and (if needed) `league_code`.
- **Add both specific and general `match_phrases`.**
  - Cover likely user phrasings and edge cases.
- **Normalize phrase variants.**
  - Add punctuation-free variants for user text like: `graph of the goals scored by inter milan juventus and napoli in the last 10 years`.

## 1.1 Intent & League Code Safety Checks

- **Prevent false-positive league code inference from common words.**
  - Example pitfall: `of` being interpreted as a league code.
  - Restrict inferred codes to known aliases and valid explicit code patterns.
- **Prefer template `league_code` when present.**
  - For fixed-intent queries (for example Serie A comparison), lock the league in template config.

## 2. Code/Helper Changes

- **When adding a new helper:**
  - Place the function in `league_records.py` or a dedicated helpers file.
  - Ensure the function returns a dict or list of dicts (never a plain string).
  - If returning images, always include both `image_path` and `base64_image`.

- **When changing code:**
  - Run all relevant scripts with a test question to ensure no exceptions are raised.
  - Check that the output type matches what the cache expects (list of dicts).

## 2.1 Graphical Helper Output Contract

- **For all `graphical_*` intents, return a consistent top-level response envelope:**
  - `intent`, `image`, `base64_image`, and `meta`.
  - Keep detailed payload in `meta` (`image_path`, labels, series, badge resolution info).
- **Export graphical outputs in 4K by default.**
  - Prefer a 3840px-wide export baseline (for example 16 inches at 240 DPI).
  - Downscaling is acceptable; upscaling should be avoided.
- **Do not rely on list-only cache payloads for graphical responses.**
  - Handle both dict and list cache shapes defensively.

## 2.2 Badge/Data Source Compatibility

- **Global rule (mandatory): if a team name is present in any report, include that team's badge.**
  - This applies to graphical reports, tabular reports, and textual summaries that render team labels.
  - If the primary badge source fails, implement fallback logic (alternate schema/table, league-name matching, file/blob fallback).
  - A report is not considered complete until badge placement is solved for every displayed team name.
- **Place badge next to the team name in output/UI.**
  - For charts: place badge next to legend/team label.
  - For tables/text reports: render badge adjacent to each team label where supported.
  - If a client surface cannot render images, return badge metadata/URL/blob marker and document the limitation.

- **Support multiple badge schemas.**
  - If `team.badge_path` exists, allow file-path badge loading.
  - If not, fallback to `team_badge.badge_image` blob loading.
  - For chart scripts that render directly from event/provider IDs, add a live API fallback path for missing `team_badge` rows and persist newly fetched badges back into `team_badge`.
  - Badge API pulls are considered free for this project, so always attempt API backfill when a displayed team has no local badge blob.
- **Do not assume league IDs are in the same namespace across tables.**
  - Local `league.league_id` may differ from provider IDs in `team_badge.league_id`.
  - Rank badge matches by: local league_id match, then normalized `league_name` match, then latest season/update.
- **Badge legend rendering must be resilient.**
  - Try image insertion in legend area; fallback gracefully to line-only legend if image decode fails.
  - Place badge annotations in **figure coordinates after layout is finalized** (after `tight_layout` + legend draw), otherwise icons may render tiny/misaligned or disappear after redraw.

## 3. Validation & Testing

- **Validate all JSON files after editing.**
- **Run the dynamic helper pipeline with a test question:**
  - Confirm the correct intent and league_code are matched.
  - Check that the helper is created or reused as expected.
  - Ensure the output includes all required fields (image, base64, etc.).
- **Run a performance baseline for helper hot paths (recommended before/after optimizations):**
  - `python3 scripts/maintenance/benchmark_helpers.py --question "..." --runs 20 --warmups 3 [--cache]`
  - Review generated JSON report in `benchmarks/` for latency (`p50`/`p95`) and DB execute-call counts.
- **CLI invocation check (required):**
  - Use `--question` (not `--intent`).
  - Provide DB credentials explicitly (for example env var + `--user root` when needed).
- **Check the cache table for new/updated entries.**
- **For graphical intents, verify these fields explicitly:**
  - `intent == graphical_*`
  - `image == True` and `base64_image == True`
  - `meta.image_path` exists
  - `meta.team_badges` is populated (for badge-based charts)
- **For graphical outputs, verify export quality:**
  - Output should be 4K-class by default (target 3840px width unless the chart requires larger height).
- **For any report with team names, verify badge completeness:**
  - Every displayed team label has a resolved badge (image render or explicit fallback metadata).
  - Badge appears visually next to team label (or documented client limitation is returned in metadata).
- **If errors occur:**
  - Read logs for `AttributeError`, `JSONDecodeError`, or `KeyError`.
  - Check for type mismatches (e.g., string vs dict).
  - Validate that all required fields are present in the output.

## 4. Debugging & Recovery

- **If the wrong intent or league_code is matched:**
  - Check `match_phrases` and `league_code` in the template.
  - Ensure `infer_intent` uses the template’s league_code if present.
  - Ensure `infer_league_code` cannot parse common non-code tokens.
- **If the helper output causes a crash:**
  - Patch the code to normalize outputs to a list of dicts.
- **If the cache is not updated or used:**
  - Check the cache key logic and table schema.
  - Ensure `latest_data_timestamp` is extracted correctly.
  - Do not run API sync inline in the request path for stale cache entries.
  - Serve stale cache immediately and enqueue refresh intent to `scripts/helpers/refresh_queue.jsonl`.
  - Include `stale` and `refresh_queued` flags in cache metadata when stale payload is served.
  - Tune duplicate-trigger suppression with `HELPER_REFRESH_TRIGGER_COOLDOWN_SECONDS`.
- **If badges are null/missing:**
  - Inspect DB schema (`team` vs `team_badge`).
  - Verify league ID namespace mismatch and enable `league_name` fallback.
  - Validate blob/image decode paths and legend fallback.

## 5. LLM Agent Guidelines

- **Always validate JSON before using it.**
- **Default data-source policy: unless the user explicitly asks for local-only analysis, verify local data freshness against API data before answering.**
  - If local data is behind, run or propose the minimal sync needed before producing final results.
  - If the user explicitly says local-only, do not call the API and clearly label results as local-data-only.
- **Historical-era coverage rule (mandatory): do not guess when source scope cannot cover the era.**
  - If the requested player/team era is outside DB/API coverage (for example many 1970s-1980s player queries), explicitly return that no supported answer is available from current sources.
  - Do not backfill with unsourced memory when the user requires DB/API-backed answers.
- **For website-hosted frontends, prefer an HTTP backend wrapper over direct script execution from clients.**
  - Expose stable JSON endpoints and keep DB/API credentials server-side only.
  - External agents should integrate through API contracts, not direct shell access.
  - Require bearer-token authentication and enable rate limiting before internet exposure.
- **Always respect API-Football limits in every script and report flow.**
  - Use header-aware pacing and stop conditions (`requests-remaining`, `retry-after`, reset headers).
  - When remaining quota reaches reserve threshold, stop further API calls for that run.
  - On HTTP 429, honor retry windows before any further attempt.
- **Result-update workflows must keep player stats in lockstep.**
  - Any operational workflow that updates fixture results through `sync_api_football_events.py` must also pass `--max-player-stats-calls` (and avoid `--skip-player-stats-sync`) so per-player match stats remain consistent with fixture/event/stat updates.
  - Global enforcement is now active in `sync_api_football_events.py`: runs that enable fixture outcome enrichment (`events`, `event backfill`, `statistics`, or `lineups`) will fail fast if `--skip-player-stats-sync` is set.
  - Validation command:
    - `grep -n "sync_api_football_events.py" scripts/maintenance/worker_run_next_task.sh && grep -n "max-player-stats-calls" scripts/maintenance/worker_run_next_task.sh && conda run -p ./.conda --no-capture-output python sync_api_football_events.py --league-id 39 --season-year 2025 --skip-player-stats-sync --max-event-calls 1 --skip-fixture-refresh`
- **Before writing one-off logic, evaluate whether the task is recurrent and should become a reusable helper Python function.**
  - Prefer adding/expanding helper functions in `scripts/helpers/` (or another stable module) when the same operation is likely to be repeated.
  - Keep helper signatures explicit and return structured payloads that are easy to test and reuse.
- **When patching code, ensure type robustness (never assume output type).**
- **After any change, run a full end-to-end test with a real question.**
- **If a new error appears, document the fix in a troubleshooting section.**
- **Never finish a task without updating `WORKFLOW.md` when behavior changed.**

---

## Example: Adding a New Graphical Intent

1. Add a new object to `intent_templates.json` with all required fields.
2. Implement the helper function to return a dict with `image_path` and `base64_image`.
3. Validate `intent_templates.json`.
4. Run the CLI with a matching question and check the output.
5. If the output is correct, commit the change. If not, debug using the steps above.

## Example: Quick E2E Verification Command

Use this pattern for a compact runtime check:

```bash
MYSQL_PASSWORD='***' python3 - <<'PY'
from scripts.helpers.league_records import DBConfig
from scripts.helpers.dynamic_helper_manager import answer_question_with_helpers

db = DBConfig(user='root', password='***')
q = 'graph of the goals scored by inter, milan, juventus and napoli in the last 10 years'
res = answer_question_with_helpers(question=q, db=db)
print('intent:', res.get('intent'))
print('image:', bool(res.get('image')))
print('base64_image:', bool(res.get('base64_image')))
print('image_path:', (res.get('meta') or {}).get('image_path'))
print('team_badges:', (res.get('meta') or {}).get('team_badges'))
PY
```

---

**Keep this file up to date as the system evolves.**
