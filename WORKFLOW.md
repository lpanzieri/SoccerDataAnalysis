# Dynamic Football Q&A System: Robust Workflow & Best Practices

## Purpose
This document provides a step-by-step workflow and best practices to efficiently add new questions, templates, or code changes to the dynamic football Q&A system, minimizing debugging cycles and ensuring robust automation.

## 0. Documentation Maintenance Rule (Mandatory)

- **Always update this file after every behavior-changing code/config change.**
  - If you touch intent routing, helper output format, cache behavior, schema assumptions, CLI usage, or API bridge behavior, reflect it here in the same PR/change.
  - Do not defer docs updates. `WORKFLOW.md` is part of the implementation, not optional commentary.
- **Update both what changed and how to validate it.**
  - Add one short bullet under the relevant section and one validation command/example.

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
- **If badges are null/missing:**
  - Inspect DB schema (`team` vs `team_badge`).
  - Verify league ID namespace mismatch and enable `league_name` fallback.
  - Validate blob/image decode paths and legend fallback.

## 5. LLM Agent Guidelines

- **Always validate JSON before using it.**
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
