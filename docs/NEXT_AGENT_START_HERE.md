# Next Agent Start Here

If you are a new Copilot session resuming API-Football ingestion work in this repo, read exactly one of these first:

1. Linux session on this machine:
   [docs/LINUX_AGENT_HANDOFF_20260425.md](LINUX_AGENT_HANDOFF_20260425.md)
2. Windows session on another machine:
   [docs/WINDOWS_AGENT_HANDOFF_20260425.md](WINDOWS_AGENT_HANDOFF_20260425.md)

## Then do this
1. Validate schema with [setup_schema.py](../setup_schema.py).
2. Query DB coverage before running anything.
3. Check recent `429` timestamps in `event_api_call_log`.
4. If rate limiting is recent, wait about 60 minutes.
5. Resume only missing league-season slices.
6. Prefer `--skip-lineups-sync` first if the goal is to maximize progress safely.
7. For any team chart or graphic, include badges.
8. Export charts in 4K by default.
9. Always consider creating or extending a reusable helper `.py` function when a task is likely to recur.
10. Unless the user explicitly asks for local-only work, verify local data freshness against API data first.

## Relevant Files
- [sync_api_football_events.py](../sync_api_football_events.py)
- [docs/RUNBOOK.md](RUNBOOK.md)
- [docs/LINUX_AGENT_HANDOFF_20260425.md](LINUX_AGENT_HANDOFF_20260425.md)
- [docs/WINDOWS_AGENT_HANDOFF_20260425.md](WINDOWS_AGENT_HANDOFF_20260425.md)
- [scripts/analysis/generate_goal_heatmap.py](../scripts/analysis/generate_goal_heatmap.py)
- [scripts/web/agent_api_server.py](../scripts/web/agent_api_server.py)
- [docs/EXTERNAL_AGENT_INTEGRATION.md](EXTERNAL_AGENT_INTEGRATION.md)
- [docs/examples/frontend_question_client.html](examples/frontend_question_client.html)
