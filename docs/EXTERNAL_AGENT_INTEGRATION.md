# External Agent Integration Guide

## Goal
Use the existing data and helper scripts through a stable HTTP API so a website frontend can call them safely.

## Why this helps
- External AI agents should not run shell commands directly in production.
- Frontends should call a backend API, not MySQL directly.
- This wrapper enforces one contract for question answering and freshness policy.

## New API Bridge
File:
- [scripts/web/agent_api_server.py](../scripts/web/agent_api_server.py)

Formal API spec:
- [docs/openapi/agent_api_openapi.yaml](openapi/agent_api_openapi.yaml)

Contract maintenance rule:
- Any behavior change in `scripts/web/agent_api_server.py` must update `docs/openapi/agent_api_openapi.yaml` in the same commit/PR.

Endpoints:
- GET /health
- POST /v1/question

## Start the service
Example on this Linux environment:

1) Load env
- source ./.cron.env

2) Run server
- conda run -p /home/lpanzieri/Data-Analysis/.conda --no-capture-output python scripts/web/agent_api_server.py --bind 127.0.0.1 --port 8081 --allowed-origin https://your-frontend-domain.example --rate-limit-rpm 60

If a reverse proxy is used, keep bind as 127.0.0.1 and expose via nginx or similar.

3) Set auth token in backend env
- export AGENT_API_TOKEN="your-strong-random-token"

## Request contract
POST /v1/question JSON body:
- question: string (required)
- local_only: boolean (optional, default false)
- freshness_days_back: integer (optional, default 3)
- no_cache: boolean (optional, default false)
- cache_ttl_seconds: integer or null (optional)

Required header when `AGENT_API_TOKEN` is set:
- Authorization: Bearer <token>

Default behavior:
- If local_only is false, the API wrapper attempts a small freshness sync against API data before answering.
- If local_only is true, it skips API sync and answers from local data only.

## Response contract
Success response shape:
- ok: true
- freshness: object describing whether API freshness sync was performed
- answer: original output from answer_question_with_helpers

Error response shape:
- ok: false
- error: short code
- detail: optional detail string for debugging

Rate limit response extras:
- HTTP 429
- retry_after_seconds in body
- Retry-After response header

## Website frontend pattern
Recommended flow from website frontend:
1. User sends natural-language question.
2. Frontend POSTs to backend endpoint /v1/question.
3. Frontend renders answer and image payload fields if present.
4. Frontend never stores DB credentials and never calls MySQL directly.
5. Frontend sends bearer token only to your backend endpoint, never to third-party domains.

Reference frontend snippet:
- [docs/examples/frontend_question_client.html](examples/frontend_question_client.html)

## Security and deployment notes
- Use AGENT_API_ALLOWED_ORIGIN or --allowed-origin to lock CORS to your frontend domain.
- Keep secrets only in backend environment variables.
- Put this service behind HTTPS via reverse proxy.
- Use AGENT_API_TOKEN (or --api-token-env) to require bearer authentication.
- Use AGENT_API_RATE_LIMIT_RPM (or --rate-limit-rpm) to cap request rate per client.

## Suggested next improvements
- Add request and latency logging to logs directory.
- Containerize service for easier deployment.
- Add persistent distributed rate limiting if you run multiple server instances.
