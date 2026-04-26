#!/usr/bin/env python3
"""Generate an HTML snapshot of helper advancement status.

The report is read-only and focuses on:
- current helper template coverage
- registry and generated-helper health
- unknown-question backlog and how much is now covered
"""

from __future__ import annotations

import argparse
import datetime as dt
import html
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Sequence


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.helpers.dynamic_helper_manager import infer_intent


HELPERS_ROOT = ROOT / "scripts" / "helpers"
REGISTRY_PATH = HELPERS_ROOT / "helper_registry.json"
TEMPLATES_PATH = HELPERS_ROOT / "intent_templates.json"
UNKNOWN_LOG_PATH = HELPERS_ROOT / "unknown_questions.log"
GENERATED_DIR = HELPERS_ROOT / "generated"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate helper advancement HTML report")
    parser.add_argument(
        "--out",
        default="",
        help="Optional explicit output path. Defaults to plans/reports/helper_advancement_report_<timestamp>.html",
    )
    return parser.parse_args()


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def classify_cell(value: object) -> str:
    if value is None:
        return ""
    text = str(value)
    if text in {"OK", "COVERED", "COVERED_AND_REGISTERED", "PRESENT", "YES", "true", "True"}:
        return "cell-good"
    if text in {"PARTIAL", "COVERED_NOT_REGISTERED", "MISSING_FILE", "NO", "false", "False"}:
        return "cell-warn"
    if text in {"UNKNOWN", "UNREGISTERED", "MISSING", "EMPTY"}:
        return "cell-bad"
    return ""


def render_table(columns: Sequence[str], rows: Iterable[Sequence[object]]) -> str:
    header_html = "".join(f"<th>{html.escape(str(col))}</th>" for col in columns)
    body_rows = []
    for row in rows:
        cells = "".join(
            f'<td class="{classify_cell(value)}">{html.escape("" if value is None else str(value))}</td>'
            for value in row
        )
        body_rows.append(f"<tr>{cells}</tr>")
    body_html = "".join(body_rows) if body_rows else f'<tr><td colspan="{len(columns)}">No rows</td></tr>'
    return f'<table class="report-table"><thead><tr>{header_html}</tr></thead><tbody>{body_html}</tbody></table>'


def render_kpi_cards(cards: Sequence[dict[str, str]]) -> str:
    items = []
    for card in cards:
        items.append(
            "".join(
                [
                    '<div class="kpi-card">',
                    f'<div class="kpi-label">{html.escape(card["label"])}</div>',
                    f'<div class="kpi-value">{html.escape(card["value"])}</div>',
                    f'<div class="kpi-note">{html.escape(card["note"])}</div>',
                    "</div>",
                ]
            )
        )
    return f'<section class="card col-12"><h2>Snapshot Highlights</h2><div class="kpi-grid">{"".join(items)}</div></section>'


def render_bar_chart(title: str, rows: Sequence[dict[str, object]], value_key: str, note_key: str) -> str:
    max_value = max((float(row[value_key]) for row in rows), default=0.0)
    body = []
    for row in rows:
        label = html.escape(str(row["label"]))
        value = float(row[value_key])
        note = html.escape(str(row[note_key]))
        width_pct = 0.0 if max_value <= 0 else round((value * 100.0) / max_value, 2)
        value_text = html.escape(str(row["value_text"]))
        body.append(
            "".join(
                [
                    '<div class="bar-row">',
                    '<div class="bar-head">',
                    f'<span class="bar-label">{label}</span>',
                    f'<span class="bar-value">{value_text}</span>',
                    "</div>",
                    f'<div class="bar-track"><div class="bar-fill" style="width: {width_pct}%"></div></div>',
                    f'<div class="bar-note">{note}</div>',
                    "</div>",
                ]
            )
        )
    return f'<section class="card col-6"><h2>{html.escape(title)}</h2><div class="bar-chart">{"".join(body)}</div></section>'


def render_summary(title: str, items: Sequence[str]) -> str:
    content = "".join(f"<li>{html.escape(item)}</li>" for item in items)
    return f'<section class="card col-12"><h2>{html.escape(title)}</h2><ul class="summary-list">{content}</ul></section>'


def report_header(snapshot_time: str) -> str:
    return f"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Helper Advancement Report</title>
  <style>
    :root {{
      --bg: #f6f2ea;
      --panel: #fffdf9;
      --ink: #1d2a33;
      --muted: #5b6b76;
      --line: #d9d3c5;
      --accent: #0f766e;
      --ok: #0f766e;
      --warn: #9a6700;
      --bad: #b42318;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      color: var(--ink);
      font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
      background: radial-gradient(circle at top left, #fff8ea 0, var(--bg) 42%), linear-gradient(180deg, #f6f2ea 0%, #efe7da 100%);
    }}
    .wrap {{ max-width: 1360px; margin: 0 auto; padding: 28px 18px 40px; }}
    .hero, .card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 16px;
      box-shadow: 0 8px 22px rgba(15, 118, 110, 0.08);
    }}
    .hero {{ padding: 22px; }}
    .grid {{ display: grid; grid-template-columns: repeat(12, 1fr); gap: 14px; margin-top: 14px; }}
    .card {{ padding: 16px; }}
    .col-12 {{ grid-column: span 12; }}
    .col-6 {{ grid-column: span 6; }}
    h1 {{ margin: 0 0 8px; font-size: 30px; color: var(--ink); }}
    h2 {{ margin: 0 0 12px; font-size: 18px; color: var(--ink); }}
    p {{ margin: 0; }}
    .sub {{ color: var(--muted); font-size: 14px; }}
    .legend {{ display: flex; gap: 8px; flex-wrap: wrap; margin-top: 10px; }}
    .chip {{ padding: 4px 8px; border-radius: 999px; font-size: 12px; border: 1px solid var(--line); background: #fff; }}
    .chip.ok {{ color: var(--ok); background: #e8f8f4; border-color: #98d8ca; }}
    .chip.warn {{ color: var(--warn); background: #fff4d8; border-color: #f0d28a; }}
    .chip.bad {{ color: var(--bad); background: #fdecec; border-color: #f1b3aa; }}
    .search-box {{ display: flex; gap: 12px; margin-top: 18px; align-items: center; }}
    .search-input {{ flex: 1; padding: 12px 14px; border: 2px solid var(--line); border-radius: 10px; font-size: 14px; font-family: inherit; background: #fff; color: var(--ink); }}
    .search-input:focus {{ outline: none; border-color: var(--accent); }}
    .search-hint {{ font-size: 12px; color: var(--muted); }}
    .kpi-grid {{ display: grid; grid-template-columns: repeat(6, 1fr); gap: 12px; }}
    .kpi-card {{ border: 1px solid var(--line); border-radius: 14px; padding: 14px; background: linear-gradient(180deg, #fffdf9 0%, #f6f9f8 100%); }}
    .kpi-label {{ font-size: 12px; text-transform: uppercase; letter-spacing: 0.08em; color: var(--muted); margin-bottom: 8px; }}
    .kpi-value {{ font-size: 28px; line-height: 1; font-weight: 700; color: var(--ok); margin-bottom: 8px; }}
    .kpi-note {{ font-size: 12px; color: var(--muted); }}
    .bar-chart {{ display: grid; gap: 10px; }}
    .bar-row {{ border: 1px solid #dfddd5; border-radius: 12px; padding: 10px 12px; background: #fcfbf7; }}
    .bar-head {{ display: flex; justify-content: space-between; gap: 12px; font-size: 13px; margin-bottom: 6px; }}
    .bar-label {{ font-weight: 600; color: var(--ink); }}
    .bar-value {{ color: var(--muted); }}
    .bar-track {{ width: 100%; height: 10px; background: #e8e2d6; border-radius: 999px; overflow: hidden; }}
    .bar-fill {{ height: 100%; background: linear-gradient(90deg, #0f766e 0%, #14b8a6 100%); border-radius: 999px; }}
    .bar-note {{ margin-top: 6px; font-size: 12px; color: var(--muted); }}
    .summary-list {{ margin: 0; padding-left: 18px; display: grid; gap: 8px; }}
    .report-table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    .report-table th, .report-table td {{ text-align: left; padding: 8px 9px; border-bottom: 1px solid #e7e0d5; vertical-align: top; }}
    .report-table thead th {{ background: #efe7da; position: sticky; top: 0; z-index: 1; color: #2b4b46; font-weight: 600; }}
    .report-table tbody tr:nth-child(even) {{ background: #fcfaf5; }}
    .cell-good {{ color: var(--ok); font-weight: 600; }}
    .cell-warn {{ color: var(--warn); font-weight: 600; }}
    .cell-bad {{ color: var(--bad); font-weight: 600; }}
    .hidden {{ display: none !important; }}
    @media (max-width: 980px) {{
      .col-6, .col-12 {{ grid-column: span 12; }}
      .kpi-grid {{ grid-template-columns: repeat(2, 1fr); }}
      h1 {{ font-size: 24px; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>Helper Advancement Report</h1>
      <p class="sub">Snapshot generated: {html.escape(snapshot_time)}. Scope: helper template coverage, generated helper registry health, and unknown-question backlog advancement.</p>
      <div class="legend">
        <span class="chip ok">COVERED means the current templates classify the question</span>
        <span class="chip warn">COVERED_NOT_REGISTERED means a template exists but no registry entry exists yet</span>
        <span class="chip bad">UNKNOWN means the backlog item still has no matching template</span>
      </div>
      <div class="search-box">
        <input type="text" id="searchInput" class="search-input" placeholder="Filter tables, sections, and text across the report...">
        <span class="search-hint">Type to filter</span>
      </div>
    </section>
    <div class="grid">
"""


def report_footer() -> str:
    return """
    </div>
  </div>
  <script>
    (function() {
      const searchInput = document.getElementById('searchInput');
      if (!searchInput) return;

      function normalizeText(text) {
        return (text || '').toLowerCase().trim();
      }

      function getElementText(el) {
        let text = '';
        if (el.nodeType === Node.TEXT_NODE) {
          return el.textContent;
        }
        for (let node of el.childNodes) {
          text += getElementText(node);
        }
        return text;
      }

      function filter(query) {
        const cards = document.querySelectorAll('.card');
        const normalizedQuery = normalizeText(query);
        cards.forEach(card => {
          const text = normalizeText(getElementText(card));
          const visible = !normalizedQuery || text.includes(normalizedQuery);
          card.classList.toggle('hidden', !visible);
        });
      }

      searchInput.addEventListener('input', (e) => filter(e.target.value));
    })();
  </script>
</body>
</html>
"""


def main() -> None:
    args = parse_args()
    timestamp = dt.datetime.now(dt.timezone.utc)
    output_path = Path(args.out) if args.out else Path(
        f"plans/reports/helper_advancement_report_{timestamp.strftime('%Y%m%d_%H%M%S')}.html"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)

    templates = load_json(TEMPLATES_PATH, [])
    registry = load_json(REGISTRY_PATH, {})
    unknown_rows = load_jsonl(UNKNOWN_LOG_PATH)

    generated_files = sorted(
        p for p in GENERATED_DIR.glob("*.py") if p.name != "__init__.py"
    )

    registry_rows: list[list[object]] = []
    missing_registry_rows: list[list[object]] = []
    registry_by_intent: Counter[str] = Counter()
    registry_missing_count = 0

    for helper_key, entry in sorted(registry.items()):
        intent = str(entry.get("intent", ""))
        registry_by_intent[intent] += 1
        helper_file = Path(str(entry.get("helper_file", "")))
        exists = helper_file.exists()
        if not exists:
            registry_missing_count += 1
            missing_registry_rows.append(
                [
                    helper_key,
                    intent,
                    entry.get("league_code") or "ALL",
                    str(helper_file),
                    entry.get("source_question_example", ""),
                ]
            )
        registry_rows.append(
            [
                helper_key,
                intent,
                entry.get("league_code") or "ALL",
                "PRESENT" if exists else "MISSING_FILE",
                helper_file.name,
                json.dumps(entry.get("helper_kwargs") or {}, ensure_ascii=True),
                entry.get("source_question_example", ""),
            ]
        )

    template_rows: list[list[object]] = []
    template_by_intent: Counter[str] = Counter()
    for tmpl in templates:
        intent = str(tmpl.get("intent", "unknown"))
        template_by_intent[intent] += 1
        phrases = tmpl.get("match_phrases") or []
        template_rows.append(
            [
                intent,
                tmpl.get("helper_function", ""),
                len(phrases) if isinstance(phrases, list) else 0,
                tmpl.get("requires_league", False),
                tmpl.get("pass_league_code", False),
                tmpl.get("league_code") or "",
                json.dumps(tmpl.get("kwargs") or {}, ensure_ascii=True),
                ", ".join(str(item) for item in (tmpl.get("dynamic_kwargs") or [])),
            ]
        )

    backlog_rows: list[list[object]] = []
    advancement_status_counts: Counter[str] = Counter()
    backlog_by_intent: Counter[str] = Counter()
    now_covered_registered = 0
    now_covered_unregistered = 0
    still_unknown = 0

    for row in unknown_rows:
        question = str(row.get("question", ""))
        intent_info = infer_intent(question)
        current_intent = intent_info.intent
        current_league_code = intent_info.league_code or ""
        matched_registry = "NO"

        if current_intent == "unknown":
            advancement_status = "UNKNOWN"
            still_unknown += 1
        else:
            matched_registry = "YES" if any(
                str(entry.get("source_question_example", "")) == question
                or (
                    str(entry.get("intent", "")) == current_intent
                    and str(entry.get("league_code") or "") == current_league_code
                )
                for entry in registry.values()
            ) else "NO"
            if matched_registry == "YES":
                advancement_status = "COVERED_AND_REGISTERED"
                now_covered_registered += 1
            else:
                advancement_status = "COVERED_NOT_REGISTERED"
                now_covered_unregistered += 1

        advancement_status_counts[advancement_status] += 1
        backlog_by_intent[current_intent] += 1
        backlog_rows.append(
            [
                row.get("timestamp_utc", ""),
                question,
                row.get("league_code") or "",
                current_intent,
                current_league_code,
                advancement_status,
                matched_registry,
            ]
        )

    intent_chart_rows = []
    all_intents = sorted(set(template_by_intent) | set(registry_by_intent))
    for intent in all_intents:
        template_count = template_by_intent.get(intent, 0)
        registry_count = registry_by_intent.get(intent, 0)
        intent_chart_rows.append(
            {
                "label": intent,
                "value": registry_count,
                "value_text": f"registry={registry_count}",
                "note": f"templates={template_count}",
            }
        )

    backlog_chart_rows = [
        {
            "label": "Covered and registered",
            "value": now_covered_registered,
            "value_text": str(now_covered_registered),
            "note": "Backlog items that now map to an existing registry entry.",
        },
        {
            "label": "Covered not registered",
            "value": now_covered_unregistered,
            "value_text": str(now_covered_unregistered),
            "note": "Backlog items that now match a template but have no registry entry yet.",
        },
        {
            "label": "Still unknown",
            "value": still_unknown,
            "value_text": str(still_unknown),
            "note": "Backlog items that still have no template coverage.",
        },
    ]

    generated_missing_from_registry = max(0, len(generated_files) - len(registry))
    summary_items = [
        f"{len(unknown_rows)} backlog questions logged so far; {now_covered_registered + now_covered_unregistered} are now covered by the current templates.",
        f"{now_covered_unregistered} covered backlog questions still need a registry/materialization pass if you want them pre-generated.",
        f"Registry contains {len(registry)} helper entries; {registry_missing_count} point to helper files that are currently missing on disk.",
        f"Generated helper directory currently holds {len(generated_files)} Python helper files on disk.",
    ]
    if generated_missing_from_registry:
        summary_items.append(
            f"Generated directory contains {generated_missing_from_registry} extra helper file(s) beyond the registry count; inspect if cleanup is needed."
        )
    if still_unknown:
        summary_items.append(f"{still_unknown} backlog question(s) are still unknown and need new template coverage.")

    cards = [
        {
            "label": "Template Intents",
            "value": str(len(templates)),
            "note": f"Distinct configured templates in {TEMPLATES_PATH.name}",
        },
        {
            "label": "Registry Entries",
            "value": str(len(registry)),
            "note": f"Generated helper keys tracked in {REGISTRY_PATH.name}",
        },
        {
            "label": "Generated Files",
            "value": str(len(generated_files)),
            "note": f"Python helper files currently on disk under generated/",
        },
        {
            "label": "Missing Helper Files",
            "value": str(registry_missing_count),
            "note": "Registry entries whose helper_file path is missing on disk",
        },
        {
            "label": "Backlog Covered Now",
            "value": str(now_covered_registered + now_covered_unregistered),
            "note": f"Out of {len(unknown_rows)} logged unknown questions",
        },
        {
            "label": "Still Unknown",
            "value": str(still_unknown),
            "note": "Backlog items with no matching template today",
        },
    ]

    html_doc = "".join(
        [
            report_header(timestamp.strftime("%Y-%m-%d %H:%M:%S UTC")),
            render_kpi_cards(cards),
            render_summary("Advancement Summary", summary_items),
            render_bar_chart("Registry Entries by Intent", intent_chart_rows, "value", "note"),
            render_bar_chart("Unknown Backlog Advancement", backlog_chart_rows, "value", "note"),
            '<section class="card col-12"><h2>Template Coverage</h2>'
            + render_table(
                [
                    "intent",
                    "helper_function",
                    "phrase_count",
                    "requires_league",
                    "pass_league_code",
                    "template_league_code",
                    "static_kwargs",
                    "dynamic_kwargs",
                ],
                template_rows,
            )
            + "</section>",
            '<section class="card col-12"><h2>Registry Health</h2>'
            + render_table(
                [
                    "helper_key",
                    "intent",
                    "league_code",
                    "file_status",
                    "helper_file",
                    "helper_kwargs",
                    "source_question_example",
                ],
                registry_rows,
            )
            + "</section>",
            '<section class="card col-12"><h2>Unknown Question Backlog</h2>'
            + render_table(
                [
                    "timestamp_utc",
                    "question",
                    "logged_league_code",
                    "current_intent",
                    "current_league_code",
                    "advancement_status",
                    "matched_registry",
                ],
                backlog_rows,
            )
            + "</section>",
            '<section class="card col-12"><h2>Registry Entries With Missing Helper Files</h2>'
            + render_table(
                [
                    "helper_key",
                    "intent",
                    "league_code",
                    "helper_file",
                    "source_question_example",
                ],
                missing_registry_rows,
            )
            + "</section>",
            report_footer(),
        ]
    )

    output_path.write_text(html_doc, encoding="utf-8")
    print(f"REPORT_PATH={output_path}")


if __name__ == "__main__":
    main()