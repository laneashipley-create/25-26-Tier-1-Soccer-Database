"""
Regenerate report_recordings_library.html from soccer record replay list of sr sport event ids.json.

Drops unused venue/status/category columns and adds one Yes/No column per distinct apis[].description.
"""

from __future__ import annotations

import html
import json
import re
from datetime import datetime, timezone
from pathlib import Path

_DT_MIN = datetime.min.replace(tzinfo=timezone.utc)

RECORDINGS_JSON = "soccer record replay list of sr sport event ids.json"
REPORT_HTML = "report_recordings_library.html"


def parse_iso(s: object):
    from datetime import datetime as dt_mod, timezone as tz

    if s is None or s == "":
        return None
    raw = str(s).strip().replace("Z", "+00:00")
    try:
        d = dt_mod.fromisoformat(raw)
    except ValueError:
        return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=tz.utc)
    return d.astimezone(tz.utc)


def game_id_sort_key(gid: object) -> int:
    if not gid:
        return 0
    s = str(gid)
    if ":" in s:
        try:
            return int(s.split(":")[-1])
        except ValueError:
            pass
    return 0


def load_records(path: Path) -> list[dict]:
    doc = json.loads(path.read_text(encoding="utf-8"))
    recs = (((doc or {}).get("data") or {}).get("recordingsBySport") or [])
    return [x for x in recs if isinstance(x, dict)]


def collect_sorted_descriptions(records: list[dict]) -> list[str]:
    found: set[str] = set()
    for rec in records:
        for a in rec.get("apis") or []:
            if isinstance(a, dict):
                d = str(a.get("description") or "").strip()
                if d:
                    found.add(d)
    return sorted(found)


# apis[].description column order (report cols 7–19). Keys must match export JSON exactly.
# Headers use the same strings except Events → "Push - Events" (see description_display_name).
CANONICAL_API_DESCRIPTION_ORDER = [
    "Sport Event Timeline",
    "Sport Event Summary",
    "Sport Event Lineups",
    "Sport Event Extended Timeline",
    "Sport Event Extended Summary",
    "Season Standings",
    "Season Summaries",
    "Season Links",
    "Season Info",
    "Season Competitors",
    "Season Missing Players",
    "Season Leaders",
    "Events",
]


def order_description_columns(sorted_cols: list[str]) -> list[str]:
    """Apply canonical column order; unknown descriptions go before Events (still last)."""
    available = set(sorted_cols)
    ordered = [d for d in CANONICAL_API_DESCRIPTION_ORDER if d in available]
    unknown = sorted(available - set(ordered))
    if unknown:
        if "Events" in ordered:
            idx = ordered.index("Events")
            ordered = ordered[:idx] + unknown + ordered[idx:]
        else:
            ordered.extend(unknown)
    return ordered


def descriptions_for_record(rec: dict) -> set[str]:
    out: set[str] = set()
    for a in rec.get("apis") or []:
        if isinstance(a, dict):
            d = str(a.get("description") or "").strip()
            if d:
                out.add(d)
    return out


def build_rows(records: list[dict], description_cols: list[str]) -> list[dict]:
    rows: list[dict] = []
    EM = "\u2014"
    for rec in records:
        rid = rec.get("id")
        meta = rec.get("meta") if isinstance(rec.get("meta"), dict) else {}
        game_id = meta.get("gameId")
        if not rid or not game_id:
            continue

        sched_display = meta.get("scheduled")
        if not sched_display:
            dt = parse_iso(rec.get("scheduled"))
            if dt:
                sched_display = dt.strftime("%Y-%m-%dT%H:%M:%S") + "+00:00"

        dt_sort = parse_iso(sched_display) or parse_iso(rec.get("scheduled"))
        desc_set = descriptions_for_record(rec)

        rows.append(
            {
                "sort": (
                    dt_sort or _DT_MIN,
                    game_id_sort_key(game_id),
                ),
                "game_id": str(game_id),
                "rid": str(rid),
                "title": rec.get("title") or "",
                "league": rec.get("league") or "",
                "sched": sched_display or EM,
                "descriptions": desc_set,
            }
        )

    rows.sort(key=lambda x: x["sort"])
    return rows


def td_mono_code(val: str) -> str:
    esc = html.escape(val)
    dv = html.escape(val, quote=True)
    return f'<td class="mono" data-val="{dv}"><code>{esc}</code></td>'


def description_display_name(api_description: str) -> str:
    """Only Events uses a different header (endpoint name in JSON is still Events)."""
    if api_description == "Events":
        return "Push - Events"
    return api_description


def thead_rows(description_cols: list[str]) -> tuple[str, str]:
    """Return (header row html, filter row html). Column indices are sequential."""
    labels = [
        "ID",
        "sr_sport_event_id",
        "recording_id",
        "Title",
        "Competition Name",
        "Sport Event Start",
        *description_cols,
    ]
    th_cells = []
    filter_cells = []
    for i, lab in enumerate(labels):
        cls = ' class="col-api-desc"' if lab in description_cols else ""
        head_text = description_display_name(lab) if lab in description_cols else lab
        th_cells.append(f"<th{cls} data-col=\"{i}\" title=\"Click to sort\">{html.escape(head_text)}</th>")
        filter_cells.append(
            f"<th><button type=\"button\" class=\"excel-filter-btn\" data-col=\"{i}\" "
            f'data-table="table-recordings-library" title="Pick values like Excel">Values\u2026</button></th>'
        )
    return "<tr>" + "".join(th_cells) + "</tr>", "<tr class=\"derived-col-filters\">" + "".join(filter_cells) + "</tr>"


def tbody_rows(data_rows: list[dict], description_cols: list[str]) -> str:
    EM = "\u2014"
    lines: list[str] = []

    for i, r in enumerate(data_rows, start=1):
        t_esc = html.escape(r["title"])
        t_dv = html.escape(r["title"], quote=True)
        gid_esc = html.escape(r["game_id"])
        gid_dv = html.escape(r["game_id"], quote=True)
        rid_esc = html.escape(r["rid"])
        rid_dv = html.escape(r["rid"], quote=True)
        lg = r["league"] if r["league"] else EM
        lg_esc = html.escape(lg)
        lg_dv = html.escape(lg, quote=True)
        sch = r["sched"] if r["sched"] else EM
        sch_esc = html.escape(sch)
        sch_dv = html.escape(sch, quote=True)

        parts: list[str] = [
            f'<td data-val="{i}">{i}</td>',
            td_mono_code(r["game_id"]),
            td_mono_code(r["rid"]),
            f'<td data-val="{t_dv}">{t_esc}</td>',
            f'<td data-val="{lg_dv}">{lg_esc}</td>',
            f'<td data-val="{sch_dv}">{sch_esc}</td>',
        ]

        ds = r["descriptions"]
        for dcol in description_cols:
            yn = "Yes" if dcol in ds else "No"
            css = "cell-api-yes" if yn == "Yes" else "cell-api-no"
            parts.append(f'<td class="{css}" data-val="{yn}">{yn}</td>')

        lines.append("<tr>" + "".join(parts) + "</tr>")

    return "\n".join(lines)


def inject_api_cell_colors(report: str) -> str:
    """Green/red shading for Yes/No API columns (insert once)."""
    if "cell-api-yes" in report:
        return report
    return report.replace(
        "  </style>",
        """    #table-recordings-library tbody td.cell-api-yes {
      background: #cfead8;
      color: #0d3f18;
      font-weight: 600;
    }
    #table-recordings-library tbody td.cell-api-no {
      background: #fad4d6;
      color: #7a121c;
      font-weight: 600;
    }

  </style>""",
        1,
    )


def inject_css(report: str) -> str:
    needle = "\n    .report-sticky-top {"
    patch = """

    thead tr:first-child th.col-api-desc {
      white-space: normal;
      max-width: 8.5rem;
      line-height: 1.25;
      vertical-align: bottom;
      font-size: 0.72rem;
      font-weight: 600;
    }"""
    if "col-api-desc" not in report:
        return report.replace(needle, patch + needle, 1)
    return report


def main() -> None:
    root = Path(__file__).resolve().parent
    json_path = root / RECORDINGS_JSON
    report_path = root / REPORT_HTML

    records = load_records(json_path)
    description_cols = order_description_columns(collect_sorted_descriptions(records))
    data_rows = build_rows(records, description_cols)

    h1, h2 = thead_rows(description_cols)
    tbody = tbody_rows(data_rows, description_cols)
    inner = f"""        <thead>
          {h1}
          {h2}
        </thead>
        <tbody>
{tbody}
</tbody>"""

    report = report_path.read_text(encoding="utf-8")
    report = inject_css(report)
    report = inject_api_cell_colors(report)

    n = len(data_rows)
    nd = len(description_cols)
    meta_line = (
        f'  <p class="meta"><strong>{n}</strong> recording row(s). '
        f"<strong>ID</strong> is row order in this report (1 = oldest <strong>Sport Event Start</strong>). "
        f"After Sport Event Start, each column is one distinct <code>apis[].description</code> from the export JSON "
        f"(<strong>{nd}</strong> total); <strong>Yes</strong> is shaded green and <strong>No</strong> red. "
        f'Regenerate this page with <code>{Path(__file__).name}</code> after updating <code>{RECORDINGS_JSON}</code>. '
        f"Related Supabase sync: <code>sync_recordings_library.py</code>.</p>"
    )
    report = re.sub(
        r"\s*<p class=\"meta\">.*?</p>",
        "\n" + meta_line + "\n  ",
        report,
        count=1,
        flags=re.DOTALL,
    )

    gen = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    report = re.sub(
        r"(Generated )\d{4}-\d{2}-\d{2}[\s\d:]+UTC",
        rf"\g<1>{gen}",
        report,
        count=1,
    )

    t_open = '<table id="table-recordings-library" class="sortable-derived">'
    i1 = report.find(t_open)
    if i1 == -1:
        raise SystemExit("Could not find recordings library table opening tag.")
    i2 = report.find("</table>", i1)
    if i2 == -1:
        raise SystemExit("Could not find closing </table>.")
    report = report[:i1] + t_open + "\n" + inner + "\n      </table>" + report[i2 + len("</table>") :]

    report_path.write_text(report, encoding="utf-8")
    print(f"Wrote {report_path.name}: {n} rows, {nd} api-description columns.")


if __name__ == "__main__":
    main()
