"""
Generate standalone water-break unpaired HTML report from existing Supabase tables.

This does not require report_hub integration and only builds:
  - report_water_break_unpaired.html
"""

from __future__ import annotations

from config import (
    REPORT_BLURB_WATER_BREAK_UNPAIRED,
    REPORT_HTML_WATER_BREAK_UNPAIRED,
    SEASON_LABEL,
    USE_SUPABASE,
)
from step7_generate_reports import (
    _derived_build_table,
    _derived_filter_bundle,
    _derived_page_shell,
)


def main() -> None:
    if not USE_SUPABASE:
        raise SystemExit(
            "USE_SUPABASE is False. Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (or config_local) first."
        )

    import db

    rows = db.fetch_water_break_unpaired_match_rows()
    for i, row in enumerate(rows, start=1):
        row["row_num"] = i

    headers = [
        "#",
        "ID",
        "Date",
        "title",
        "competition name",
        "competition ID",
        "sport event timeline - water_break_start",
        "sport event timeline - water_break_end",
        "delta (start - end)",
    ]
    keys = [
        "row_num",
        "sport_event_id",
        "sport_event_start",
        "title",
        "competition_name",
        "sportradar_competition_id",
        "sport_timeline_water_break_start",
        "sport_timeline_water_break_end",
        "sport_timeline_delta",
    ]

    payload_html, controls_html = _derived_filter_bundle(
        rows, comp_key="competition_name", date_key="sport_event_start"
    )
    table_html = _derived_build_table(
        headers,
        keys,
        rows,
        "table-water-break-unpaired",
        slicer_comp_key="competition_name",
        slicer_date_key="sport_event_start",
    )
    doc = _derived_page_shell(
        title=f"Water-break unpaired — {SEASON_LABEL}",
        badge="Sportradar Soccer",
        headline="Matches with unpaired water-break counts",
        subtitle=REPORT_BLURB_WATER_BREAK_UNPAIRED,
        meta="<strong>Review queue:</strong> matches where <code>water_break_start</code> and "
        "<code>water_break_end</code> differ in completed <code>sport_event timeline</code> data. "
        f"<strong>{len(rows):,}</strong> match(es) in this export.",
        table_html=table_html,
        nav_href="report_water_break_unpaired.html",
        filter_payload_html=payload_html,
        filter_controls_html=controls_html,
    )
    with open(REPORT_HTML_WATER_BREAK_UNPAIRED, "w", encoding="utf-8") as f:
        f.write(doc)
    print(f"Wrote {REPORT_HTML_WATER_BREAK_UNPAIRED} ({len(rows)} rows)")


if __name__ == "__main__":
    main()
