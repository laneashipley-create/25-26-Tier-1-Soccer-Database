"""
Compare regular timeline status vs extended timeline status for matches that are still
missing rows in public."Completed Matches - Extended Timeline".

Outputs CSV with columns:
1) match date
2) sr:sport_event:ID
3) Regular Timeline Status
4) Extended Timeline Status
"""

from __future__ import annotations

import csv
import json
import urllib.request

import db
from config import API_KEY, BASE_URL, BASE_URL_SOCCER_EXTENDED

OUT_CSV = "data/regular_vs_extended_timeline_status.csv"


def _fetch_status(url: str) -> str:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode())
    ses = data.get("sport_event_status") or {}
    return str(ses.get("status") or "")


def _match_date(start_time: object) -> str:
    s = str(start_time or "").strip()
    return s[:10] if len(s) >= 10 else ""


def main() -> int:
    rows = db.get_completed_matches_without_extended_timeline_for_configured_seasons()
    out_rows: list[dict[str, str]] = []

    for row in rows:
        event_id = str(row.get("sport_event_id") or "").strip()
        if not event_id:
            continue
        regular_url = f"{BASE_URL}/sport_events/{event_id}/timeline.json?api_key={API_KEY}"
        extended_url = (
            f"{BASE_URL_SOCCER_EXTENDED}/sport_events/{event_id}/extended_timeline.json"
            f"?api_key={API_KEY}"
        )
        try:
            regular_status = _fetch_status(regular_url)
        except Exception as e:
            regular_status = f"ERROR: {e}"
        try:
            extended_status = _fetch_status(extended_url)
        except Exception as e:
            extended_status = f"ERROR: {e}"

        out_rows.append(
            {
                "match date": _match_date(row.get("start_time")),
                "sr:sport_event:ID": event_id,
                "Regular Timeline Status": regular_status,
                "Extended Timeline Status": extended_status,
            }
        )

    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "match date",
                "sr:sport_event:ID",
                "Regular Timeline Status",
                "Extended Timeline Status",
            ],
        )
        writer.writeheader()
        writer.writerows(out_rows)

    print(f"Wrote {len(out_rows)} row(s) to {OUT_CSV}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
