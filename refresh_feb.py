"""Deletes cached timeline files for all completed matches from 2026-02-01 onwards,
then re-fetches them fresh from the Sportradar API."""

import csv
import os
import json
import time
import urllib.request
import urllib.error

from config import API_KEY, BASE_URL, SCHEDULE_CSV, TIMELINES_DIR, REQUEST_DELAY_SECONDS

REFRESH_FROM = "2026-02-01"


def cache_path(sport_event_id: str) -> str:
    safe_id = sport_event_id.replace(":", "_")
    return os.path.join(TIMELINES_DIR, f"{safe_id}.json")


def main():
    with open(SCHEDULE_CSV, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    targets = [
        r for r in rows
        if r["start_time"] >= REFRESH_FROM and r["status"] in ("closed", "ended")
    ]
    print(f"Completed matches from {REFRESH_FROM} onwards: {len(targets)}")

    # Delete stale cache files
    deleted = 0
    for r in targets:
        path = cache_path(r["sport_event_id"])
        if os.path.exists(path):
            os.remove(path)
            deleted += 1
    print(f"Deleted {deleted} stale cache files\n")

    # Re-fetch
    fetched = 0
    errors = 0
    for i, r in enumerate(targets, 1):
        event_id = r["sport_event_id"]
        date = r["start_time"][:10]
        home = r["home_team"]
        away = r["away_team"]
        print(f"[{i}/{len(targets)}] {date}  {home} vs {away}")
        url = f"{BASE_URL}/sport_events/{event_id}/timeline.json?api_key={API_KEY}"
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode())
            with open(cache_path(event_id), "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
            fetched += 1
        except urllib.error.HTTPError as e:
            print(f"  HTTP {e.code} - skipping")
            errors += 1
        except Exception as e:
            print(f"  Error: {e} - skipping")
            errors += 1
        time.sleep(REQUEST_DELAY_SECONDS)

    print(f"\nDone.  Fetched: {fetched}  Errors: {errors}")
    print("\nNow re-extracting own goals and regenerating report...")

    import step4_extract_own_goals
    import generate_report

    step4_extract_own_goals.main()

    rows_og = generate_report.load_own_goals(generate_report.OWN_GOALS_CSV)
    rows_og.sort(key=lambda r: (r["match_date"], int(r["minute"]) if str(r["minute"]).isdigit() else 0))
    completed = generate_report.count_completed_matches()
    html = generate_report.generate_html(rows_og, completed, {})
    with open(generate_report.REPORT_HTML, "w", encoding="utf-8") as f:
        f.write(html)
    generate_report.write_legacy_report_redirect()
    generate_report.write_derived_reports()
    print(f"Report updated: {generate_report.REPORT_HTML} (+ companion HTML)")


if __name__ == "__main__":
    main()
