"""
STEP 3 — Fetch sport_event_timeline for completed matches.

CSV mode:
• Reads match IDs from data/schedule.csv
• Only fetches matches with status in COMPLETED_STATUSES
• Caches raw JSON responses in data/timelines/<sport_event_id>.json

Supabase weekly (--full-backfill): completed games missing a timelines row.

Supabase daily (--daily): ``main_daily_timeline_kickoff_window`` reads All Games whose kickoff
UTC date is yesterday–tomorrow (configurable), fetches timeline.json per sport_event_id, and
stores only when ``sport_event_status.status`` is closed/ended — no season schedule pagination.

Respects the 1 req/sec trial-key rate limit.
"""

import csv
import json
import os
import time
import urllib.request
import urllib.error

from config import (
    API_KEY,
    BASE_URL,
    SCHEDULE_CSV,
    TIMELINES_DIR,
    COMPLETED_STATUSES,
    PIPELINE_DAILY_TIMELINE_KICKOFF_DAYS_AFTER,
    PIPELINE_DAILY_TIMELINE_KICKOFF_DAYS_BEFORE,
    REQUEST_DELAY_SECONDS,
    USE_SUPABASE,
)


def load_completed_matches(csv_path: str) -> list[dict]:
    """Return rows from schedule CSV where status is completed."""
    if not os.path.exists(csv_path):
        raise FileNotFoundError(
            f"{csv_path} not found — run step2_get_schedule.py first"
        )
    rows = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["status"] in COMPLETED_STATUSES:
                rows.append(row)
    return rows


def fetch_timeline(sport_event_id: str) -> dict:
    """Fetch the timeline JSON for a single sport event."""
    url = f"{BASE_URL}/sport_events/{sport_event_id}/timeline.json?api_key={API_KEY}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


def cache_path(sport_event_id: str) -> str:
    safe_id = sport_event_id.replace(":", "_")
    return os.path.join(TIMELINES_DIR, f"{safe_id}.json")


def timeline_feed_marked_completed(data: dict) -> bool:
    """True when Sportradar timeline payload shows a finished fixture (closed / ended)."""
    ses = data.get("sport_event_status") or {}
    return (ses.get("status") or "") in COMPLETED_STATUSES


def main_daily_timeline_kickoff_window(
    *,
    days_before: int | None = None,
    days_after: int | None = None,
) -> int:
    """
    Daily path: Supabase games in a UTC kickoff date window → timeline.json each → upsert only if completed.

    Returns the number of timelines newly stored.
    """
    if not USE_SUPABASE:
        raise RuntimeError("main_daily_timeline_kickoff_window requires USE_SUPABASE")

    import db

    b = PIPELINE_DAILY_TIMELINE_KICKOFF_DAYS_BEFORE if days_before is None else days_before
    a = PIPELINE_DAILY_TIMELINE_KICKOFF_DAYS_AFTER if days_after is None else days_after

    db.get_or_create_seasons()
    candidates = db.get_games_kickoff_utc_calendar_window_for_configured_seasons(
        days_before=b, days_after=a
    )
    game_ids = [str(r["id"]) for r in candidates if r.get("id")]
    existing_tl = db.get_game_ids_with_timeline_among(game_ids)

    print(
        f"Daily timeline scan: {len(candidates)} game(s) with kickoff UTC date in "
        f"[today-{b}, today+{a}] inclusive ({len(existing_tl)} already have a timeline row).",
        flush=True,
    )

    stored = 0
    skipped_have_timeline = 0
    skipped_not_finished = 0
    errors = 0

    for i, match in enumerate(candidates, 1):
        game_id = match.get("id")
        event_id = match.get("sport_event_id") or ""
        if not game_id or not event_id:
            continue
        gid_s = str(game_id)
        if gid_s in existing_tl:
            skipped_have_timeline += 1
            continue

        home = match.get("home_team", "")
        away = match.get("away_team", "")
        start_time = match.get("start_time", "")
        date = str(start_time)[:10] if start_time else "?"
        print(f"[{i}/{len(candidates)}] Timeline probe {date}  {home} vs {away}  ({event_id})")

        try:
            data = fetch_timeline(event_id)
            if not timeline_feed_marked_completed(data):
                skipped_not_finished += 1
                continue
            db.upsert_timeline(
                gid_s,
                data,
                sport_event_id=event_id,
                start_time=match.get("start_time"),
            )
            existing_tl.add(gid_s)
            stored += 1
        except urllib.error.HTTPError as e:
            print(f"  HTTP {e.code} — skipping")
            errors += 1
        except Exception as e:
            print(f"  Error: {e} — skipping")
            errors += 1

        time.sleep(REQUEST_DELAY_SECONDS)

    print("\nDaily timeline window done.")
    print(f"  Newly stored (completed) : {stored}")
    print(f"  Already had timeline   : {skipped_have_timeline}")
    print(f"  Not finished in feed   : {skipped_not_finished}")
    print(f"  Errors                 : {errors}")
    print('  Timelines in Supabase public."Completed Matches - full sport_event_timelines"')
    return stored


def main(recent_start_days: int | None = None):
    """
    Fetch timelines for completed matches missing storage.

    When ``recent_start_days`` is set (Supabase daily mode), only matches whose kickoff is
    within that many days are considered; older gaps are left for a full backfill run.
    """
    os.makedirs(TIMELINES_DIR, exist_ok=True)

    if USE_SUPABASE:
        import db
        db.get_or_create_seasons()
        if recent_start_days is not None and recent_start_days > 0:
            matches = db.get_completed_matches_without_timeline_for_configured_seasons_recent(
                recent_start_days=recent_start_days,
            )
            print(
                f"Completed matches without timeline (Supabase, kickoff within last "
                f"{recent_start_days} d): {len(matches)}"
            )
        else:
            matches = db.get_completed_matches_without_timeline_for_configured_seasons()
            print(f"Completed matches without timeline (from Supabase): {len(matches)}")
    else:
        matches = load_completed_matches(SCHEDULE_CSV)
        print(f"Completed matches to process: {len(matches)}")

    fetched = 0
    skipped = 0
    errors = 0

    for i, match in enumerate(matches, 1):
        event_id = match["sport_event_id"]
        game_id = match.get("id")  # games.id when from Supabase
        out_path = cache_path(event_id)

        if USE_SUPABASE:
            skip = False  # We only got matches without timeline
        else:
            skip = os.path.exists(out_path)

        if skip:
            skipped += 1
            continue

        home = match.get("home_team", "")
        away = match.get("away_team", "")
        start_time = match.get("start_time", "")
        date = str(start_time)[:10] if start_time else "?"
        print(f"[{i}/{len(matches)}] Fetching {date}  {home} vs {away}  ({event_id})")

        try:
            data = fetch_timeline(event_id)
            if USE_SUPABASE and game_id:
                db.upsert_timeline(
                    game_id,
                    data,
                    sport_event_id=event_id,
                    start_time=match.get("start_time"),
                )
            if not USE_SUPABASE:
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False)
            fetched += 1
        except urllib.error.HTTPError as e:
            print(f"  HTTP {e.code} — skipping")
            errors += 1
        except Exception as e:
            print(f"  Error: {e} — skipping")
            errors += 1

        time.sleep(REQUEST_DELAY_SECONDS)

    print(f"\nDone.")
    print(f"  Newly fetched : {fetched}")
    print(f"  Already cached: {skipped}")
    print(f"  Errors        : {errors}")
    if USE_SUPABASE:
        print('  Timelines saved to Supabase public."Completed Matches - full sport_event_timelines"')
    else:
        print(f"\nTimelines saved in: {TIMELINES_DIR}/")


if __name__ == "__main__":
    main()
