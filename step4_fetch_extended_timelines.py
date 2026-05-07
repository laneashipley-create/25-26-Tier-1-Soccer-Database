"""
STEP 3X — Fetch sport_event extended_timeline for completed matches.

Supabase weekly (--full-backfill): completed games missing an extended timeline row.

Supabase daily (--daily): reads All Games whose kickoff UTC date is yesterday–tomorrow
(configurable), fetches extended_timeline.json per sport_event_id, and stores only when
sport_event_status.status is closed/ended.

Respects the 1 req/sec trial-key rate limit.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request

from config import (
    API_KEY,
    BASE_URL_SOCCER_EXTENDED,
    COMPLETED_STATUSES,
    PIPELINE_EXTENDED_TIMELINE_MAX_CONSECUTIVE_404,
    PIPELINE_DAILY_TIMELINE_KICKOFF_DAYS_AFTER,
    PIPELINE_DAILY_TIMELINE_KICKOFF_DAYS_BEFORE,
    REQUEST_DELAY_SECONDS,
    USE_SUPABASE,
)


def fetch_extended_timeline(sport_event_id: str) -> dict:
    """Fetch the extended_timeline JSON for a single sport event."""
    url = (
        f"{BASE_URL_SOCCER_EXTENDED}/sport_events/{sport_event_id}/extended_timeline.json"
        f"?api_key={API_KEY}"
    )
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


def timeline_feed_marked_completed(data: dict) -> bool:
    """True when Sportradar payload shows a finished fixture (closed / ended)."""
    ses = data.get("sport_event_status") or {}
    return (ses.get("status") or "") in COMPLETED_STATUSES


def main_daily_extended_timeline_kickoff_window(
    *,
    days_before: int | None = None,
    days_after: int | None = None,
) -> int:
    """
    Daily path: kickoff-window games -> extended_timeline.json each -> upsert if completed.

    Returns the number of extended timelines newly stored.
    """
    if not USE_SUPABASE:
        raise RuntimeError("main_daily_extended_timeline_kickoff_window requires USE_SUPABASE")

    import db

    print(
        f"Using Soccer Extended endpoint base: {BASE_URL_SOCCER_EXTENDED}",
        flush=True,
    )

    b = PIPELINE_DAILY_TIMELINE_KICKOFF_DAYS_BEFORE if days_before is None else days_before
    a = PIPELINE_DAILY_TIMELINE_KICKOFF_DAYS_AFTER if days_after is None else days_after

    db.get_or_create_seasons()
    candidates = db.get_games_kickoff_utc_calendar_window_for_configured_seasons(
        days_before=b, days_after=a
    )
    game_ids = [str(r["id"]) for r in candidates if r.get("id")]
    existing_tl = db.get_game_ids_with_extended_timeline_among(game_ids)

    print(
        f"Daily extended timeline scan: {len(candidates)} game(s) with kickoff UTC date in "
        f"[today-{b}, today+{a}] inclusive ({len(existing_tl)} already have a row).",
        flush=True,
    )

    stored = 0
    skipped_have_timeline = 0
    skipped_not_finished = 0
    errors = 0
    consecutive_404 = 0

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
        print(f"[{i}/{len(candidates)}] Extended probe {date}  {home} vs {away}  ({event_id})")

        try:
            data = fetch_extended_timeline(event_id)
            consecutive_404 = 0
            if not timeline_feed_marked_completed(data):
                skipped_not_finished += 1
                continue
            db.upsert_extended_timeline(
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
            if e.code == 404:
                consecutive_404 += 1
                if consecutive_404 >= PIPELINE_EXTENDED_TIMELINE_MAX_CONSECUTIVE_404:
                    raise RuntimeError(
                        "Aborting extended timeline daily run after "
                        f"{consecutive_404} consecutive HTTP 404 responses. "
                        "Check API host/path/key entitlement for soccer-extended."
                    ) from e
            else:
                consecutive_404 = 0
        except Exception as e:
            print(f"  Error: {e} — skipping")
            errors += 1
            consecutive_404 = 0

        time.sleep(REQUEST_DELAY_SECONDS)

    print("\nDaily extended timeline window done.")
    print(f"  Newly stored (completed) : {stored}")
    print(f"  Already had row          : {skipped_have_timeline}")
    print(f"  Not finished in feed     : {skipped_not_finished}")
    print(f"  Errors                   : {errors}")
    print('  Rows in Supabase public."Completed Matches - Extended Timeline"')
    return stored


def main() -> int:
    """Full backfill: fetch extended timelines for completed matches missing storage."""
    if not USE_SUPABASE:
        raise RuntimeError("main requires USE_SUPABASE")

    import db

    print(
        f"Using Soccer Extended endpoint base: {BASE_URL_SOCCER_EXTENDED}",
        flush=True,
    )

    db.get_or_create_seasons()
    matches = db.get_completed_matches_without_extended_timeline_for_configured_seasons()
    print(f"Completed matches without extended timeline (from Supabase): {len(matches)}")

    fetched = 0
    errors = 0
    skipped_not_finished = 0
    consecutive_404 = 0

    for i, match in enumerate(matches, 1):
        event_id = match["sport_event_id"]
        game_id = match.get("id")
        if not game_id:
            continue

        home = match.get("home_team", "")
        away = match.get("away_team", "")
        start_time = match.get("start_time", "")
        date = str(start_time)[:10] if start_time else "?"
        print(f"[{i}/{len(matches)}] Fetching extended {date}  {home} vs {away}  ({event_id})")

        try:
            data = fetch_extended_timeline(event_id)
            consecutive_404 = 0
            if not timeline_feed_marked_completed(data):
                skipped_not_finished += 1
                continue
            db.upsert_extended_timeline(
                str(game_id),
                data,
                sport_event_id=event_id,
                start_time=match.get("start_time"),
            )
            fetched += 1
        except urllib.error.HTTPError as e:
            print(f"  HTTP {e.code} — skipping")
            errors += 1
            if e.code == 404:
                consecutive_404 += 1
                if consecutive_404 >= PIPELINE_EXTENDED_TIMELINE_MAX_CONSECUTIVE_404:
                    raise RuntimeError(
                        "Aborting extended timeline backfill after "
                        f"{consecutive_404} consecutive HTTP 404 responses. "
                        "Check API host/path/key entitlement for soccer-extended."
                    ) from e
            else:
                consecutive_404 = 0
        except Exception as e:
            print(f"  Error: {e} — skipping")
            errors += 1
            consecutive_404 = 0

        time.sleep(REQUEST_DELAY_SECONDS)

    print("\nDone.")
    print(f"  Newly fetched         : {fetched}")
    print(f"  Not finished in feed  : {skipped_not_finished}")
    print(f"  Errors                : {errors}")
    print('  Rows saved to Supabase public."Completed Matches - Extended Timeline"')
    return fetched


if __name__ == "__main__":
    main()
