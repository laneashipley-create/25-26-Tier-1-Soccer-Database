"""
STEP 2 — Pull configured season schedules and save every match to CSV.

Columns saved:
  sport_event_id, start_time, round, home_team, home_team_id,
  away_team, away_team_id, status, match_status, home_score, away_score

Run independently to refresh the schedule without touching timeline data.

Full weekly pulls GET …/seasons/{season_id}/schedules.json with ``start`` / ``limit`` query params
until every sport event is returned (Soccer v4; ``limit`` max 1000). Trial feeds often cap page
size smaller — ``PIPELINE_SCHEDULE_PAGE_SIZE`` (default 100) controls ``limit``.

When ``recent_kickoff_window_days`` is set (daily CI), each season schedule is still fetched in
full from Sportradar (same pagination), but only matches whose kickoff falls within ±that many
UTC calendar days from today are upserted to Supabase; ``data/schedule.csv`` is left unchanged
so the weekly full sync remains the canonical export.
"""

import csv
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone

from config import (
    API_KEY,
    BASE_URL,
    COMPETITIONS,
    PIPELINE_SCHEDULE_PAGE_SIZE,
    SCHEDULE_CSV,
    REQUEST_DELAY_SECONDS,
    USE_SUPABASE,
)

CSV_FIELDS = [
    "competition_id",
    "season_id",
    "season_name",
    "sport_event_id",
    "start_time",
    "round",
    "home_team",
    "home_team_id",
    "away_team",
    "away_team_id",
    "status",
    "match_status",
    "home_score",
    "away_score",
]


def _schedules_total_hint(headers: object) -> int | None:
    """Parse Sportradar pagination hints from response headers (names vary by edge)."""
    get = getattr(headers, "get", None)
    if not callable(get):
        return None
    for key in (
        "x-max-results",
        "X-Max-Results",
        "x-total",
        "X-Total",
        "x-total-count",
        "X-Total-Count",
    ):
        raw = get(key)
        if raw is None:
            continue
        try:
            return int(str(raw).strip())
        except ValueError:
            continue
    return None


def fetch_schedule(season_id: str) -> list[dict]:
    """Fetch a full season schedule from Sportradar, paging until every sport event is retrieved."""
    page_size = min(1000, max(1, PIPELINE_SCHEDULE_PAGE_SIZE))
    schedules: list[dict] = []
    start = 0
    total_hint: int | None = None
    base_path = f"{BASE_URL}/seasons/{season_id}/schedules.json"
    print(f"Fetching schedule from: {base_path} (paginated, limit={page_size})", flush=True)

    max_pages = 250  # safety valve (~25k rows at page_size 100)
    pages_fetched = 0
    chunk: list[dict] = []
    for _page_ix in range(max_pages):
        qs = urllib.parse.urlencode({"api_key": API_KEY, "start": start, "limit": page_size})
        url = f"{base_path}?{qs}"
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=90) as resp:
            hdrs = resp.headers
            payload = json.loads(resp.read().decode())
        chunk = payload.get("schedules") or []
        pages_fetched += 1
        if pages_fetched == 1:
            total_hint = _schedules_total_hint(hdrs)
            if total_hint is not None:
                print(f"  -> API reports ~{total_hint} sport events (response header)", flush=True)
        if not chunk:
            break
        schedules.extend(chunk)
        if len(chunk) < page_size:
            break
        if total_hint is not None and len(schedules) >= total_hint:
            break
        start += len(chunk)
        time.sleep(REQUEST_DELAY_SECONDS)

    if total_hint is not None and len(schedules) < total_hint:
        print(
            f"  WARNING: retrieved {len(schedules)} rows but header indicated {total_hint}; "
            "check PIPELINE_SCHEDULE_PAGE_SIZE / API access.",
            flush=True,
        )

    print(f"  -> {len(schedules)} sport events returned ({pages_fetched} page(s))", flush=True)
    return schedules


def parse_schedule(schedules: list[dict], competition_id: str, season_id: str, season_name: str) -> list[dict]:
    """Flatten each schedule entry into a flat row dict."""
    rows = []
    for item in schedules:
        se = item.get("sport_event", {})
        ses = item.get("sport_event_status", {})

        competitors = se.get("competitors", [])
        home = next((c for c in competitors if c.get("qualifier") == "home"), {})
        away = next((c for c in competitors if c.get("qualifier") == "away"), {})

        round_num = ""
        ctx = se.get("sport_event_context", {})
        if ctx:
            round_num = ctx.get("round", {}).get("number", "")

        rows.append({
            "competition_id": competition_id,
            "season_id": season_id,
            "season_name": season_name,
            "sport_event_id": se.get("id", ""),
            "start_time": se.get("start_time", ""),
            "round": round_num,
            "home_team": home.get("name", ""),
            "home_team_id": home.get("id", ""),
            "away_team": away.get("name", ""),
            "away_team_id": away.get("id", ""),
            "status": ses.get("status", ""),
            "match_status": ses.get("match_status", ""),
            "home_score": ses.get("home_score", ""),
            "away_score": ses.get("away_score", ""),
        })
    return rows


def kickoff_date_utc(start_time_val: object) -> date | None:
    """UTC calendar date for a Sportradar sport_event.start_time string, or None if unparsable."""
    if start_time_val is None:
        return None
    s = str(start_time_val).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        try:
            dt = datetime.fromisoformat(s.replace(" ", "T"))
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.date()


def filter_rows_kickoff_within_days(rows: list[dict], *, window_days: int, center: date | None = None) -> list[dict]:
    """Keep rows whose kickoff UTC date is in [center - window_days, center + window_days] inclusive."""
    if window_days < 0:
        raise ValueError("window_days must be non-negative")
    c = center or datetime.now(timezone.utc).date()
    lo = c - timedelta(days=window_days)
    hi = c + timedelta(days=window_days)
    out = []
    for row in rows:
        d = kickoff_date_utc(row.get("start_time"))
        if d is not None and lo <= d <= hi:
            out.append(row)
    return out


def save_csv(rows: list[dict], path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  -> Saved {len(rows)} rows to {path}")


def main(recent_kickoff_window_days: int | None = None) -> None:
    """
    Fetch each configured season schedule from Sportradar and upsert games to Supabase (if enabled).

    ``recent_kickoff_window_days``: if not None, only upsert matches whose kickoff (UTC date) is
    within ± this many days of today. Does not overwrite ``data/schedule.csv`` (weekly full run
    owns the export).
    """
    rows: list[dict] = []
    for competition in COMPETITIONS:
        competition_id = competition["competition_id"]
        season_id = competition["season_id"]
        season_name = competition["season_name"]
        print(f"\nCompetition: {competition_id} | Season: {season_name} ({season_id})")
        schedules = fetch_schedule(season_id)
        parsed = parse_schedule(schedules, competition_id, season_id, season_name)
        rows.extend(parsed)
        time.sleep(REQUEST_DELAY_SECONDS)

    utc_today = datetime.now(timezone.utc).date()
    rows_for_db = rows
    if recent_kickoff_window_days is not None:
        rows_for_db = filter_rows_kickoff_within_days(rows, window_days=recent_kickoff_window_days)
        print(
            f"\nKickoff window (UTC): ±{recent_kickoff_window_days} day(s) from {utc_today} → "
            f"{len(rows_for_db)} / {len(rows)} match(es) selected for Supabase upsert.",
            flush=True,
        )

    print(
        f"\nSchedules fetched for all competitions ({len(rows)} rows in memory). "
        "Next: Supabase games upsert (when enabled)…",
        flush=True,
    )

    if USE_SUPABASE:
        import db
        season_ids = db.get_or_create_seasons()
        by_season: dict[str, list[dict]] = {sid: [] for sid in season_ids.values()}
        for row in rows_for_db:
            sid = season_ids.get(row["season_id"])
            if sid:
                by_season[sid].append(row)
        upserted = 0
        n_seasons = sum(1 for v in by_season.values() if v)
        print(
            f"  -> Upserting games for {n_seasons} season(s) into public.\"All Games (sr:sport_events)\"…",
            flush=True,
        )
        for sid, season_rows in by_season.items():
            if not season_rows:
                continue
            db.upsert_games(sid, season_rows)
            upserted += len(season_rows)
        print(f"  -> Upserted {upserted} rows to Supabase public.\"All Games (sr:sport_events)\"", flush=True)
    elif recent_kickoff_window_days is not None:
        print("  (Supabase disabled — no DB upsert; enable USE_SUPABASE for daily window refresh.)", flush=True)

    if recent_kickoff_window_days is None:
        save_csv(rows, SCHEDULE_CSV)
    else:
        print(f"  (Skipped writing {SCHEDULE_CSV} — full weekly sync exports the complete CSV.)", flush=True)

    summary_rows = rows_for_db if recent_kickoff_window_days is not None else rows
    closed = sum(1 for r in summary_rows if r["status"] in {"closed", "ended"})
    upcoming = sum(1 for r in summary_rows if r["status"] not in {"closed", "ended"})
    print(f"\nSummary (upsert scope):")
    print(f"  Total matches : {len(summary_rows)}")
    print(f"  Completed     : {closed}")
    print(f"  Not yet played: {upcoming}")


if __name__ == "__main__":
    win: int | None = None
    if "--recent-window" in sys.argv:
        i = sys.argv.index("--recent-window")
        try:
            win = int(sys.argv[i + 1])
        except (IndexError, ValueError):
            print("Usage: python step2_get_schedule.py [--recent-window DAYS]", file=sys.stderr)
            raise SystemExit(2)
    main(recent_kickoff_window_days=win)
