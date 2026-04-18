"""
STEP 2 — Pull configured season schedules and save every match to CSV.

Columns saved:
  sport_event_id, start_time, round, home_team, home_team_id,
  away_team, away_team_id, status, match_status, home_score, away_score

Run independently to refresh the schedule without touching timeline data.
"""

import csv
import json
import os
import time
import urllib.request
import urllib.error

from config import API_KEY, BASE_URL, COMPETITIONS, SCHEDULE_CSV, REQUEST_DELAY_SECONDS, USE_SUPABASE

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


def fetch_schedule(season_id: str) -> list[dict]:
    """Fetch a full season schedule from Sportradar."""
    url = f"{BASE_URL}/seasons/{season_id}/schedules.json?api_key={API_KEY}"
    print(f"Fetching schedule from: {url.split('?')[0]}")
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode())
    schedules = data.get("schedules", [])
    print(f"  -> {len(schedules)} sport events returned")
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


def save_csv(rows: list[dict], path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  -> Saved {len(rows)} rows to {path}")


def main():
    rows = []
    for competition in COMPETITIONS:
        competition_id = competition["competition_id"]
        season_id = competition["season_id"]
        season_name = competition["season_name"]
        print(f"\nCompetition: {competition_id} | Season: {season_name} ({season_id})")
        schedules = fetch_schedule(season_id)
        parsed = parse_schedule(schedules, competition_id, season_id, season_name)
        rows.extend(parsed)
        time.sleep(REQUEST_DELAY_SECONDS)

    print(
        f"\nSchedules fetched for all competitions ({len(rows)} rows in memory). "
        "Next: Supabase games upsert (can take several minutes) and CSV export…",
        flush=True,
    )

    if USE_SUPABASE:
        import db
        season_ids = db.get_or_create_seasons()
        by_season: dict[str, list[dict]] = {sid: [] for sid in season_ids.values()}
        for row in rows:
            sid = season_ids.get(row["season_id"])
            if sid:
                by_season[sid].append(row)
        upserted = 0
        n_seasons = sum(1 for v in by_season.values() if v)
        print(f"  -> Upserting games for {n_seasons} season(s) into public.games…", flush=True)
        for sid, season_rows in by_season.items():
            if not season_rows:
                continue
            db.upsert_games(sid, season_rows)
            upserted += len(season_rows)
        print(f"  -> Upserted {upserted} rows to Supabase public.games", flush=True)

    save_csv(rows, SCHEDULE_CSV)

    closed = sum(1 for r in rows if r["status"] in {"closed", "ended"})
    upcoming = sum(1 for r in rows if r["status"] not in {"closed", "ended"})
    print(f"\nSummary:")
    print(f"  Total matches : {len(rows)}")
    print(f"  Completed     : {closed}")
    print(f"  Not yet played: {upcoming}")


if __name__ == "__main__":
    main()
