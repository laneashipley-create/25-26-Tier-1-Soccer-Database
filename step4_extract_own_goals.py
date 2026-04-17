"""
STEP 4 — Scan cached timelines and extract own goal events.

Own goals are identified by:
    timeline event: type == "score_change"  AND  method == "own_goal"

For each own goal, we extract:
    match_date, home_team, away_team, round,
    og_player, og_player_team (the team the scorer plays for — i.e. the LOSING team),
    benefiting_competitor (home/away),
    minute, stoppage_time,
    home_score_after, away_score_after,
    final_home_score, final_away_score,
    commentary, sport_event_id

Output: data/own_goals.csv
"""

import csv
import json
import os

from config import SCHEDULE_CSV, TIMELINES_DIR, OWN_GOALS_CSV, USE_SUPABASE

OG_FIELDS = [
    "competition_id",
    "season_id",
    "season_name",
    "sport_event_id",
    "match_date",
    "round",
    "home_team",
    "away_team",
    "og_player",
    "og_player_id",
    "og_player_team",
    "benefiting_team",
    "minute",
    "stoppage_time",
    "home_score_after",
    "away_score_after",
    "final_home_score",
    "final_away_score",
    "commentary",
]


def load_schedule_lookup(csv_path: str) -> dict[str, dict]:
    """Build a dict keyed by sport_event_id for quick lookups."""
    lookup = {}
    if not os.path.exists(csv_path):
        return lookup
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            lookup[row["sport_event_id"]] = row
    return lookup


def cache_path(sport_event_id: str) -> str:
    safe_id = sport_event_id.replace(":", "_")
    return os.path.join(TIMELINES_DIR, f"{safe_id}.json")


def _schedule_row_for_extract(row: dict) -> dict:
    """Normalize schedule row (from DB or CSV) for extract_own_goals_from_timeline."""
    start_time = row.get("start_time", "")
    return {
        "competition_id": row.get("competition_id", ""),
        "season_id": row.get("season_id", ""),
        "season_name": row.get("season_name", ""),
        "sport_event_id": row.get("sport_event_id", ""),
        "start_time": str(start_time) if start_time else "",
        "home_team": row.get("home_team", ""),
        "away_team": row.get("away_team", ""),
        "home_team_id": row.get("home_team_id", ""),
        "away_team_id": row.get("away_team_id", ""),
        "round": row.get("round", ""),
    }


def extract_own_goals_from_timeline(data: dict, schedule_row: dict) -> list[dict]:
    """Return a list of own goal rows from a single timeline response."""
    timeline = data.get("timeline", [])
    og_events = [
        ev for ev in timeline
        if ev.get("type") == "score_change" and ev.get("method") == "own_goal"
    ]
    if not og_events:
        return []

    final_status = data.get("sport_event_status", {})
    final_home = final_status.get("home_score", "")
    final_away = final_status.get("away_score", "")

    event_id = schedule_row.get("sport_event_id", "")
    competition_id = schedule_row.get("competition_id", "")
    season_id = schedule_row.get("season_id", "")
    season_name = schedule_row.get("season_name", "")
    home_team = schedule_row.get("home_team", "")
    home_team_id = schedule_row.get("home_team_id", "")
    away_team = schedule_row.get("away_team", "")
    away_team_id = schedule_row.get("away_team_id", "")
    round_num = schedule_row.get("round", "")
    match_date = str(schedule_row.get("start_time", ""))[:10]

    rows = []
    for ev in og_events:
        players = ev.get("players", [])
        scorer = next((p for p in players if p.get("type") == "scorer"), {})

        # The "competitor" field = the team that BENEFITS from the own goal
        benefiting_competitor = ev.get("competitor", "")
        if benefiting_competitor == "home":
            benefiting_team = home_team
            # The own-goaler plays for the AWAY team
            og_player_team = away_team
        else:
            benefiting_team = away_team
            og_player_team = home_team

        commentaries = ev.get("commentaries", [])
        commentary = commentaries[0].get("text", "") if commentaries else ""

        minute = ev.get("match_time", "")
        stoppage = ev.get("stoppage_time", "")

        rows.append({
            "competition_id": competition_id,
            "season_id": season_id,
            "season_name": season_name,
            "sport_event_id": event_id,
            "match_date": match_date,
            "round": round_num,
            "home_team": home_team,
            "away_team": away_team,
            "og_player": scorer.get("name", "Unknown"),
            "og_player_id": scorer.get("id", ""),
            "og_player_team": og_player_team,
            "benefiting_team": benefiting_team,
            "minute": minute,
            "stoppage_time": stoppage,
            "home_score_after": ev.get("home_score", ""),
            "away_score_after": ev.get("away_score", ""),
            "final_home_score": final_home,
            "final_away_score": final_away,
            "commentary": commentary,
        })
    return rows


def main():
    os.makedirs(os.path.dirname(OWN_GOALS_CSV), exist_ok=True)

    all_own_goals = []
    matches_with_og = 0

    if USE_SUPABASE:
        import db
        db.get_or_create_seasons()
        matches_with_tl = db.get_completed_matches_with_timelines_for_configured_seasons()
        print(f"Scanning {len(matches_with_tl)} timelines from Supabase...")

        for row in matches_with_tl:
            data = row.get("timeline_json") or {}
            sched_row = _schedule_row_for_extract(row)
            ogs = extract_own_goals_from_timeline(data, sched_row)
            if ogs:
                matches_with_og += 1
                all_own_goals.extend(ogs)
                home = sched_row["home_team"]
                away = sched_row["away_team"]
                date = sched_row["start_time"][:10]
                print(f"  Found {len(ogs)} OG(s) in {date}  {home} vs {away}")

        all_own_goals.sort(key=lambda r: (r["match_date"], r["minute"] if r["minute"] != "" else 0))

        if all_own_goals:
            db.upsert_own_goals(all_own_goals, replace=True)
        with open(OWN_GOALS_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=OG_FIELDS)
            writer.writeheader()
            writer.writerows(all_own_goals)
        print(f"\nDone.")
        print(f"  Matches with own goals : {matches_with_og}")
        print(f"  Total own goals found  : {len(all_own_goals)}")
        print(f"  Saved to Supabase + {OWN_GOALS_CSV}")

    else:
        schedule = load_schedule_lookup(SCHEDULE_CSV)
        if not schedule:
            raise FileNotFoundError(
                f"{SCHEDULE_CSV} not found — run step2_get_schedule.py first"
            )

        timeline_files = [f for f in os.listdir(TIMELINES_DIR) if f.endswith(".json")]
        print(f"Scanning {len(timeline_files)} cached timelines...")

        for filename in sorted(timeline_files):
            sport_event_id = filename.replace("sr_sport_event_", "sr:sport_event:").replace(".json", "")

            row = schedule.get(sport_event_id)
            if not row:
                continue

            filepath = os.path.join(TIMELINES_DIR, filename)
            with open(filepath, encoding="utf-8") as f:
                data = json.load(f)

            ogs = extract_own_goals_from_timeline(data, row)
            if ogs:
                matches_with_og += 1
                all_own_goals.extend(ogs)
                home = row["home_team"]
                away = row["away_team"]
                print(f"  Found {len(ogs)} OG(s) in {row['start_time'][:10]}  {home} vs {away}")

        all_own_goals.sort(key=lambda r: (r["match_date"], r["minute"] if r["minute"] != "" else 0))

        with open(OWN_GOALS_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=OG_FIELDS)
            writer.writeheader()
            writer.writerows(all_own_goals)

        print(f"\nDone.")
        print(f"  Matches with own goals : {matches_with_og}")
        print(f"  Total own goals found  : {len(all_own_goals)}")
        print(f"  Saved to               : {OWN_GOALS_CSV}")


if __name__ == "__main__":
    main()
