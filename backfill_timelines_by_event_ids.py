"""
One-off: fetch Sportradar summary + timeline for specific sport_event IDs,
ensure public."Competitions" / "Seasons (current sr:season:ID)" / "All Games (sr:sport_events)" rows exist, then upsert timelines.

Uses the same API + Supabase env as the rest of the project (config, db).
"""

from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.request

from config import API_KEY, BASE_URL, REQUEST_DELAY_SECONDS, USE_SUPABASE
from db import T_COMPETITIONS, T_GAMES, T_SEASONS

MISSING_EVENT_IDS = [
    "sr:sport_event:68305676",
    "sr:sport_event:61491157",
    "sr:sport_event:61491153",
    "sr:sport_event:61491145",
    "sr:sport_event:61491147",
    "sr:sport_event:61491151",
    "sr:sport_event:61491161",
    "sr:sport_event:61491165",
    "sr:sport_event:61491149",
    "sr:sport_event:61491155",
    "sr:sport_event:61491159",
    "sr:sport_event:61491195",
    "sr:sport_event:61491205",
    "sr:sport_event:61491199",
    "sr:sport_event:61491209",
    "sr:sport_event:61491213",
]


def fetch_summary(sport_event_id: str) -> dict:
    url = f"{BASE_URL}/sport_events/{sport_event_id}/summary.json?api_key={API_KEY}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


def fetch_timeline(sport_event_id: str) -> dict:
    url = f"{BASE_URL}/sport_events/{sport_event_id}/timeline.json?api_key={API_KEY}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


def summary_to_game_row(summary: dict) -> dict:
    se = summary["sport_event"]
    ses = summary.get("sport_event_status") or {}
    competitors = se.get("competitors") or []
    home = next((c for c in competitors if c.get("qualifier") == "home"), {})
    away = next((c for c in competitors if c.get("qualifier") == "away"), {})
    ctx = se.get("sport_event_context") or {}
    rnd = ctx.get("round") or {}
    round_num = rnd.get("number", "")
    return {
        "sport_event_id": se.get("id", ""),
        "start_time": se.get("start_time") or None,
        "round": str(round_num) if round_num != "" else None,
        "home_team": home.get("name", ""),
        "home_team_id": home.get("id", ""),
        "away_team": away.get("name", ""),
        "away_team_id": away.get("id", ""),
        "status": ses.get("status", ""),
        "match_status": ses.get("match_status", ""),
        "home_score": ses.get("home_score", ""),
        "away_score": ses.get("away_score", ""),
    }


def _api_gender_to_db(g: str | None) -> str | None:
    if not g:
        return None
    g = str(g).lower()
    if g in ("men", "male"):
        return "male"
    if g in ("women", "female"):
        return "female"
    return None


def ensure_competition_and_season(supabase, summary: dict) -> str:
    se = summary["sport_event"]
    ctx = se.get("sport_event_context") or {}
    comp = ctx.get("competition") or {}
    season = ctx.get("season") or {}
    category = ctx.get("category") or {}

    comp_id = comp.get("id")
    comp_name = comp.get("name") or comp_id
    season_id = season.get("id")
    season_name = season.get("name") or season_id
    if not comp_id or not season_id:
        raise ValueError("summary missing competition.id or season.id")

    payload: dict = {
        "sportradar_competition_id": comp_id,
        "competition_name": comp_name,
    }
    gg = _api_gender_to_db(comp.get("gender"))
    if gg:
        payload["gender"] = gg
    cat_name = category.get("name")
    if isinstance(cat_name, str) and cat_name.strip():
        payload["category_name"] = cat_name.strip()
    cc = category.get("country_code")
    if isinstance(cc, str) and cc.strip():
        payload["country_code"] = cc.strip()

    supabase.table(T_COMPETITIONS).upsert(
        payload,
        on_conflict="sportradar_competition_id",
        ignore_duplicates=False,
    ).execute()
    r = supabase.table(T_COMPETITIONS).select("id").eq("sportradar_competition_id", comp_id).execute()
    comp_uuid = r.data[0]["id"]

    r2 = supabase.table(T_SEASONS).select("id").eq("sportradar_season_id", season_id).execute()
    if r2.data:
        return r2.data[0]["id"]
    ins = supabase.table(T_SEASONS).insert({
        "sportradar_season_id": season_id,
        "competition_id": comp_uuid,
        "name": season_name,
    }).execute()
    return ins.data[0]["id"]


def main() -> int:
    if not API_KEY:
        print("SPORTRADAR_API_KEY / config_local.API_KEY required.", file=sys.stderr)
        return 1
    if not USE_SUPABASE:
        print("Supabase not configured (SUPABASE_URL + SUPABASE_KEY).", file=sys.stderr)
        return 1

    import db

    supabase = db.get_client()
    ok = 0
    err = 0

    for i, eid in enumerate(MISSING_EVENT_IDS, 1):
        print(f"[{i}/{len(MISSING_EVENT_IDS)}] {eid}")
        try:
            summary = fetch_summary(eid)
            time.sleep(REQUEST_DELAY_SECONDS)
            season_uuid = ensure_competition_and_season(supabase, summary)
            game_row = summary_to_game_row(summary)
            db.upsert_games(season_uuid, [game_row])
            r = supabase.table(T_GAMES).select("id").eq("season_id", season_uuid).eq("sport_event_id", eid).execute()
            if not r.data:
                raise RuntimeError("games upsert did not produce a row")
            game_id = r.data[0]["id"]
            tl = fetch_timeline(eid)
            time.sleep(REQUEST_DELAY_SECONDS)
            db.upsert_timeline(
                str(game_id),
                tl,
                sport_event_id=eid,
                start_time=game_row.get("start_time"),
            )
            print(f"  -> OK ({game_row.get('home_team')} vs {game_row.get('away_team')})")
            ok += 1
        except urllib.error.HTTPError as e:
            print(f"  HTTP {e.code}: {e.reason}", file=sys.stderr)
            err += 1
        except Exception as e:
            print(f"  Error: {e}", file=sys.stderr)
            err += 1

    print(f"\nDone. OK={ok} errors={err}")
    return 0 if err == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
