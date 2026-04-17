"""
Supabase helpers for Own Goals pipeline.

Use when USE_SUPABASE is True in config. Provides:
  - get_client() — PostgREST client (avoids full supabase pkg + C++ deps)
  - get_or_create_competition() — ensure public.competitions row exists
  - get_or_create_season() / get_or_create_seasons() — ensure public.seasons rows
  - upsert_games() — upsert rows in public.games (sport events per season)
  - get_completed_matches_without_timeline* — for step 3
  - upsert_timeline() / get_timeline_json() — public.sport_event_timelines
  - upsert_own_goals() — write extracted own goals

Install: pip install postgrest httpx
Env: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_ANON_KEY)
"""

from __future__ import annotations

from config import (
    USE_SUPABASE,
    SUPABASE_URL,
    SUPABASE_KEY,
    COMPETITIONS,
    SEASON_ID,
    COMPETITION_ID,
    SEASON_NAME,
)

_client = None


def get_client():
    """Return PostgREST client for Supabase tables; raises if not configured."""
    if not USE_SUPABASE:
        raise RuntimeError("Supabase not configured. Set SUPABASE_URL and SUPABASE_KEY.")
    global _client
    if _client is None:
        from postgrest import SyncPostgrestClient
        base = SUPABASE_URL.rstrip("/")
        if not base.endswith("/rest/v1"):
            base = f"{base}/rest/v1"
        _client = SyncPostgrestClient(
            base,
            schema="public",
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Content-Type": "application/json",
            },
        )
    return _client


def get_or_create_competition(sportradar_competition_id: str, display_name: str | None = None) -> str:
    """Ensure public.competitions row exists; return its id (uuid)."""
    supabase = get_client()
    r = supabase.table("competitions").select("id").eq("sportradar_competition_id", sportradar_competition_id).execute()
    if r.data and len(r.data) > 0:
        return r.data[0]["id"]
    label = display_name or sportradar_competition_id
    ins = supabase.table("competitions").insert({
        "sportradar_competition_id": sportradar_competition_id,
        "name": label,
    }).execute()
    return ins.data[0]["id"]


def get_or_create_season():
    """Ensure the current season exists in public.seasons; return its id (uuid)."""
    supabase = get_client()
    r = supabase.table("seasons").select("id").eq("sportradar_season_id", SEASON_ID).execute()
    if r.data and len(r.data) > 0:
        return r.data[0]["id"]
    comp_pk = get_or_create_competition(
        COMPETITION_ID,
        COMPETITIONS[0].get("competition_name"),
    )
    ins = supabase.table("seasons").insert({
        "sportradar_season_id": SEASON_ID,
        "competition_id": comp_pk,
        "name": SEASON_NAME,
    }).execute()
    return ins.data[0]["id"]


def get_or_create_season_entry(entry: dict) -> str:
    """Ensure a season exists for a COMPETITIONS list entry; return season UUID."""
    season_id = entry["season_id"]
    season_name = entry["season_name"]
    sportradar_cid = entry["competition_id"]
    supabase = get_client()
    r = supabase.table("seasons").select("id").eq("sportradar_season_id", season_id).execute()
    if r.data and len(r.data) > 0:
        return r.data[0]["id"]
    comp_pk = get_or_create_competition(sportradar_cid, entry.get("competition_name"))
    ins = supabase.table("seasons").insert({
        "sportradar_season_id": season_id,
        "competition_id": comp_pk,
        "name": season_name,
    }).execute()
    return ins.data[0]["id"]


def get_or_create_seasons() -> dict[str, str]:
    """
    Ensure all configured seasons exist.
    Returns mapping: sportradar season_id -> seasons.id UUID
    """
    mapping: dict[str, str] = {}
    for c in COMPETITIONS:
        sid = c["season_id"]
        mapping[sid] = get_or_create_season_entry(c)
    return mapping


def upsert_games(season_id: str, rows: list[dict]) -> None:
    """Upsert public.games rows for the given season_id. Each row must include sport_event_id and match fields."""
    if not rows:
        return
    supabase = get_client()
    for row in rows:
        payload = {
            "season_id": season_id,
            "sport_event_id": row["sport_event_id"],
            "start_time": row.get("start_time") or None,
            "round": row.get("round") or None,
            "home_team": row.get("home_team") or None,
            "home_team_id": row.get("home_team_id") or None,
            "away_team": row.get("away_team") or None,
            "away_team_id": row.get("away_team_id") or None,
            "status": row.get("status") or None,
            "match_status": row.get("match_status") or None,
            "home_score": _int_or_none(row.get("home_score")),
            "away_score": _int_or_none(row.get("away_score")),
        }
        supabase.table("games").upsert(
            payload,
            on_conflict="season_id,sport_event_id",
            ignore_duplicates=False,
        ).execute()


def get_completed_games_for_season(season_id: str) -> list[dict]:
    """Return games rows that are completed (status in closed/ended)."""
    supabase = get_client()
    r = supabase.table("games").select("*").eq("season_id", season_id).in_("status", ["closed", "ended"]).execute()
    return r.data or []


def get_game_ids_with_timeline(season_id: str) -> set[str]:
    """Return set of games.id (uuid) that already have a sport_event_timelines row."""
    supabase = get_client()
    sched = supabase.table("games").select("id").eq("season_id", season_id).execute()
    sched_ids = [x["id"] for x in (sched.data or [])]
    if not sched_ids:
        return set()
    tl = supabase.table("sport_event_timelines").select("game_id").in_("game_id", sched_ids).execute()
    return {x["game_id"] for x in (tl.data or [])}


def get_completed_matches_without_timeline(season_id: str) -> list[dict]:
    """Return completed games rows that do not yet have a timeline. Use this in step 3 to only fetch missing."""
    completed = get_completed_games_for_season(season_id)
    with_timeline = get_game_ids_with_timeline(season_id)
    return [r for r in completed if r["id"] not in with_timeline]


def get_completed_matches_without_timeline_for_configured_seasons() -> list[dict]:
    """Return completed games rows without timelines across all configured seasons."""
    season_ids = get_or_create_seasons()
    out = []
    for season_uuid in season_ids.values():
        out.extend(get_completed_matches_without_timeline(season_uuid))
    return out


def upsert_timeline(game_id: str, timeline_json: dict | None = None) -> None:
    """Record that we have fetched the timeline for this game row. Optionally store timeline_json."""
    supabase = get_client()
    supabase.table("sport_event_timelines").upsert({
        "game_id": game_id,
        "timeline_json": timeline_json,
    }, on_conflict="game_id", ignore_duplicates=False).execute()


def get_timeline_json(game_id: str) -> dict | None:
    """Return stored timeline JSON for a game row, or None if not stored."""
    supabase = get_client()
    r = supabase.table("sport_event_timelines").select("timeline_json").eq("game_id", game_id).execute()
    if r.data and len(r.data) > 0 and r.data[0].get("timeline_json"):
        return r.data[0]["timeline_json"]
    return None


def get_completed_matches_with_timelines(season_id: str) -> list[dict]:
    """Return completed games rows that have timelines, with timeline_json attached. For step4."""
    completed = get_completed_games_for_season(season_id)
    with_tl_ids = get_game_ids_with_timeline(season_id)
    result = []
    for row in completed:
        if row["id"] not in with_tl_ids:
            continue
        tl = get_timeline_json(row["id"])
        if tl:
            row_copy = dict(row)
            row_copy["timeline_json"] = tl
            result.append(row_copy)
    return result


def get_completed_matches_with_timelines_for_configured_seasons() -> list[dict]:
    """Return completed games rows with timelines across all configured seasons."""
    season_ids = get_or_create_seasons()
    meta_by_uuid = {}
    for c in COMPETITIONS:
        season_uuid = season_ids.get(c["season_id"])
        if season_uuid:
            meta_by_uuid[season_uuid] = {
                "competition_id": c["competition_id"],
                "season_id": c["season_id"],
                "season_name": c["season_name"],
            }
    out = []
    for season_uuid in season_ids.values():
        rows = get_completed_matches_with_timelines(season_uuid)
        meta = meta_by_uuid.get(season_uuid, {})
        for row in rows:
            if meta:
                row["competition_id"] = meta["competition_id"]
                row["season_id"] = meta["season_id"]
                row["season_name"] = meta["season_name"]
        out.extend(rows)
    return out


def get_all_own_goals() -> list[dict]:
    """Return all own_goals rows for the report. Keys match CSV/legacy format."""
    supabase = get_client()
    r = supabase.table("own_goals").select("*").order("match_date").order("minute").execute()
    rows = r.data or []
    event_ids = [row.get("sport_event_id") for row in rows if row.get("sport_event_id")]
    season_meta_by_event: dict[str, dict] = {}
    if event_ids:
        season_rows: list[dict] = []
        off = 0
        page = 500
        while True:
            chunk = (
                supabase.table("seasons")
                .select("id,sportradar_season_id,name,competition_id")
                .range(off, off + page - 1)
                .execute()
            )
            part = chunk.data or []
            season_rows.extend(part)
            if len(part) < page:
                break
            off += page
        season_by_uuid = {s["id"]: s for s in season_rows if s.get("id")}
        comp_uuids = list({s.get("competition_id") for s in season_rows if s.get("competition_id")})
        comp_by_uuid: dict[str, dict] = {}
        csize = 200
        for i in range(0, len(comp_uuids), csize):
            cu = comp_uuids[i : i + csize]
            cr = supabase.table("competitions").select("id,sportradar_competition_id,name").in_("id", cu).execute()
            for crow in cr.data or []:
                cid = crow.get("id")
                if cid:
                    comp_by_uuid[str(cid)] = crow
        chunk_size = 200
        for i in range(0, len(event_ids), chunk_size):
            chunk = event_ids[i : i + chunk_size]
            sched = supabase.table("games").select("sport_event_id,season_id").in_("sport_event_id", chunk).execute()
            for srow in sched.data or []:
                event_id = srow.get("sport_event_id")
                season_uuid = srow.get("season_id")
                season = season_by_uuid.get(season_uuid, {})
                comp = comp_by_uuid.get(str(season.get("competition_id", "")), {})
                if event_id:
                    season_meta_by_event[event_id] = {
                        "competition_id": comp.get("sportradar_competition_id", ""),
                        "season_id": season.get("sportradar_season_id", ""),
                        "season_name": season.get("name", ""),
                    }
    out = []
    for row in rows:
        meta = season_meta_by_event.get(row.get("sport_event_id", ""), {})
        out.append({
            "competition_id": str(meta.get("competition_id", "")),
            "season_id": str(meta.get("season_id", "")),
            "season_name": str(meta.get("season_name", "")),
            "sport_event_id": row.get("sport_event_id", ""),
            "match_date": str(row.get("match_date") or "")[:10],
            "round": str(row.get("round") or ""),
            "home_team": str(row.get("home_team") or ""),
            "away_team": str(row.get("away_team") or ""),
            "og_player": str(row.get("og_player") or ""),
            "og_player_id": str(row.get("og_player_id") or ""),
            "og_player_team": str(row.get("og_player_team") or ""),
            "benefiting_team": str(row.get("benefiting_team") or ""),
            "minute": str(row.get("minute") or ""),
            "stoppage_time": str(row.get("stoppage_time") or ""),
            "home_score_after": row.get("home_score_after") if row.get("home_score_after") is not None else "",
            "away_score_after": row.get("away_score_after") if row.get("away_score_after") is not None else "",
            "final_home_score": row.get("final_home_score") if row.get("final_home_score") is not None else "",
            "final_away_score": row.get("final_away_score") if row.get("final_away_score") is not None else "",
            "commentary": str(row.get("commentary") or ""),
        })
    return out


def get_report_stats() -> tuple[int, int]:
    """Return (completed_matches, timeline_events) for the report."""
    supabase = get_client()
    page_size = 1000
    offset = 0
    completed_matches = 0
    timeline_events = 0
    while True:
        tl = (
            supabase.table("sport_event_timelines")
            .select("timeline_json")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        rows = tl.data or []
        if not rows:
            break
        completed_matches += len(rows)
        for row in rows:
            data = row.get("timeline_json") or {}
            timeline_events += len(data.get("timeline", []))
        if len(rows) < page_size:
            break
        offset += page_size
    return completed_matches, timeline_events


def get_game_by_sport_event_id(season_id: str, sport_event_id: str) -> dict | None:
    """Return games row by sport_event_id, or None."""
    supabase = get_client()
    r = supabase.table("games").select("*").eq("season_id", season_id).eq("sport_event_id", sport_event_id).execute()
    if r.data and len(r.data) > 0:
        return r.data[0]
    return None


def clear_own_goals() -> None:
    """Delete all own_goals. Call before re-inserting to avoid duplicates on pipeline re-run."""
    supabase = get_client()
    r = supabase.table("own_goals").select("id").execute()
    ids = [x["id"] for x in (r.data or [])]
    if ids:
        supabase.table("own_goals").delete().in_("id", ids).execute()


def upsert_own_goals(rows: list[dict], replace: bool = True) -> None:
    """Insert own_goals rows. If replace=True, clears existing rows first to avoid duplicates on re-run."""
    supabase = get_client()
    if replace:
        clear_own_goals()
    if not rows:
        return
    for row in rows:
        payload = {
            "sport_event_id": row.get("sport_event_id", ""),
            "match_date": row.get("match_date"),
            "round": row.get("round"),
            "home_team": row.get("home_team"),
            "away_team": row.get("away_team"),
            "og_player": row.get("og_player"),
            "og_player_id": row.get("og_player_id"),
            "og_player_team": row.get("og_player_team"),
            "benefiting_team": row.get("benefiting_team"),
            "minute": row.get("minute"),
            "stoppage_time": row.get("stoppage_time"),
            "home_score_after": _int_or_none(row.get("home_score_after")),
            "away_score_after": _int_or_none(row.get("away_score_after")),
            "final_home_score": _int_or_none(row.get("final_home_score")),
            "final_away_score": _int_or_none(row.get("final_away_score")),
            "commentary": row.get("commentary"),
        }
        supabase.table("own_goals").insert(payload).execute()


def _int_or_none(v):
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None
