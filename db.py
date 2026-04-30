"""
Supabase helpers for Own Goals pipeline.

Use when USE_SUPABASE is True in config. Provides:
  - get_client() — PostgREST client (avoids full supabase pkg + C++ deps)
  - sync_competitions_from_config() — upsert public."Competitions" from config; category_name from Sportradar Competition Info when API key set
  - get_or_create_competition() — ensure public."Competitions" row exists
  - get_or_create_season() / get_or_create_seasons() — ensure public."Seasons (current sr:season:ID)" rows
  - upsert_games() — upsert rows in public."All Games (sr:sport_events)" (sport events per season)
  - get_completed_matches_without_timeline* — for step 3
  - upsert_timeline() / get_timeline_json() — public."Completed Matches - full sport_event_timelines"
  - upsert_own_goals() — write extracted own goals

Install: pip install postgrest httpx
Env: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_ANON_KEY)
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request

try:
    from postgrest.exceptions import APIError as PostgrestAPIError
except ImportError:  # pragma: no cover

    class PostgrestAPIError(Exception):
        """Placeholder if postgrest is unavailable."""

        pass

from config import (
    API_KEY,
    BASE_URL,
    USE_SUPABASE,
    SUPABASE_URL,
    SUPABASE_KEY,
    COMPETITIONS,
    SEASON_ID,
    COMPETITION_ID,
    SEASON_NAME,
)

# Public schema PostgREST table names (must match Postgres identifiers / Supabase renames).
T_COMPETITIONS = "Competitions"
T_SEASONS = "Seasons (current sr:season:ID)"
T_GAMES = "All Games (sr:sport_events)"
T_TIMELINES = "Completed Matches - full sport_event_timelines"
RECORDINGS_EXPORT_JSON = "soccer record replay list of sr sport event ids.json"

_client = None
_recording_id_by_event_cache: dict[str, str] | None = None


def _load_recording_id_by_event() -> dict[str, str]:
    """Map sport_event_id -> recording UUID from local export JSON."""
    global _recording_id_by_event_cache
    if _recording_id_by_event_cache is not None:
        return _recording_id_by_event_cache

    out: dict[str, str] = {}
    path = os.path.join(os.path.dirname(__file__), RECORDINGS_EXPORT_JSON)
    if not os.path.exists(path):
        _recording_id_by_event_cache = out
        return out
    try:
        with open(path, encoding="utf-8") as f:
            doc = json.load(f)
    except Exception:
        _recording_id_by_event_cache = out
        return out

    recs = (((doc or {}).get("data") or {}).get("recordingsBySport") or [])
    for rec in recs:
        if not isinstance(rec, dict):
            continue
        rid = rec.get("id")
        meta = rec.get("meta") or {}
        game_id = meta.get("gameId") if isinstance(meta, dict) else None
        if rid and game_id:
            out[str(game_id)] = str(rid)
    _recording_id_by_event_cache = out
    return out


def _recording_id_for_event(sport_event_id: object, recorded: object) -> str:
    """Return recording UUID only for recorded=true rows; else blank."""
    if recorded is not True:
        return ""
    if not sport_event_id:
        return ""
    return _load_recording_id_by_event().get(str(sport_event_id), "")


def _supabase_execute_with_retry(execute_fn, *, attempts: int = 6, base_delay: float = 1.5):
    """
    Run a PostgREST .execute() callable; retry on transient gateway / rate errors.
    Cloudflare sometimes returns HTML 502 — treated as retryable.
    """
    delay = base_delay
    last_exc: BaseException | None = None
    for i in range(attempts):
        try:
            return execute_fn()
        except PostgrestAPIError as e:
            last_exc = e
            info = e.args[0] if e.args and isinstance(e.args[0], dict) else {}
            code = info.get("code")
            if code not in (429, 502, 503, 504, 520):
                raise
        if i < attempts - 1:
            time.sleep(delay)
            delay = min(delay * 2, 45.0)
    assert last_exc is not None
    raise last_exc


def fetch_sportradar_category_name(sportradar_competition_id: str) -> str | None:
    """
    Read competition.category.name from Sportradar Soccer v4 Competition Info (JSON).
    Same value as the XML <category name="..."/> on the competition element.
    """
    if not API_KEY or not sportradar_competition_id:
        return None
    url = f"{BASE_URL}/competitions/{sportradar_competition_id}/info.json?api_key={API_KEY}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=25) as resp:
            data = json.loads(resp.read().decode())
    except (
        urllib.error.URLError,
        OSError,
        UnicodeDecodeError,
        json.JSONDecodeError,
        ValueError,
    ):
        return None
    comp = data.get("competition") or {}
    cat = comp.get("category") or {}
    name = cat.get("name")
    if isinstance(name, str) and name.strip():
        return name.strip()
    return None


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


def sync_competitions_from_config() -> None:
    """
    Upsert public."Competitions" from config.COMPETITIONS (adds new leagues, refreshes metadata).

    Call before resolving seasons so rows always match config when you add/remove/edit entries.
    category_name is set from Sportradar Competition Info (category.name) when the API key is
    configured; otherwise it falls back to the optional category_name on each config entry.
    Optional keys gender, country_code: included from config when present.
    """
    if not USE_SUPABASE:
        return
    supabase = get_client()
    for row in COMPETITIONS:
        payload: dict = {
            "sportradar_competition_id": row["competition_id"],
            "competition_name": row.get("competition_name") or row["competition_id"],
        }
        for key in ("gender", "country_code"):
            if key in row:
                payload[key] = row[key]
        api_cat = fetch_sportradar_category_name(row["competition_id"])
        if api_cat:
            payload["category_name"] = api_cat
        elif "category_name" in row:
            payload["category_name"] = row["category_name"]
        supabase.table(T_COMPETITIONS).upsert(
            payload,
            on_conflict="sportradar_competition_id",
            ignore_duplicates=False,
        ).execute()


def get_or_create_competition(competition_row: dict) -> str:
    """
    Ensure public."Competitions" row exists; return its id (uuid).

    competition_row uses config.COMPETITIONS shape:
      competition_id, competition_name, optional gender, category_name, country_code
    """
    sportradar_competition_id = competition_row["competition_id"]
    supabase = get_client()
    r = supabase.table(T_COMPETITIONS).select("id").eq("sportradar_competition_id", sportradar_competition_id).execute()
    if r.data and len(r.data) > 0:
        return r.data[0]["id"]
    label = competition_row.get("competition_name") or sportradar_competition_id
    payload: dict = {
        "sportradar_competition_id": sportradar_competition_id,
        "competition_name": label,
    }
    for key in ("gender", "country_code"):
        val = competition_row.get(key)
        if val is not None and val != "":
            payload[key] = val
    api_cat = fetch_sportradar_category_name(sportradar_competition_id)
    if api_cat:
        payload["category_name"] = api_cat
    else:
        val = competition_row.get("category_name")
        if val is not None and val != "":
            payload["category_name"] = val
    ins = supabase.table(T_COMPETITIONS).insert(payload).execute()
    return ins.data[0]["id"]


def get_or_create_season():
    """Ensure the current season exists in public."Seasons (current sr:season:ID)"; return its id (uuid)."""
    sync_competitions_from_config()
    supabase = get_client()
    r = supabase.table(T_SEASONS).select("id").eq("sportradar_season_id", SEASON_ID).execute()
    if r.data and len(r.data) > 0:
        return r.data[0]["id"]
    comp_pk = get_or_create_competition(COMPETITIONS[0])
    ins = supabase.table(T_SEASONS).insert({
        "sportradar_season_id": SEASON_ID,
        "competition_id": comp_pk,
        "name": SEASON_NAME,
    }).execute()
    return ins.data[0]["id"]


def get_or_create_season_entry(entry: dict) -> str:
    """Ensure a season exists for a COMPETITIONS list entry; return season UUID."""
    season_id = entry["season_id"]
    season_name = entry["season_name"]
    supabase = get_client()
    r = supabase.table(T_SEASONS).select("id").eq("sportradar_season_id", season_id).execute()
    if r.data and len(r.data) > 0:
        return r.data[0]["id"]
    comp_pk = get_or_create_competition(entry)
    ins = supabase.table(T_SEASONS).insert({
        "sportradar_season_id": season_id,
        "competition_id": comp_pk,
        "name": season_name,
    }).execute()
    return ins.data[0]["id"]


def get_or_create_seasons() -> dict[str, str]:
    """
    Ensure all configured seasons exist.
    Returns mapping: sportradar season_id -> seasons UUID primary key
    """
    sync_competitions_from_config()
    mapping: dict[str, str] = {}
    for c in COMPETITIONS:
        sid = c["season_id"]
        mapping[sid] = get_or_create_season_entry(c)
    return mapping


def upsert_games(season_id: str, rows: list[dict]) -> None:
    """Upsert public."All Games (sr:sport_events)" rows for the given season_id. Each row must include sport_event_id and match fields."""
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
        supabase.table(T_GAMES).upsert(
            payload,
            on_conflict="season_id,sport_event_id",
            ignore_duplicates=False,
        ).execute()


def get_completed_games_for_season(season_id: str) -> list[dict]:
    """Return games rows that are completed (status in closed/ended)."""
    supabase = get_client()
    r = supabase.table(T_GAMES).select("*").eq("season_id", season_id).in_("status", ["closed", "ended"]).execute()
    return r.data or []


def get_game_ids_with_timeline(season_id: str) -> set[str]:
    """Return set of game row ids (uuid) that already have a timelines row."""
    supabase = get_client()
    sched = supabase.table(T_GAMES).select("id").eq("season_id", season_id).execute()
    sched_ids = [x["id"] for x in (sched.data or [])]
    if not sched_ids:
        return set()
    tl = supabase.table(T_TIMELINES).select("game_id").in_("game_id", sched_ids).execute()
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
    supabase.table(T_TIMELINES).upsert({
        "game_id": game_id,
        "timeline_json": timeline_json,
    }, on_conflict="game_id", ignore_duplicates=False).execute()


def get_timeline_json(game_id: str) -> dict | None:
    """Return stored timeline JSON for a game row, or None if not stored."""
    supabase = get_client()

    def _run():
        return supabase.table(T_TIMELINES).select("timeline_json").eq("game_id", game_id).execute()

    r = _supabase_execute_with_retry(_run)
    if r.data and len(r.data) > 0 and r.data[0].get("timeline_json"):
        return r.data[0]["timeline_json"]
    return None


def get_completed_matches_with_timelines(season_id: str) -> list[dict]:
    """Return completed games rows that have timelines, with timeline_json attached. For step4."""
    completed = get_completed_games_for_season(season_id)
    with_tl_ids = get_game_ids_with_timeline(season_id)
    targets = [row for row in completed if row["id"] in with_tl_ids]
    if not targets:
        return []

    game_ids = [str(row["id"]) for row in targets]
    tl_by_game: dict[str, dict] = {}
    supabase = get_client()
    chunk = 80
    for i in range(0, len(game_ids), chunk):
        part = game_ids[i : i + chunk]

        def _run(ids=part):
            return supabase.table(T_TIMELINES).select("game_id, timeline_json").in_("game_id", ids).execute()

        r = _supabase_execute_with_retry(_run)
        for rec in r.data or []:
            gid = rec.get("game_id")
            tj = rec.get("timeline_json")
            if gid and tj:
                tl_by_game[str(gid)] = tj

    result = []
    for row in targets:
        tl = tl_by_game.get(str(row["id"]))
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
                "competition_name": c.get("competition_name", ""),
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
                row["competition_name"] = meta["competition_name"]
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
                supabase.table(T_SEASONS)
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
            cr = (
                supabase.table(T_COMPETITIONS)
                .select("id,sportradar_competition_id,competition_name,gender,category_name,country_code")
                .in_("id", cu)
                .execute()
            )
            for crow in cr.data or []:
                cid = crow.get("id")
                if cid:
                    comp_by_uuid[str(cid)] = crow
        chunk_size = 200
        for i in range(0, len(event_ids), chunk_size):
            chunk = event_ids[i : i + chunk_size]
            sched = (
                supabase.table(T_GAMES)
                .select("sport_event_id,season_id,recorded")
                .in_("sport_event_id", chunk)
                .execute()
            )
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
                        "recorded": srow.get("recorded"),
                    }
    out = []
    for row in rows:
        meta = season_meta_by_event.get(row.get("sport_event_id", ""), {})
        out.append({
            "competition_id": str(meta.get("competition_id", "")),
            "season_id": str(meta.get("season_id", "")),
            "season_name": str(meta.get("season_name", "")),
            "sport_event_id": row.get("sport_event_id", ""),
            "recorded": meta.get("recorded"),
            "recording_id": _recording_id_for_event(row.get("sport_event_id"), meta.get("recorded")),
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


def get_completed_timelines_count() -> int:
    """
    Number of stored timeline rows (= completed matches with a timeline row).
    Uses PostgREST exact count — no full timeline_json download.
    """
    supabase = get_client()

    def _count():
        return (
            supabase.table(T_TIMELINES)
            .select("game_id", count="exact")
            .limit(1)
            .execute()
        )

    r = _supabase_execute_with_retry(_count)
    c = getattr(r, "count", None)
    if c is not None:
        return int(c)
    # Fallback: paginate lightweight game_id column only (no JSON).
    page_size = 1000
    offset = 0
    total = 0
    while True:
        tl = (
            _supabase_execute_with_retry(
                lambda o=offset: supabase.table(T_TIMELINES)
                .select("game_id")
                .range(o, o + page_size - 1)
                .execute()
            )
        )
        rows = tl.data or []
        total += len(rows)
        if len(rows) < page_size:
            break
        offset += page_size
    return total


def get_pipeline_stats_by_season_name() -> dict[str, dict[str, int]]:
    """
    Map season display name (same as config season_name / report rows) to pipeline totals
    for that season only: count of stored timeline rows (matches with timelines), per season.
    """
    if not USE_SUPABASE:
        return {}
    supabase = get_client()
    season_ids = get_or_create_seasons()
    uuid_to_name: dict[str, str] = {}
    for c in COMPETITIONS:
        u = season_ids.get(c["season_id"])
        if u:
            uuid_to_name[u] = c["season_name"]

    game_to_season: dict[str, str] = {}
    for season_uuid, name in uuid_to_name.items():
        offset = 0
        page = 1000
        while True:
            r = (
                supabase.table(T_GAMES)
                .select("id")
                .eq("season_id", season_uuid)
                .in_("status", ["closed", "ended"])
                .range(offset, offset + page - 1)
                .execute()
            )
            chunk = r.data or []
            for row in chunk:
                gid = row.get("id")
                if gid:
                    game_to_season[str(gid)] = name
            if len(chunk) < page:
                break
            offset += page

    agg: dict[str, dict[str, int]] = {n: {"matches_reviewed": 0} for n in uuid_to_name.values()}

    offset = 0
    page = 1000
    while True:
        r = (
            _supabase_execute_with_retry(
                lambda o=offset: supabase.table(T_TIMELINES)
                .select("game_id")
                .range(o, o + page - 1)
                .execute()
            )
        )
        chunk = r.data or []
        if not chunk:
            break
        for row in chunk:
            gid = row.get("game_id")
            if not gid:
                continue
            sn = game_to_season.get(str(gid))
            if not sn:
                continue
            agg[sn]["matches_reviewed"] += 1
        if len(chunk) < page:
            break
        offset += page
    return agg


def get_game_by_sport_event_id(season_id: str, sport_event_id: str) -> dict | None:
    """Return games row by sport_event_id, or None."""
    supabase = get_client()
    r = supabase.table(T_GAMES).select("*").eq("season_id", season_id).eq("sport_event_id", sport_event_id).execute()
    if r.data and len(r.data) > 0:
        return r.data[0]
    return None


def clear_own_goals() -> None:
    """Delete all own_goals. Call before re-inserting to avoid duplicates on pipeline re-run."""
    _clear_table_in_chunks("own_goals", "id")


def _clear_table_in_chunks(table: str, pk_col: str, *, batch: int = 500) -> None:
    """
    Delete all rows from a table in manageable chunks.

    Using a single huge `in_(...)` delete can hit URI/query-size limits with PostgREST and
    leave rows behind. Chunking keeps refreshes idempotent for large tables.
    """
    supabase = get_client()
    while True:
        r = supabase.table(table).select(pk_col).limit(batch).execute()
        ids = [x.get(pk_col) for x in (r.data or []) if x.get(pk_col)]
        if not ids:
            break
        supabase.table(table).delete().in_(pk_col, ids).execute()


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


def clear_penalty_shootout_matches() -> None:
    """Delete all penalty_shootout_matches rows."""
    _clear_table_in_chunks("penalty_shootout_matches", "id")


def upsert_penalty_shootout_matches(rows: list[dict], replace: bool = True) -> None:
    """Insert penalty shootout match rows derived from timelines."""
    supabase = get_client()
    if replace:
        clear_penalty_shootout_matches()
    if not rows:
        return
    for row in rows:
        payload = {
            "game_id": row.get("game_id"),
            "sport_event_id": row.get("sport_event_id", ""),
            "match_date": row.get("match_date"),
            "home_team": row.get("home_team"),
            "away_team": row.get("away_team"),
            "status": row.get("status"),
            "recorded": row.get("recorded"),
            "sportradar_competition_id": row.get("sportradar_competition_id"),
            "competition_name": row.get("competition_name"),
            "shootout_attempts": _int_or_none(row.get("shootout_attempts")) or 0,
            "sudden_death": bool(row.get("sudden_death")),
        }
        supabase.table("penalty_shootout_matches").upsert(
            payload,
            on_conflict="game_id",
            ignore_duplicates=False,
        ).execute()


def clear_var_timeline_events() -> None:
    """Delete all var_timeline_events rows."""
    _clear_table_in_chunks("var_timeline_events", "id")


def upsert_var_timeline_events(rows: list[dict], replace: bool = True) -> None:
    """Insert VAR timeline event rows derived from timeline_json."""
    supabase = get_client()
    if replace:
        clear_var_timeline_events()
    if not rows:
        return
    for row in rows:
        payload = {
            "game_id": row.get("game_id"),
            "sport_event_id": row.get("sport_event_id", ""),
            "match_date": row.get("match_date"),
            "home_team": row.get("home_team"),
            "away_team": row.get("away_team"),
            "status": row.get("status"),
            "recorded": row.get("recorded"),
            "sportradar_competition_id": row.get("sportradar_competition_id"),
            "competition_name": row.get("competition_name"),
            "timeline_event_id": _int_or_none(row.get("timeline_event_id")),
            "var_event_type": row.get("var_event_type", ""),
            "description": row.get("description"),
            "decision": row.get("decision"),
            "match_minute": _int_or_none(row.get("match_minute")),
            "stoppage_minute": _int_or_none(row.get("stoppage_minute")),
            "match_clock": row.get("match_clock"),
            "period_type": row.get("period_type"),
            "competitor_side": row.get("competitor_side"),
            "affected_team": row.get("affected_team"),
            "commentary": row.get("commentary"),
        }
        supabase.table("var_timeline_events").upsert(
            payload,
            on_conflict="game_id,timeline_event_id,var_event_type",
            ignore_duplicates=False,
        ).execute()


def _fetch_all_ordered(table: str, orders: list[tuple[str, bool]]) -> list[dict]:
    """Paginated select(*) for reporting; orders is (column, desc) pairs applied in order."""
    supabase = get_client()
    out: list[dict] = []
    offset = 0
    page = 1000
    while True:

        def _build_query():
            q = supabase.table(table).select("*")
            for col, desc in orders:
                q = q.order(col, desc=desc)
            return q.range(offset, offset + page - 1).execute()

        r = _supabase_execute_with_retry(_build_query)
        chunk = r.data or []
        out.extend(chunk)
        if len(chunk) < page:
            break
        offset += page
    return out


def fetch_penalty_shootout_match_rows() -> list[dict]:
    """All rows from public.penalty_shootout_matches (newest match dates first)."""
    rows = _fetch_all_ordered(
        "penalty_shootout_matches",
        [("match_date", True), ("id", False)],
    )
    for r in rows:
        r["recording_id"] = _recording_id_for_event(r.get("sport_event_id"), r.get("recorded"))
    return rows


def fetch_var_timeline_event_rows() -> list[dict]:
    """All rows from public.var_timeline_events."""
    rows = _fetch_all_ordered(
        "var_timeline_events",
        [("match_date", True), ("id", False)],
    )
    for r in rows:
        r["recording_id"] = _recording_id_for_event(r.get("sport_event_id"), r.get("recorded"))
    return rows


def fetch_var_unpaired_match_rows() -> list[dict]:
    """Rows from public.var_unpaired_event_matches view, with recorded from games (T_GAMES)."""
    rows = _fetch_all_ordered(
        "var_unpaired_event_matches",
        [("unpaired_var_starts", True), ("sport_event_id", False)],
    )
    event_ids = list({r.get("sport_event_id") for r in rows if r.get("sport_event_id")})
    recorded_by_event: dict[str, object] = {}
    if event_ids:
        supabase = get_client()
        chunk_size = 200
        for i in range(0, len(event_ids), chunk_size):
            chunk = event_ids[i : i + chunk_size]
            sched = (
                supabase.table(T_GAMES)
                .select("sport_event_id,recorded")
                .in_("sport_event_id", chunk)
                .execute()
            )
            for srow in sched.data or []:
                eid = srow.get("sport_event_id")
                if eid:
                    recorded_by_event[str(eid)] = srow.get("recorded")
    for r in rows:
        eid = r.get("sport_event_id")
        r["recorded"] = recorded_by_event.get(str(eid)) if eid else None
        r["recording_id"] = _recording_id_for_event(eid, r.get("recorded"))
    return rows


def _int_or_none(v):
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None
