"""List stored timelines whose final sport_event_status.match_status is ap (after penalties).

Same rule as SQL:
  (timeline_json #>> '{sport_event_status,match_status}') = 'ap'

PostgREST note: methods like .eq(column, value) send operator ``eq`` (equals) in the query
string; that is not a match_status code. The only status we filter on is AFTER_PENALTIES.

Requires Supabase env (same as rest of project). Run: python list_shootout_matches.py

Sudden death (past the first five kicks per team, IFAB-style): when the feed includes
``type: "penalty_shootout"`` events with ``period_type: "penalties"``, each is one attempt
(score or miss). More than ten attempts means play continued after the initial best-of-five
phase. Some timelines only list ``score_change`` / ``method: "penalty"`` for goals; then
attempt counts and sudden death cannot be inferred from JSON alone.
"""
from __future__ import annotations

from db import T_GAMES, T_TIMELINES, get_client

# Sportradar: match ended after penalty shootout (not the string "eq").
AFTER_PENALTIES_MATCH_STATUS = "ap"


def penalty_shootout_attempt_count(timeline_json: dict | None) -> int:
    """Count shootout attempts when Sportradar emits penalty_shootout rows (goals and misses)."""
    tl = (timeline_json or {}).get("timeline") or []
    return sum(
        1
        for e in tl
        if e.get("type") == "penalty_shootout" and e.get("period_type") == "penalties"
    )


def sudden_death_label(timeline_json: dict | None) -> str:
    """yes = more than 10 shootout attempts (IFAB first phase); unknown = sparse feed."""
    n = penalty_shootout_attempt_count(timeline_json)
    if n == 0:
        return "unknown"
    return "yes" if n > 10 else "no"


def _games_by_id(c, game_ids: list[str]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    chunk = 80
    for i in range(0, len(game_ids), chunk):
        part = game_ids[i : i + chunk]
        if not part:
            continue
        r = (
            c.table(T_GAMES)
            .select("id, sport_event_id, home_team, away_team, start_time, status")
            .in_("id", part)
            .execute()
        )
        for g in r.data or []:
            gid = g.get("id")
            if gid is not None:
                out[str(gid)] = g
    return out


def main() -> None:
    c = get_client()
    status_path = "timeline_json->sport_event_status->>match_status"
    r = (
        c.table(T_TIMELINES)
        .select("game_id, timeline_json")
        .eq(status_path, AFTER_PENALTIES_MATCH_STATUS)
        .execute()
    )
    rows = r.data or []
    game_ids = sorted({str(row["game_id"]) for row in rows if row.get("game_id")})
    games = _games_by_id(c, game_ids)

    print(f"total_shootout_matches\t{len(rows)}\n")

    def sort_key(row: dict) -> str:
        g = games.get(str(row.get("game_id") or "")) or {}
        st = g.get("start_time")
        return str(st) if st else ""

    print(
        "date\thome\taway\tstatus\tshootout_attempts\tsudden_death\t"
        "sport_event_id"
    )
    for row in sorted(rows, key=sort_key):
        g = games.get(str(row.get("game_id") or "")) or {}
        st = g.get("start_time")
        date = str(st)[:10] if st else "?"
        hid = g.get("sport_event_id", "")
        home = (g.get("home_team") or "")[:40]
        away = (g.get("away_team") or "")[:40]
        stat = g.get("status", "")
        tj = row.get("timeline_json")
        n = penalty_shootout_attempt_count(tj)
        sd = sudden_death_label(tj)
        attempts = str(n) if n else ""
        print(f"{date}\t{home}\t{away}\t{stat}\t{attempts}\t{sd}\t{hid}")


if __name__ == "__main__":
    main()
