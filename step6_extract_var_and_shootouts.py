"""
STEP 5 — Derive penalty shootout + VAR tables from stored timeline JSON.

Writes (replace mode):
  - public.penalty_shootout_matches
  - public.var_timeline_events

Requires USE_SUPABASE=True and populated public."Completed Matches - full sport_event_timelines".
"""

from __future__ import annotations

from config import USE_SUPABASE


VAR_TYPES = {"video_assistant_referee", "video_assistant_referee_over"}


def _safe_int(value):
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _extract_penalty_shootout_row(row: dict) -> dict | None:
    timeline_json = row.get("timeline_json") or {}
    status = timeline_json.get("sport_event_status") or {}
    if status.get("match_status") != "ap":
        return None

    timeline = timeline_json.get("timeline") or []
    attempts = 0
    for ev in timeline:
        if (
            ev.get("type") == "penalty_shootout"
            and ev.get("period_type") == "penalties"
        ):
            attempts += 1

    return {
        "game_id": row.get("id"),
        "sport_event_id": row.get("sport_event_id"),
        "match_date": str(row.get("start_time") or "")[:10] or None,
        "home_team": row.get("home_team"),
        "away_team": row.get("away_team"),
        "status": row.get("status"),
        "recorded": row.get("recorded"),
        "sportradar_competition_id": row.get("competition_id"),
        "competition_name": row.get("competition_name"),
        "shootout_attempts": attempts,
        "sudden_death": attempts > 10,
    }


def _extract_var_rows(row: dict) -> list[dict]:
    timeline_json = row.get("timeline_json") or {}
    timeline = timeline_json.get("timeline") or []
    out = []
    for ev in timeline:
        event_type = ev.get("type")
        if event_type not in VAR_TYPES:
            continue
        competitor = ev.get("competitor")
        if competitor == "home":
            affected_team = row.get("home_team")
        elif competitor == "away":
            affected_team = row.get("away_team")
        else:
            affected_team = None

        commentaries = ev.get("commentaries") or []
        commentary = ""
        if commentaries:
            commentary = commentaries[0].get("text") or ""

        out.append(
            {
                "game_id": row.get("id"),
                "sport_event_id": row.get("sport_event_id"),
                "match_date": str(row.get("start_time") or "")[:10] or None,
                "home_team": row.get("home_team"),
                "away_team": row.get("away_team"),
                "status": row.get("status"),
                "recorded": row.get("recorded"),
                "sportradar_competition_id": row.get("competition_id"),
                "competition_name": row.get("competition_name"),
                "timeline_event_id": _safe_int(ev.get("id")),
                "var_event_type": event_type,
                "description": (ev.get("description") or "").strip() or None,
                "decision": (ev.get("decision") or "").strip() or None,
                "match_minute": _safe_int(ev.get("match_time")),
                "stoppage_minute": _safe_int(ev.get("stoppage_time")),
                "match_clock": ev.get("match_clock"),
                "period_type": ev.get("period_type"),
                "competitor_side": competitor,
                "affected_team": affected_team,
                "commentary": commentary,
            }
        )
    return out


def main():
    if not USE_SUPABASE:
        print("USE_SUPABASE is False. Skipping step5_extract_var_and_shootouts.")
        return

    import db

    rows = db.get_completed_matches_with_timelines_for_configured_seasons()
    print(f"Scanning {len(rows)} completed timelines from Supabase...")

    penalty_rows = []
    var_rows = []
    for row in rows:
        penalty_row = _extract_penalty_shootout_row(row)
        if penalty_row:
            penalty_rows.append(penalty_row)
        var_rows.extend(_extract_var_rows(row))

    db.upsert_penalty_shootout_matches(penalty_rows, replace=True)
    db.upsert_var_timeline_events(var_rows, replace=True)

    print("Done.")
    print(f"  Penalty shootout matches: {len(penalty_rows)}")
    print(f"  VAR timeline events     : {len(var_rows)}")


if __name__ == "__main__":
    main()
