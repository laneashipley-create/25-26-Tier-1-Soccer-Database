"""
Align public."All Games (sr:sport_events)".recorded with the soccer replay export JSON.

Source of truth: soccer record replay list of sr sport event ids.json
(recordingsBySport[].meta.gameId = sr:sport_event:…).

- Sets recorded = true for every game row whose sport_event_id appears in the export.
- Sets recorded = false for every game row that was recorded=true but whose sport_event_id
  is no longer in the export.

Run automatically after step 2 in run_all.py (full + --full-backfill) when USE_SUPABASE is set.
Also: python sync_games_recorded_from_export.py
"""

from __future__ import annotations

import json
import os

from config import USE_SUPABASE


def load_recorded_sport_event_ids_from_export() -> set[str]:
    """Distinct sr:sport_event:… IDs from meta.gameId in the export JSON."""
    import db as db_mod

    path = os.path.join(os.path.dirname(__file__), db_mod.RECORDINGS_EXPORT_JSON)
    if not os.path.isfile(path):
        return set()
    try:
        with open(path, encoding="utf-8") as f:
            doc = json.load(f)
    except Exception:
        return set()
    recs = (((doc or {}).get("data") or {}).get("recordingsBySport") or [])
    out: set[str] = set()
    for rec in recs:
        if not isinstance(rec, dict):
            continue
        meta = rec.get("meta") or {}
        if not isinstance(meta, dict):
            continue
        gid = meta.get("gameId")
        if gid:
            out.add(str(gid).strip())
    return out


def main() -> tuple[int, int]:
    """
    Apply recorded flags in Supabase. Returns (n_set_true_batches_rows_attempted, n_cleared_rows).

    Row counts are best-effort (depends on PostgREST response); cleared is exact batches applied.
    """
    if not USE_SUPABASE:
        print("  USE_SUPABASE is False — skipping games.recorded sync from export JSON.")
        return (0, 0)

    import db as db_mod

    recorded_ids = load_recorded_sport_event_ids_from_export()
    path = os.path.join(os.path.dirname(__file__), db_mod.RECORDINGS_EXPORT_JSON)
    if not recorded_ids:
        print(f"  No sport_event IDs in recordings export ({path}) — skipping games.recorded sync.")
        return (0, 0)

    supabase = db_mod.get_client()
    t_games = db_mod.T_GAMES
    ids_list = sorted(recorded_ids)
    chunk_size = 80
    true_batches = 0
    for i in range(0, len(ids_list), chunk_size):
        chunk = ids_list[i : i + chunk_size]

        def _patch_true(ch=chunk):
            return supabase.table(t_games).update({"recorded": True}).in_("sport_event_id", ch).execute()

        db_mod._supabase_execute_with_retry(_patch_true)
        true_batches += 1

    # Clear recorded for rows still true but not in export
    to_false: list[str] = []
    offset = 0
    page = 1000
    while True:
        r = (
            supabase.table(t_games)
            .select("id,sport_event_id")
            .eq("recorded", True)
            .range(offset, offset + page - 1)
            .execute()
        )
        rows = r.data or []
        for row in rows:
            sid = str(row.get("sport_event_id") or "").strip()
            gid = row.get("id")
            if gid and sid and sid not in recorded_ids:
                to_false.append(str(gid))
        if len(rows) < page:
            break
        offset += page

    cleared = 0
    for i in range(0, len(to_false), chunk_size):
        chunk = to_false[i : i + chunk_size]

        def _patch_false(ch=chunk):
            return supabase.table(t_games).update({"recorded": False}).in_("id", ch).execute()

        db_mod._supabase_execute_with_retry(_patch_false)
        cleared += len(chunk)

    print(
        f"  games.recorded sync: {len(recorded_ids)} sport_event_id(s) in export JSON; "
        f"set true in batches ({true_batches}); cleared {cleared} row(s) no longer in export."
    )
    db_mod.invalidate_recording_id_export_cache()
    return (true_batches, cleared)


if __name__ == "__main__":
    main()
