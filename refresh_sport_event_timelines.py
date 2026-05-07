"""
Refresh regular + extended timelines for one or more sport_event_ids.

This utility is intended for targeted corrections when Sportradar updates an event
after we have already stored timeline rows.

Behavior per sport_event_id:
  1) Resolve matching game row in Supabase (All Games table).
  2) Delete existing rows from:
       - Completed Matches - full sport_event_timelines
       - Completed Matches - Extended Timeline
  3) Re-fetch timeline.json + extended_timeline.json from Sportradar.
  4) Upsert fresh payloads back into both tables.

Usage examples:
  python refresh_sport_event_timelines.py sr:sport_event:69340226
  python refresh_sport_event_timelines.py sr:sport_event:69340226 sr:sport_event:123
  python refresh_sport_event_timelines.py --ids "sr:sport_event:69340226,sr:sport_event:123"
  python refresh_sport_event_timelines.py --file data/refresh_ids.txt
"""

from __future__ import annotations

import argparse
import json
import os
import time
import urllib.error

import db
import step3_fetch_regular_timelines
import step4_fetch_extended_timelines
from config import REQUEST_DELAY_SECONDS, USE_SUPABASE


def _parse_ids_arg(raw: str) -> list[str]:
    return [x.strip() for x in (raw or "").split(",") if x.strip()]


def _load_ids_from_file(path: str) -> list[str]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Input file not found: {path}")
    with open(path, encoding="utf-8") as f:
        text = f.read().strip()
    if not text:
        return []
    if path.lower().endswith(".json"):
        doc = json.loads(text)
        if isinstance(doc, list):
            return [str(x).strip() for x in doc if str(x).strip()]
        if isinstance(doc, dict) and isinstance(doc.get("sport_event_ids"), list):
            return [str(x).strip() for x in doc["sport_event_ids"] if str(x).strip()]
        raise ValueError("JSON file must be a list or an object with key 'sport_event_ids'")
    out: list[str] = []
    for line in text.splitlines():
        out.extend(_parse_ids_arg(line))
    return out


def _collect_target_ids(args: argparse.Namespace) -> list[str]:
    out: list[str] = []
    for raw in args.sport_event_ids:
        out.extend(_parse_ids_arg(raw))
    for raw in args.ids:
        out.extend(_parse_ids_arg(raw))
    for path in args.file:
        out.extend(_load_ids_from_file(path))
    deduped: list[str] = []
    seen: set[str] = set()
    for sid in out:
        key = sid.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(key)
    return deduped


def _get_games_by_sport_event_ids(event_ids: list[str]) -> dict[str, dict]:
    supabase = db.get_client()
    rows: list[dict] = []
    chunk_size = 200
    for i in range(0, len(event_ids), chunk_size):
        chunk = event_ids[i : i + chunk_size]
        res = (
            supabase.table(db.T_GAMES)
            .select("id,sport_event_id,start_time,home_team,away_team,status")
            .in_("sport_event_id", chunk)
            .execute()
        )
        rows.extend(res.data or [])
    out: dict[str, dict] = {}
    for row in rows:
        sid = str(row.get("sport_event_id") or "").strip()
        if not sid:
            continue
        prev = out.get(sid)
        if prev is None:
            out[sid] = row
            continue
        prev_start = str(prev.get("start_time") or "")
        row_start = str(row.get("start_time") or "")
        if row_start > prev_start:
            out[sid] = row
    return out


def _delete_existing_rows(game_id: str, sport_event_id: str) -> None:
    supabase = db.get_client()
    supabase.table(db.T_TIMELINES).delete().eq("game_id", game_id).execute()
    supabase.table(db.T_TIMELINES_EXTENDED).delete().eq("game_id", game_id).execute()
    supabase.table(db.T_TIMELINES).delete().eq("sport_event_id", sport_event_id).execute()
    supabase.table(db.T_TIMELINES_EXTENDED).delete().eq("sport_event_id", sport_event_id).execute()


def refresh_sport_event_ids(event_ids: list[str]) -> int:
    games_by_event = _get_games_by_sport_event_ids(event_ids)
    refreshed = 0
    missing = 0
    not_finished = 0
    errors = 0

    for idx, event_id in enumerate(event_ids, 1):
        game = games_by_event.get(event_id)
        if not game:
            print(f"[{idx}/{len(event_ids)}] {event_id}: no All Games row found; skipping")
            missing += 1
            continue
        game_id = str(game.get("id") or "").strip()
        if not game_id:
            print(f"[{idx}/{len(event_ids)}] {event_id}: game row missing id; skipping")
            missing += 1
            continue

        home = game.get("home_team") or ""
        away = game.get("away_team") or ""
        date = str(game.get("start_time") or "")[:10] or "?"
        print(f"[{idx}/{len(event_ids)}] Refreshing {date}  {home} vs {away}  ({event_id})")

        try:
            regular = step3_fetch_regular_timelines.fetch_timeline(event_id)
            time.sleep(REQUEST_DELAY_SECONDS)
            extended = step4_fetch_extended_timelines.fetch_extended_timeline(event_id)
            time.sleep(REQUEST_DELAY_SECONDS)

            regular_done = step3_fetch_regular_timelines.timeline_feed_marked_completed(regular)
            extended_done = step4_fetch_extended_timelines.timeline_feed_marked_completed(extended)
            if not (regular_done and extended_done):
                print(
                    "  Feed not marked completed on both payloads "
                    f"(regular={regular_done}, extended={extended_done}); leaving rows deleted."
                )
                not_finished += 1
                continue

            _delete_existing_rows(game_id, event_id)
            db.upsert_timeline(
                game_id,
                regular,
                sport_event_id=event_id,
                start_time=game.get("start_time"),
            )
            db.upsert_extended_timeline(
                game_id,
                extended,
                sport_event_id=event_id,
                start_time=game.get("start_time"),
            )
            refreshed += 1
        except urllib.error.HTTPError as e:
            print(f"  HTTP {e.code} while refreshing {event_id}; existing rows were left unchanged.")
            errors += 1
        except Exception as e:
            print(f"  Error while refreshing {event_id}: {e}; existing rows were left unchanged.")
            errors += 1

    print("\nDone targeted timeline refresh.")
    print(f"  Requested IDs          : {len(event_ids)}")
    print(f"  Refreshed successfully : {refreshed}")
    print(f"  Missing game rows      : {missing}")
    print(f"  Not completed in feed  : {not_finished}")
    print(f"  Errors                 : {errors}")
    return 0 if errors == 0 else 1


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Delete + refresh regular and extended timelines for specific sport_event_ids."
    )
    p.add_argument(
        "sport_event_ids",
        nargs="*",
        help="One or more sport_event_ids (space-separated). Comma-separated values are also allowed.",
    )
    p.add_argument(
        "--ids",
        action="append",
        default=[],
        help='Comma-separated sport_event_ids (repeatable), e.g. --ids "sr:sport_event:1,sr:sport_event:2"',
    )
    p.add_argument(
        "--file",
        action="append",
        default=[],
        help="Path to file with IDs (txt/csv lines or JSON list / {sport_event_ids:[...]}).",
    )
    return p


def main() -> None:
    if not USE_SUPABASE:
        raise SystemExit("Supabase is required (SUPABASE_URL + SUPABASE key).")

    args = _build_parser().parse_args()
    target_ids = _collect_target_ids(args)
    if not target_ids:
        raise SystemExit("No sport_event_ids provided. Pass IDs directly, with --ids, or with --file.")

    print(f"Target sport_event_ids: {len(target_ids)}")
    code = refresh_sport_event_ids(target_ids)
    raise SystemExit(code)


if __name__ == "__main__":
    main()
