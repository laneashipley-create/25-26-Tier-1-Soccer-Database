"""
Resolve current Sportradar season_id + name per competition_id (trial API).

Usage (from repo root, with SPORTRADAR_API_KEY or config_local.API_KEY):
  python tools/resolve_season_ids.py

Prints TSV: competition_id, season_id, season_name
Use output to fill config.COMPETITIONS season_id / season_name.
"""

from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import API_KEY, BASE_URL  # noqa: E402


def fetch_seasons(competition_id: str) -> list[dict]:
    url = f"{BASE_URL}/competitions/{competition_id}/seasons.json?api_key={API_KEY}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode())
    return data.get("seasons") or []


def pick_season(seasons: list[dict]) -> dict | None:
    if not seasons:
        return None
    active = [s for s in seasons if not s.get("disabled")]
    pool = active if active else seasons

    def sort_key(s: dict):
        sd = s.get("start_date") or ""
        return sd

    pool.sort(key=sort_key, reverse=True)
    return pool[0]


def main() -> int:
    if not API_KEY:
        print("Set SPORTRADAR_API_KEY or config_local.API_KEY", file=sys.stderr)
        return 1
    comp_ids = [
        "sr:competition:7",
        "sr:competition:325",
        "sr:competition:35",
        "sr:competition:17",
        "sr:competition:196",
        "sr:competition:8",
        "sr:competition:34",
        "sr:competition:242",
        "sr:competition:23",
        "sr:competition:679",
        "sr:competition:34480",
        "sr:competition:23755",
        "sr:competition:16",
        "sr:competition:1",
        "sr:competition:217",
        "sr:competition:19",
        "sr:competition:329",
        "sr:competition:335",
        "sr:competition:328",
        "sr:competition:346",
        "sr:competition:465",
        "sr:competition:384",
        "sr:competition:27466",
    ]
    for cid in comp_ids:
        try:
            seasons = fetch_seasons(cid)
            s = pick_season(seasons)
            if not s:
                print(f"{cid}\t\t(no seasons)", flush=True)
                continue
            sid = s.get("id", "")
            name = s.get("name", "")
            print(f"{cid}\t{sid}\t{name}", flush=True)
        except (urllib.error.URLError, OSError, KeyError, json.JSONDecodeError) as e:
            print(f"{cid}\t\tERROR {e}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
