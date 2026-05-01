"""
Remove Championship (sr:competition:18 / sr:season:130921) data introduced via backfill.

Deletes:
  - own_goals rows for those sport_event_ids
  - Soccer // Recordings Library rows for those sport_event_ids
  - Seasons row sr:season:130921 (cascades games → timelines, penalty_shootout, var_timeline_events)
  - Competitions row sr:competition:18 if no seasons remain pointing at it

Requires SUPABASE_SERVICE_ROLE_KEY (or config_local.SUPABASE_KEY) and SUPABASE_URL.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from urllib.parse import quote

import httpx

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

EVENT_IDS = [
    "sr:sport_event:61491145",
    "sr:sport_event:61491147",
    "sr:sport_event:61491149",
    "sr:sport_event:61491151",
    "sr:sport_event:61491153",
    "sr:sport_event:61491155",
    "sr:sport_event:61491157",
    "sr:sport_event:61491159",
    "sr:sport_event:61491161",
    "sr:sport_event:61491165",
    "sr:sport_event:61491195",
    "sr:sport_event:61491199",
    "sr:sport_event:61491205",
    "sr:sport_event:61491209",
    "sr:sport_event:61491213",
    "sr:sport_event:68305676",
]

SPORTRADAR_SEASON_ID = "sr:season:130921"
SPORTRADAR_COMPETITION_ID = "sr:competition:18"

T_SEASONS = "Seasons (current sr:season:ID)"
T_COMPETITIONS = "Competitions"
T_OWN_GOALS = "own_goals"
T_RECORDINGS = "Soccer // Recordings Library"


def _rest_base() -> str:
    try:
        from config import SUPABASE_URL as cfg_url

        url = (os.environ.get("SUPABASE_URL", "").strip() or (cfg_url or "").strip()).strip()
    except ImportError:
        url = os.environ.get("SUPABASE_URL", "").strip()
    if not url:
        url = "https://yoesorfzvtbdmvrdtqoo.supabase.co"
    if not url.endswith("/rest/v1"):
        url = f"{url}/rest/v1"
    return url


def _key() -> str:
    k = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    if k:
        return k
    try:
        from config_local import SUPABASE_KEY as lk

        return (lk or "").strip()
    except ImportError:
        pass
    try:
        from config import SUPABASE_KEY as ck

        return (ck or "").strip()
    except ImportError:
        pass
    return (
        os.environ.get("SUPABASE_ANON_KEY", "").strip()
        or os.environ.get("SUPABASE_KEY", "").strip()
    )


def _headers(key: str) -> dict[str, str]:
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "return=representation,count=exact",
    }


def _tbl(rest: str, table: str) -> str:
    return f"{rest}/{quote(table, safe='')}"


def _delete_own_goals_in(client: httpx.Client, rest: str, key: str, ids: list[str]) -> None:
    """Delete own_goals rows where sport_event_id is in ids (chunked)."""
    url = _tbl(rest, T_OWN_GOALS)
    h = _headers(key)
    chunk = 50
    for i in range(0, len(ids), chunk):
        part = ids[i : i + chunk]
        quoted = ",".join(f'"{x}"' for x in part)
        filt = f"in.({quoted})"
        r = client.delete(url, headers=h, params={"sport_event_id": filt})
        r.raise_for_status()
        print(f"  own_goals chunk {i // chunk + 1}: {r.status_code}")


def _delete_recordings_in(client: httpx.Client, rest: str, key: str, ids: list[str]) -> None:
    url = _tbl(rest, T_RECORDINGS)
    h = _headers(key)
    quoted = ",".join(f'"{x}"' for x in ids)
    filt = f"in.({quoted})"
    r = client.delete(url, headers=h, params={"sr_sport_event_id": filt})
    r.raise_for_status()
    print(f"  recordings library delete: {r.status_code}")


def main() -> int:
    key = _key()
    if not key:
        print("Missing SUPABASE_SERVICE_ROLE_KEY / SUPABASE_KEY.", file=sys.stderr)
        return 1

    rest = _rest_base()
    headers_get = {"apikey": key, "Authorization": f"Bearer {key}", "Accept": "application/json"}

    with httpx.Client(timeout=120.0) as client:
        # Resolve season UUID (sanity check)
        surl = _tbl(rest, T_SEASONS)
        r = client.get(
            surl,
            headers=headers_get,
            params={
                "select": "id,sportradar_season_id,competition_id",
                "sportradar_season_id": f"eq.{SPORTRADAR_SEASON_ID}",
            },
        )
        r.raise_for_status()
        seasons = r.json()
        if not seasons:
            print(f"No season row found for {SPORTRADAR_SEASON_ID}; nothing to delete.")
            return 0
        season_row = seasons[0]
        sid = season_row["id"]
        print(f"Season {SPORTRADAR_SEASON_ID} uuid={sid}")

        # Games count before (informational)
        gurl = _tbl(rest, "All Games (sr:sport_events)")
        rg = client.get(
            gurl,
            headers={**headers_get, "Prefer": "count=exact"},
            params={"select": "id", "season_id": f"eq.{sid}"},
        )
        rg.raise_for_status()
        gc = rg.headers.get("content-range", "")
        print(f"Games rows for this season (header): {gc}")

        print("Deleting own_goals for 16 sport_event_ids...")
        _delete_own_goals_in(client, rest, key, EVENT_IDS)

        print(f"Deleting rows from '{T_RECORDINGS}' for those sport_event_ids...")
        try:
            _delete_recordings_in(client, rest, key, EVENT_IDS)
        except httpx.HTTPStatusError as e:
            print(f"  Recordings library delete skipped or failed: {e.response.status_code} {e.response.text[:500]}")
            raise

        print(f"Deleting season row {SPORTRADAR_SEASON_ID} (cascades games, timelines, VAR, shootouts)...")
        rdel = client.delete(
            surl,
            headers=_headers(key),
            params={"sportradar_season_id": f"eq.{SPORTRADAR_SEASON_ID}"},
        )
        rdel.raise_for_status()
        print(f"  Season delete: {rdel.status_code}")

        # Competition: delete only if unused
        curl = _tbl(rest, T_COMPETITIONS)
        rc = client.get(
            curl,
            headers=headers_get,
            params={
                "select": "id",
                "sportradar_competition_id": f"eq.{SPORTRADAR_COMPETITION_ID}",
            },
        )
        rc.raise_for_status()
        comps = rc.json()
        if not comps:
            print("No competition row for sr:competition:18.")
            return 0
        comp_uuid = comps[0]["id"]

        rs2 = client.get(
            surl,
            headers=headers_get,
            params={"select": "id", "competition_id": f"eq.{comp_uuid}"},
        )
        rs2.raise_for_status()
        remaining = rs2.json()
        if remaining:
            print(
                f"Competition {SPORTRADAR_COMPETITION_ID} still referenced by {len(remaining)} season(s); not deleting competition.",
                file=sys.stderr,
            )
            return 0

        print(f"Deleting competition row {SPORTRADAR_COMPETITION_ID}...")
        cd = client.delete(
            curl,
            headers=_headers(key),
            params={"sportradar_competition_id": f"eq.{SPORTRADAR_COMPETITION_ID}"},
        )
        cd.raise_for_status()
        print(f"  Competition delete: {cd.status_code}")

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
