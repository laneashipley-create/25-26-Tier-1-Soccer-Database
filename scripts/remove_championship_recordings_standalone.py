"""
Remove Championship recording rows from:
  1) Local Record/Replay export JSON (soccer record replay list of sr sport event ids.json)
  2) Standalone recordings Supabase — requires RECORDINGS_LIBRARY_SUPABASE_URL and
     RECORDINGS_LIBRARY_SUPABASE_SERVICE_ROLE_KEY (never uses main SUPABASE_URL).

Usage:
  python scripts/remove_championship_recordings_standalone.py
  python scripts/remove_championship_recordings_standalone.py --json-only
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from urllib.parse import quote

import httpx

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

RECORDINGS_JSON = _REPO_ROOT / "soccer record replay list of sr sport event ids.json"
T_LIBRARY = "Soccer // Recordings Library"

GAME_IDS_TO_REMOVE = frozenset(
    {
        "sr:sport_event:61491209",
        "sr:sport_event:61491213",
        "sr:sport_event:61491195",
        "sr:sport_event:61491205",
        "sr:sport_event:61491199",
        "sr:sport_event:61491159",
        "sr:sport_event:61491155",
        "sr:sport_event:61491161",
        "sr:sport_event:61491147",
        "sr:sport_event:61491151",
        "sr:sport_event:61491149",
        "sr:sport_event:61491165",
        "sr:sport_event:61491145",
        "sr:sport_event:61491153",
        "sr:sport_event:61491157",
        "sr:sport_event:68305676",
    }
)


def _prune_json(path: Path) -> tuple[int, int]:
    """Return (removed_count, remaining_count)."""
    with path.open(encoding="utf-8") as f:
        doc = json.load(f)
    recs = (((doc or {}).get("data") or {}).get("recordingsBySport")) or []
    if not isinstance(recs, list):
        raise ValueError("Unexpected JSON shape: data.recordingsBySport")

    kept = []
    removed = 0
    for r in recs:
        gid = ((r.get("meta") or {}) if isinstance(r.get("meta"), dict) else {}).get("gameId")
        if gid in GAME_IDS_TO_REMOVE:
            removed += 1
            continue
        kept.append(r)

    doc.setdefault("data", {})["recordingsBySport"] = kept
    with path.open("w", encoding="utf-8") as f:
        json.dump(doc, f, indent=4)
        f.write("\n")
    return removed, len(kept)


def _standalone_credentials() -> tuple[str, str]:
    import os

    url = os.environ.get("RECORDINGS_LIBRARY_SUPABASE_URL", "").strip()
    key = (
        os.environ.get("RECORDINGS_LIBRARY_SUPABASE_SERVICE_ROLE_KEY", "").strip()
        or os.environ.get("RECORDINGS_LIBRARY_SUPABASE_KEY", "").strip()
    )
    try:
        import config_local as cl

        url = url or str(getattr(cl, "RECORDINGS_LIBRARY_SUPABASE_URL", "") or "").strip()
        key = key or str(
            getattr(cl, "RECORDINGS_LIBRARY_SUPABASE_SERVICE_ROLE_KEY", "") or ""
        ).strip()
        key = key or str(getattr(cl, "RECORDINGS_LIBRARY_SUPABASE_KEY", "") or "").strip()
    except ImportError:
        pass
    return url, key


def _delete_standalone(rest_base: str, key: str) -> None:
    ids = sorted(GAME_IDS_TO_REMOVE)
    quoted = ",".join(f'"{x}"' for x in ids)
    filt = f"in.({quoted})"
    url = f"{rest_base.rstrip('/')}/{quote(T_LIBRARY, safe='')}"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }
    with httpx.Client(timeout=60.0) as client:
        r = client.delete(url, headers=headers, params={"sr_sport_event_id": filt})
        r.raise_for_status()
        print(f"Standalone DB delete OK ({r.status_code}) for {len(ids)} sr_sport_event_id(s).")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--json-only",
        action="store_true",
        help="Only prune the local JSON; do not call standalone Supabase.",
    )
    args = ap.parse_args()

    if not RECORDINGS_JSON.is_file():
        print(f"Missing {RECORDINGS_JSON}", file=sys.stderr)
        return 1

    rem, left = _prune_json(RECORDINGS_JSON)
    print(f"Local JSON: removed {rem} recording(s), {left} remain.")

    if args.json_only:
        return 0

    proj_url, key = _standalone_credentials()
    if not proj_url or not key:
        print(
            "Skipping standalone Supabase delete: set RECORDINGS_LIBRARY_SUPABASE_URL and "
            "RECORDINGS_LIBRARY_SUPABASE_SERVICE_ROLE_KEY (service role recommended).",
            file=sys.stderr,
        )
        return 0

    base = proj_url.rstrip("/")
    if not base.endswith("/rest/v1"):
        base = f"{base}/rest/v1"
    try:
        _delete_standalone(base, key)
    except httpx.HTTPStatusError as e:
        print(e.response.text[:800], file=sys.stderr)
        raise
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
