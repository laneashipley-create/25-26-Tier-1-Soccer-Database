"""

Load Record/Replay export JSON into Supabase for the Soccer // Recordings Library.



Target table — Library Details Excel columns:

  Migration 20260430153000_soccer_recordings_library.sql + rename 20260430220000_*.

  Physical table name: "Soccer // Recordings Library" (slashes require URL-encoded REST paths).



Environment:

  RECORDINGS_LIBRARY_SUPABASE_URL — optional; defaults to SUPABASE_URL / config.

  RECORDINGS_LIBRARY_SUPABASE_SERVICE_ROLE_KEY — optional; defaults to SUPABASE_KEY.



Usage:

  python sync_recordings_library.py

  python sync_recordings_library.py path/to/export.json

"""



from __future__ import annotations



import json

import os

import sys

from datetime import datetime, timezone

from pathlib import Path

from urllib.parse import quote



import httpx



RECORDINGS_JSON_DEFAULT = "soccer record replay list of sr sport event ids.json"

T_LIBRARY = "Soccer // Recordings Library"





def _repo_root() -> Path:

    return Path(__file__).resolve().parent





def _recordings_supabase_urls() -> tuple[str, str]:

    url = (

        os.environ.get("RECORDINGS_LIBRARY_SUPABASE_URL", "").strip()

        or os.environ.get("SUPABASE_URL", "").strip()

    )

    key = (

        os.environ.get("RECORDINGS_LIBRARY_SUPABASE_SERVICE_ROLE_KEY", "").strip()

        or os.environ.get("RECORDINGS_LIBRARY_SUPABASE_KEY", "").strip()

    )

    if not key:

        try:

            from config_local import SUPABASE_KEY as _lk  # type: ignore



            key = str(_lk).strip()

        except ImportError:

            key = (

                os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()

                or os.environ.get("SUPABASE_ANON_KEY", "").strip()

            )

    if not url:

        try:

            from config import SUPABASE_URL as _u  # type: ignore



            url = str(_u).strip()

        except ImportError:

            pass

    if not url or not key:

        raise SystemExit(

            "Missing Supabase credentials. Set RECORDINGS_LIBRARY_SUPABASE_URL and "

            "RECORDINGS_LIBRARY_SUPABASE_SERVICE_ROLE_KEY, or SUPABASE_URL + SUPABASE_KEY "

            "(service role recommended)."

        )

    return url, key





def _rest_v1_base(project_url: str) -> str:

    base = project_url.rstrip("/")

    if not base.endswith("/rest/v1"):

        base = f"{base}/rest/v1"

    return base





def _table_endpoint(rest_v1_base: str, table: str) -> str:

    return f"{rest_v1_base}/{quote(table, safe='')}"





def _headers(key: str) -> dict[str, str]:

    return {

        "apikey": key,

        "Authorization": f"Bearer {key}",

        "Content-Type": "application/json",

        "Prefer": "return=minimal",

    }





def _execute_with_retry_http(

    fn,

    *,

    attempts: int = 6,

    base_delay: float = 1.5,

):

    import time



    delay = base_delay

    last_exc: BaseException | None = None

    for i in range(attempts):

        try:

            return fn()

        except httpx.HTTPStatusError as e:

            last_exc = e

            code = e.response.status_code

            if code not in (429, 502, 503, 504, 520):

                raise

        if i < attempts - 1:

            time.sleep(delay)

            delay = min(delay * 2.0, 60.0)

    assert last_exc is not None

    raise last_exc





def _truncate_recordings_library_rpc(client: httpx.Client, rest_v1_base: str, key: str) -> None:
    """Truncate + restart identity so the next inserts use ID 1, 2, ... (see migration truncate_soccer_recordings_library)."""
    rpc_url = f"{rest_v1_base.rstrip('/')}/rpc/truncate_soccer_recordings_library"
    hdr = _headers(key)

    def post():
        r = client.post(rpc_url, headers=hdr, json={})
        r.raise_for_status()

    _execute_with_retry_http(post)


def _insert_batches(

    client: httpx.Client,

    endpoint: str,

    key: str,

    rows: list[dict],

    *,

    batch_size: int = 200,

) -> None:

    hdr = _headers(key)

    for i in range(0, len(rows), batch_size):

        chunk = rows[i : i + batch_size]



        def ins(c=chunk):

            r = client.post(endpoint, headers=hdr, json=c)

            r.raise_for_status()



        _execute_with_retry_http(ins)





def _parse_iso_utc(s: object) -> datetime | None:

    if s is None or s == "":

        return None

    raw = str(s).strip().replace("Z", "+00:00")

    try:

        dt = datetime.fromisoformat(raw)

    except ValueError:

        return None

    if dt.tzinfo is None:

        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)





def _api_names_lower(api_objs: list[dict]) -> set[str]:

    out: set[str] = set()

    for a in api_objs:

        if not isinstance(a, dict):

            continue

        n = str(a.get("name") or "").strip().lower()

        if n:

            out.add(n)

    return out





def _excel_api_flags(names: set[str]) -> dict[str, bool]:

    """Map JSON `apis[].name` slugs to Library Details checkbox columns."""

    summary = (

        "summary" in names

        or "summaries" in names

        or "extended-summary" in names

    )

    stats = "statistics" in names

    lineup = "lineups" in names

    timeline = "timeline" in names or "extended-timeline" in names

    var_ = any(

        n == "var"

        or n.startswith("var-")

        or n.endswith("-var")

        or "_var_" in n

        or n.startswith("var_")

        for n in names

    )

    commentary = any("commentary" in n for n in names)

    win_prob = any(

        "prob" in n or "win_probability" in n or "win-prob" in n or "winprob" in n

        for n in names

    )

    return {

        "Event Summary": summary,

        "Event Statistics": stats,

        "Event Lineup": lineup,

        "Event VAR": var_,

        "Event Commentary": commentary,

        "Event Timeline": timeline,

        "Event Win Prob": win_prob,

    }





def load_export(path: Path) -> list[dict]:

    doc = json.loads(path.read_text(encoding="utf-8"))

    recs = (((doc or {}).get("data") or {}).get("recordingsBySport") or [])

    if not isinstance(recs, list):

        return []

    return [x for x in recs if isinstance(x, dict)]





def library_rows_from_export(records: list[dict]) -> list[dict]:

    rows: list[dict] = []



    for rec in records:

        rid = rec.get("id")

        meta = rec.get("meta") if isinstance(rec.get("meta"), dict) else {}

        game_id = meta.get("gameId")

        if not rid or not game_id:

            continue



        apis = rec.get("apis") if isinstance(rec.get("apis"), list) else []

        api_objs = [a for a in apis if isinstance(a, dict)]

        flags = _excel_api_flags(_api_names_lower(api_objs))



        scheduled_top = _parse_iso_utc(rec.get("scheduled"))



        rows.append(

            {

                "sr_sport_event_id": str(game_id),

                "recording_id": str(rid),

                "Title": rec.get("title"),

                "Season": None,

                "Competition Name": rec.get("league"),

                "Category": None,

                "Sport Event Status": None,

                "Sport Event Start": scheduled_top.isoformat() if scheduled_top else None,

                "Sport Event Venue": None,

                "SR Sport Event Venue ID": None,

                "Sport Event City": None,

                "Event Summary": flags["Event Summary"],

                "Event Statistics": flags["Event Statistics"],

                "Event Lineup": flags["Event Lineup"],

                "Event VAR": flags["Event VAR"],

                "Event Commentary": flags["Event Commentary"],

                "Event Timeline": flags["Event Timeline"],

                "Event Win Prob": flags["Event Win Prob"],

                "Status": None,

            }

        )



    return rows





def sync(json_path: Path) -> None:

    records = load_export(json_path)

    lib_rows = library_rows_from_export(records)

    if not lib_rows:

        print("No recordings found in JSON (expected path data.recordingsBySport).")

        return



    url, key = _recordings_supabase_urls()

    rest = _rest_v1_base(url)

    endpoint = _table_endpoint(rest, T_LIBRARY)



    with httpx.Client(timeout=120.0) as client:

        print(f"Truncating {T_LIBRARY} (reset ID sequence) ...")

        _truncate_recordings_library_rpc(client, rest, key)



        print(f"Inserting {len(lib_rows)} rows into {T_LIBRARY} ...")

        _insert_batches(client, endpoint, key, lib_rows)

    print("Done.")





def main() -> int:

    path = Path(sys.argv[1]) if len(sys.argv) > 1 else _repo_root() / RECORDINGS_JSON_DEFAULT

    if not path.is_file():

        print(f"File not found: {path}", file=sys.stderr)

        return 1

    sync(path)

    return 0





if __name__ == "__main__":

    raise SystemExit(main())


