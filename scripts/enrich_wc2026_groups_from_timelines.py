from __future__ import annotations

import csv
import pathlib
import re
import sys
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import API_KEY, BASE_URL_SOCCER_EXTENDED

CSV_PATH = ROOT / "wc2026_schedule.csv"
FALLBACK_CSV_PATH = ROOT / "wc2026_schedule.with_groups.csv"


def _group_from_root(root: ET.Element) -> str:
    """Extract group letter from <group group_name="D" />."""
    for group in root.findall(".//{*}group"):
        group_name = (group.attrib.get("group_name") or "").strip()
        if group_name:
            return group_name
        name = (group.attrib.get("name") or "").strip()
        if name:
            m = re.search(r"Group\s+([A-Z])\b", name, flags=re.IGNORECASE)
            if m:
                return m.group(1).upper()
    return ""


def _stage_type_for_match_date(root: ET.Element, match_start_iso: str) -> str:
    """
    Map match kickoff date to Sportradar <stage type="..."/> (league | cup).
    When date falls in overlapping ranges, lower <stage order="..."/> wins.
    """
    match_day = (match_start_iso or "")[:10]
    if len(match_day) < 10:
        return ""

    candidates: list[tuple[int, str]] = []
    for stag in root.findall(".//{*}stage"):
        typ = (stag.attrib.get("type") or "").strip().lower()
        if typ not in ("league", "cup"):
            continue
        sd = (stag.attrib.get("start_date") or "")[:10]
        ed = (stag.attrib.get("end_date") or "")[:10]
        if len(sd) < 10 or len(ed) < 10:
            continue
        if sd <= match_day <= ed:
            try:
                order = int(stag.attrib.get("order") or "999")
            except ValueError:
                order = 999
            candidates.append((order, typ))

    if not candidates:
        return ""
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]


def _fetch_timeline_root(sport_event_id: str) -> ET.Element:
    if not API_KEY:
        raise RuntimeError(
            "Missing API key. Set SPORTRADAR_API_KEY or config_local.API_KEY first."
        )
    url = (
        f"{BASE_URL_SOCCER_EXTENDED}/sport_events/{sport_event_id}/timeline.xml"
        f"?api_key={API_KEY}"
    )
    req = urllib.request.Request(url, headers={"Accept": "application/xml"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        xml_bytes = resp.read()
    return ET.fromstring(xml_bytes)


def main() -> None:
    if not CSV_PATH.exists():
        raise SystemExit(f"Missing CSV: {CSV_PATH}")

    with CSV_PATH.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        fieldnames = list(reader.fieldnames or [])
        if "group" not in fieldnames:
            fieldnames.append("group")
        if "stage" not in fieldnames:
            fieldnames.append("stage")
        rows = list(reader)

    cache: dict[str, ET.Element | None] = {}
    updated = 0
    failed = 0

    for row in rows:
        sport_event_id = (row.get("sport_event_id") or "").strip()
        start_iso = (row.get("start_time") or "").strip()
        if not sport_event_id:
            row["group"] = ""
            row["stage"] = ""
            continue

        cache_key = sport_event_id
        if cache_key not in cache:
            try:
                cache[cache_key] = _fetch_timeline_root(sport_event_id)
            except (urllib.error.URLError, TimeoutError, ET.ParseError, RuntimeError):
                cache[cache_key] = None
                failed += 1
            time.sleep(0.18)

        root = cache[cache_key]
        if root is None:
            grp, stg = "", ""
        else:
            grp = _group_from_root(root)
            stg = _stage_type_for_match_date(root, start_iso)
        before_g = (row.get("group") or "").strip()
        before_s = (row.get("stage") or "").strip()
        row["group"] = grp
        row["stage"] = stg
        if before_g != grp or before_s != stg:
            updated += 1

    target_path = CSV_PATH
    try:
        with target_path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
    except PermissionError:
        target_path = FALLBACK_CSV_PATH
        with target_path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    print(
        f"Updated {target_path.name} with group + stage (timeline). "
        f"rows_changed={updated}, api_failures={failed}, total_rows={len(rows)}"
    )


if __name__ == "__main__":
    main()
