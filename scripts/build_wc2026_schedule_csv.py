from __future__ import annotations

import csv
import pathlib


ROOT = pathlib.Path(__file__).resolve().parents[1]
SOURCE = ROOT / "data" / "schedule.csv"
OUTPUT = ROOT / "wc2026_schedule.csv"
SEASON_ID = "sr:season:101177"


def main() -> None:
    if not SOURCE.exists():
        raise SystemExit(f"Missing source CSV: {SOURCE}")

    with SOURCE.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        fieldnames = list(reader.fieldnames or [])
        for col in ("group", "stage"):
            if col not in fieldnames:
                fieldnames.append(col)
        rows = [row for row in reader if (row.get("season_id") or "").strip() == SEASON_ID]

    for row in rows:
        for col in ("group", "stage"):
            row.setdefault(col, "")

    with OUTPUT.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {OUTPUT.name}")


if __name__ == "__main__":
    main()
