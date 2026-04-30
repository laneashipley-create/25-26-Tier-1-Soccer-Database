"""
Write data/own_goals.csv from the Supabase own_goals table (same columns as step4).

Used by the weekly email workflow so the CSV matches the DB without re-scanning timelines.
Requires USE_SUPABASE (set SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY in CI).
"""

from __future__ import annotations

import sys

from config import OWN_GOALS_CSV, USE_SUPABASE


def main() -> int:
    if not USE_SUPABASE:
        print("export_own_goals_csv: Supabase not configured.", flush=True)
        return 1
    import db

    out = sys.argv[1] if len(sys.argv) > 1 else OWN_GOALS_CSV
    db.write_own_goals_csv_export(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
