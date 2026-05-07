"""
Runner for the Completed Matches - Extended Timeline dataset.

Usage
  python run_extended_timeline_pipeline.py --full-backfill
      One-time backfill of all completed matches missing an extended timeline row.

  python run_extended_timeline_pipeline.py --daily
      Daily kickoff-window load: probes nearby matches and stores newly completed fixtures.
"""

from __future__ import annotations

import sys

import step3_fetch_extended_timelines
from config import USE_SUPABASE


def _parse_mode() -> str:
    if "--help" in sys.argv or "-h" in sys.argv:
        print(__doc__)
        raise SystemExit(0)
    if "--daily" in sys.argv:
        return "daily"
    if "--full-backfill" in sys.argv:
        return "full_backfill"
    raise SystemExit("Pass one mode: --full-backfill or --daily")


def main() -> None:
    if not USE_SUPABASE:
        raise SystemExit("Supabase is required (SUPABASE_URL + SUPABASE key).")

    mode = _parse_mode()
    if mode == "daily":
        step3_fetch_extended_timelines.main_daily_extended_timeline_kickoff_window()
        return
    step3_fetch_extended_timelines.main()


if __name__ == "__main__":
    main()
