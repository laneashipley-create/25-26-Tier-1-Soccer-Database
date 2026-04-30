"""
Master runner — executes all steps in order:
  Step 2: Fetch schedule → data/schedule.csv (+ Supabase games when enabled)
  Step 3: Fetch timelines for completed matches that do not have one yet
  Step 4: Extract own goals → data/own_goals.csv
  Step 5: Extract VAR/shootout event tables (Supabase mode)
  Step 6: Generate HTML reports → own goals + penalty / VAR / VAR-unpaired / recordings library pages

Usage:
  python run_all.py
      Always runs steps 2–6 (used by weekly email job and local full refresh).

  python run_all.py --conditional-daily
      After step 2, counts completed matches missing timelines; if that count is 0,
      skips steps 3–6. Used by daily-update.yml so we only pull timelines and
      rebuild downstream data when there is new completed-match work.

Safe to re-run: timelines are cached, so only new/missing ones are fetched.
"""

from __future__ import annotations

import os
import sys

import step2_get_schedule
import step3_fetch_timelines
import step4_extract_own_goals
import step5_extract_var_and_shootouts
import generate_report
from config import SCHEDULE_CSV, USE_SUPABASE

DIVIDER = "-" * 60


def section(title: str):
    print(f"\n{DIVIDER}")
    print(f"  {title}")
    print(DIVIDER)


def count_pending_timelines() -> int:
    """How many completed matches still need a timeline row (after latest schedule sync)."""
    if USE_SUPABASE:
        import db

        return len(db.get_completed_matches_without_timeline_for_configured_seasons())
    from step3_fetch_timelines import cache_path, load_completed_matches

    matches = load_completed_matches(SCHEDULE_CSV)
    return sum(1 for row in matches if not os.path.exists(cache_path(row["sport_event_id"])))


def run_main(*, conditional_daily: bool) -> None:
    section("STEP 2 — Fetching schedule")
    step2_get_schedule.main()

    pending = count_pending_timelines()
    print(f"\nCompleted matches without timeline (pending fetch): {pending}")

    if conditional_daily and pending == 0:
        print(
            "\nConditional daily mode: no completed matches are missing timelines. "
            "Skipping steps 3–6 (no timeline API pulls, no derived table rebuild, no HTML regen)."
        )
        print(f"\n{DIVIDER}")
        print("  Partial run done (schedule / games refresh only).")
        print(DIVIDER)
        return

    section("STEP 3 — Fetching timelines (cached)")
    step3_fetch_timelines.main()

    section("STEP 4 — Extracting own goals")
    step4_extract_own_goals.main()

    section("STEP 5 — Extracting VAR + penalty shootout tables")
    step5_extract_var_and_shootouts.main()

    section("STEP 6 — Generating HTML reports")
    generate_report.main()

    print(f"\n{DIVIDER}")
    print(
        "  All done! Open report_hub.html or report_own_goals.html "
        "(nav links to all reports including recordings library)."
    )
    print(DIVIDER)


if __name__ == "__main__":
    if "--help" in sys.argv or "-h" in sys.argv:
        print(__doc__)
        raise SystemExit(0)
    conditional = "--conditional-daily" in sys.argv
    run_main(conditional_daily=conditional)
