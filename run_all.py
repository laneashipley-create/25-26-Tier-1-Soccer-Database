"""
Master runner — pipeline steps for games, timelines, derived tables, and HTML.

Steps
  1 — Fetch all games via season summaries (step1_fetch_all_games.py)
  2 — Add recorded flag to All Games (Supabase only; step2_sync_recorded_flag.py)
  3 — Fetch regular timelines for completed matches (step3_fetch_regular_timelines.py)
  4 — Fetch extended timelines for completed matches (step4_fetch_extended_timelines.py)
  5 — Extract own goals from completed regular timelines (step5_extract_own_goals.py)
  6 — Build derived tables: VAR + penalty shootouts + water breaks (step6_extract_var_and_shootouts.py)
  7 — Generate HTML reports (step7_generate_reports.py)

Usage
  python run_all.py
  python run_all.py --full-backfill
      Full pipeline (steps 1–7): schedule sync, timelines, derived tables, and HTML.
      ``--full-backfill`` is the explicit flag used by the bi-weekly Actions workflow.

  python run_all.py --daily
      Supabase only. Step 3 reads All Games whose kickoff UTC calendar date falls in
      [today - PIPELINE_DAILY_TIMELINE_KICKOFF_DAYS_BEFORE, today +
      PIPELINE_DAILY_TIMELINE_KICKOFF_DAYS_AFTER] (defaults: 1 day before / 1 day after).
      For each game without a stored timeline yet, calls Sportradar timeline.json; only when
      sport_event_status.status is closed/ended is the timeline written (no season schedule
      fetch — weekly full sync refreshes fixtures).

      If no regular timelines were newly stored, skips steps 5–7. Intended for daily-update.yml.

  python run_all.py --reports-only
      Step 7 only — regenerate HTML from current database (or CSV when not on Supabase).
      Weekly email workflow: pair with export_own_goals_csv.py for data/own_goals.csv.

  python run_all.py --conditional-daily
      Same as --daily (kept for older scripts / workflows).
"""

from __future__ import annotations

import os
import sys

import step1_fetch_all_games
import step3_fetch_regular_timelines
import step4_fetch_extended_timelines
import step5_extract_own_goals
import step6_extract_var_and_shootouts
import step7_generate_reports
from config import SCHEDULE_CSV, USE_SUPABASE

DIVIDER = "-" * 60


def section(title: str) -> None:
    print(f"\n{DIVIDER}")
    print(f"  {title}")
    print(DIVIDER)


def count_pending_timelines() -> int:
    """Completed matches missing a timeline (full list; after a schedule sync)."""
    if USE_SUPABASE:
        import db

        return len(db.get_completed_matches_without_timeline_for_configured_seasons())
    from step3_fetch_regular_timelines import cache_path, load_completed_matches

    matches = load_completed_matches(SCHEDULE_CSV)
    return sum(1 for row in matches if not os.path.exists(cache_path(row["sport_event_id"])))


def count_pending_extended_timelines() -> int:
    """Completed matches missing an extended timeline (Supabase only)."""
    if not USE_SUPABASE:
        return 0
    import db

    return len(db.get_completed_matches_without_extended_timeline_for_configured_seasons())


def _parse_mode() -> str:
    if "--help" in sys.argv or "-h" in sys.argv:
        print(__doc__)
        raise SystemExit(0)
    if "--reports-only" in sys.argv:
        return "reports_only"
    if "--full-backfill" in sys.argv:
        return "full_backfill"
    if "--daily" in sys.argv or "--conditional-daily" in sys.argv:
        return "daily"
    return "full"


def run_main(*, mode: str) -> None:
    if mode == "reports_only":
        section("STEP 6 — Generating HTML reports (reports-only)")
        step7_generate_reports.main()
        print(f"\n{DIVIDER}")
        print("  Reports-only run finished.")
        print(DIVIDER)
        return

    if mode == "daily":
        if not USE_SUPABASE:
            print("ERROR: --daily requires Supabase (SUPABASE_URL + service role key).", flush=True)
            raise SystemExit(1)
        section("STEP 3 — Daily regular timelines (kickoff UTC window from Supabase)")
        stored_regular = step3_fetch_regular_timelines.main_daily_timeline_kickoff_window()
        section("STEP 4 — Daily extended timelines (kickoff UTC window from Supabase)")
        step4_fetch_extended_timelines.main_daily_extended_timeline_kickoff_window()
        if stored_regular == 0:
            print(
                "\nNo new completed regular timelines in the kickoff window — skipping steps 5–7.\n"
                "Weekly --full-backfill picks up schedule changes and older backlog.",
                flush=True,
            )
            print(f"\n{DIVIDER}")
            print("  Daily run done (no new timelines).")
            print(DIVIDER)
            return
        section("STEP 5 — Extracting own goals")
        step5_extract_own_goals.main()
        section("STEP 6 — Extracting VAR + penalty shootout + water-break tables")
        step6_extract_var_and_shootouts.main()
        section("STEP 7 — Generating HTML reports")
        step7_generate_reports.main()
        print(f"\n{DIVIDER}")
        print(
            "  Daily run done. Open report_hub.html or report_own_goals.html "
            "(nav links to all reports including recordings library)."
        )
        print(DIVIDER)
        return

    # full / full_backfill
    section("STEP 1 — Fetching all games via season summaries")
    step1_fetch_all_games.main()

    if USE_SUPABASE:
        section('STEP 2 — Sync "recorded" flag to All Games from recordings JSON')
        import step2_sync_recorded_flag

        step2_sync_recorded_flag.main()

    pending_regular = count_pending_timelines()
    print(f"\nCompleted matches without regular timeline (pending fetch): {pending_regular}")
    pending_extended = count_pending_extended_timelines()
    if USE_SUPABASE:
        print(f"Completed matches without extended timeline (pending fetch): {pending_extended}")

    if pending_regular > 0:
        section("STEP 3 — Fetching regular timelines")
        step3_fetch_regular_timelines.main()
    else:
        print("\nNo completed matches are missing regular timelines — skipping step 3.", flush=True)

    if USE_SUPABASE:
        if pending_extended > 0:
            section("STEP 4 — Fetching extended timelines")
            step4_fetch_extended_timelines.main()
        else:
            print("\nNo completed matches are missing extended timelines — skipping step 4.", flush=True)

    section("STEP 5 — Extracting own goals")
    step5_extract_own_goals.main()

    section("STEP 6 — Extracting VAR + penalty shootout + water-break tables")
    step6_extract_var_and_shootouts.main()

    section("STEP 7 — Generating HTML reports")
    step7_generate_reports.main()

    print(f"\n{DIVIDER}")
    if mode == "full_backfill":
        print(
            "  Full backfill done (steps 1–7 — data + HTML). "
            "Open report_hub.html or report_own_goals.html "
            "(nav links to all reports including recordings library)."
        )
    else:
        print(
            "  All done! Open report_hub.html or report_own_goals.html "
            "(nav links to all reports including recordings library)."
        )
    print(DIVIDER)


if __name__ == "__main__":
    run_main(mode=_parse_mode())
