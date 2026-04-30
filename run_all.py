"""
Master runner — pipeline steps for schedule, timelines, derived tables, and HTML.

Steps
  2 — Fetch Sportradar schedules → data/schedule.csv (+ Supabase games when enabled)
  3 — Fetch timelines for completed matches missing a stored timeline
  4 — Extract own goals → data/own_goals.csv (+ Supabase own_goals)
  5 — VAR + penalty shootout derived tables (Supabase)
  6 — Generate HTML reports (from Supabase or CSV)

Usage
  python run_all.py
      Full pipeline (steps 2–6). For local full refresh.

  python run_all.py --full-backfill
      Steps 2–5 only (no HTML). Use the weekly data workflow: full schedule sync,
      all missing timelines, then derived tables. Run before the fast weekly email job.

  python run_all.py --daily
      Supabase only. Skips step 2 (no Sportradar schedule pull). Step 3 only considers
      completed matches missing a timeline whose kickoff is within the last
      PIPELINE_RECENT_TIMELINE_DAYS days (default 14; override with env). If nothing is
      pending, skips steps 3–6. Intended for daily-update.yml.

      Schedule / status refresh is done by --full-backfill once per week; without that,
      new completions may not appear in games until the next full sync.

  python run_all.py --reports-only
      Step 6 only — regenerate HTML from current database (or CSV when not on Supabase).
      Weekly email workflow: pair with export_own_goals_csv.py for data/own_goals.csv.

  python run_all.py --conditional-daily
      Same as --daily (kept for older scripts / workflows).
"""

from __future__ import annotations

import os
import sys

import generate_report
import step2_get_schedule
import step3_fetch_timelines
import step4_extract_own_goals
import step5_extract_var_and_shootouts
from config import PIPELINE_RECENT_TIMELINE_DAYS, SCHEDULE_CSV, USE_SUPABASE

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
    from step3_fetch_timelines import cache_path, load_completed_matches

    matches = load_completed_matches(SCHEDULE_CSV)
    return sum(1 for row in matches if not os.path.exists(cache_path(row["sport_event_id"])))


def count_pending_timelines_recent(recent_start_days: int) -> int:
    """Like count_pending_timelines(), but restricted to recent kickoffs (Supabase daily mode)."""
    if not USE_SUPABASE:
        return count_pending_timelines()
    import db

    return len(
        db.get_completed_matches_without_timeline_for_configured_seasons_recent(
            recent_start_days=recent_start_days,
        )
    )


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
        generate_report.main()
        print(f"\n{DIVIDER}")
        print("  Reports-only run finished.")
        print(DIVIDER)
        return

    if mode == "daily":
        if not USE_SUPABASE:
            print("ERROR: --daily requires Supabase (SUPABASE_URL + service role key).", flush=True)
            raise SystemExit(1)
        pending = count_pending_timelines_recent(PIPELINE_RECENT_TIMELINE_DAYS)
        print(
            f"\nDaily mode: skipping schedule fetch (step 2). "
            f"Missing timelines (kickoff within last {PIPELINE_RECENT_TIMELINE_DAYS} d): {pending}",
            flush=True,
        )
        if pending == 0:
            print(
                "\nNo pending timelines in the recent window — skipping steps 3–6.\n"
                "Run weekly --full-backfill if you need a full schedule sync or older backlog.",
                flush=True,
            )
            print(f"\n{DIVIDER}")
            print("  Daily run done (no work).")
            print(DIVIDER)
            return
        section("STEP 3 — Fetching timelines (recent window)")
        step3_fetch_timelines.main(recent_start_days=PIPELINE_RECENT_TIMELINE_DAYS)
        section("STEP 4 — Extracting own goals")
        step4_extract_own_goals.main()
        section("STEP 5 — Extracting VAR + penalty shootout tables")
        step5_extract_var_and_shootouts.main()
        section("STEP 6 — Generating HTML reports")
        generate_report.main()
        print(f"\n{DIVIDER}")
        print(
            "  Daily run done. Open report_hub.html or report_own_goals.html "
            "(nav links to all reports including recordings library)."
        )
        print(DIVIDER)
        return

    # full / full_backfill
    section("STEP 2 — Fetching schedule")
    step2_get_schedule.main()

    pending = count_pending_timelines()
    print(f"\nCompleted matches without timeline (pending fetch): {pending}")

    if pending > 0:
        section("STEP 3 — Fetching timelines (cached)")
        step3_fetch_timelines.main()
    else:
        print("\nNo completed matches are missing timelines — skipping step 3.", flush=True)

    section("STEP 4 — Extracting own goals")
    step4_extract_own_goals.main()

    section("STEP 5 — Extracting VAR + penalty shootout tables")
    step5_extract_var_and_shootouts.main()

    if mode == "full_backfill":
        print(f"\n{DIVIDER}")
        print("  Full backfill done (steps 2–5; no HTML — use --reports-only or weekly email job).")
        print(DIVIDER)
        return

    section("STEP 6 — Generating HTML reports")
    generate_report.main()

    print(f"\n{DIVIDER}")
    print(
        "  All done! Open report_hub.html or report_own_goals.html "
        "(nav links to all reports including recordings library)."
    )
    print(DIVIDER)


if __name__ == "__main__":
    run_main(mode=_parse_mode())
