"""
Master runner — executes all steps in order:
  Step 2: Fetch schedule → data/schedule.csv
  Step 3: Fetch timelines → data/timelines/*.json
  Step 4: Extract own goals → data/own_goals.csv
  Step 5: Extract VAR/shootout event tables (Supabase mode)
  Step 6: Generate HTML reports → own goals + penalty / VAR / VAR-unpaired / recordings library pages

Safe to re-run: timelines are cached, so only new/missing ones are fetched.
"""

import step2_get_schedule
import step3_fetch_timelines
import step4_extract_own_goals
import step5_extract_var_and_shootouts
import generate_report

DIVIDER = "-" * 60

def section(title: str):
    print(f"\n{DIVIDER}")
    print(f"  {title}")
    print(DIVIDER)

if __name__ == "__main__":
    section("STEP 2 — Fetching schedule")
    step2_get_schedule.main()

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
