# Own Goals Tracker — Multi-Competition

Pulls data from the Sportradar Soccer API to identify own goals across one or more configured soccer competitions/seasons and generates a self-contained HTML report.

---

## Project Structure

```
├── config.py                  # API key, competition/season list, file paths
├── step1_fetch_all_games.py      # Step 1: fetch all games via season summaries
├── step2_sync_recorded_flag.py   # Step 2: sync games.recorded from recordings JSON (Supabase)
├── step3_fetch_regular_timelines.py # Step 3: fetch regular timelines for completed matches
├── step4_fetch_extended_timelines.py # Step 4: fetch extended timelines for completed matches
├── step5_extract_own_goals.py # Step 5: extract own goals from regular timelines
├── step6_extract_var_and_shootouts.py # Step 6: build VAR + shootout derived tables
├── step7_generate_reports.py   # Step 7: generate all report HTML files
├── gen_report_recordings_library_html.py # recordings-only report regeneration from export JSON
├── report_navigation.py       # Shared nav bar for all report pages
├── run_all.py                 # Orchestrate Steps 1–7
├── run_extended_timeline_pipeline.py # Extended-only runner (full-backfill/daily)
├── data/
│   ├── schedule.csv           # Matches across configured competitions/seasons
│   ├── own_goals.csv          # Extracted own goal records
│   └── timelines/             # Cached raw JSON from Sportradar (one file per match)
├── report_own_goals.html       # Own goals (primary; nav links to the three below)
├── report_penalty_shootouts.html
├── report_var_events.html
└── report_var_unpaired.html
```

---

## API Details

- **API**: Sportradar Soccer v4 (trial)
- **Configured in**: `config.py` → `COMPETITIONS` list
- **Each entry includes**: `competition_id`, `competition_name`, `season_id`, `season_name`
- **Supabase** (when enabled): `public."Competitions"`, `public."Seasons (current sr:season:ID)"`, `public."All Games (sr:sport_events)"`, `public."Completed Matches - full sport_event_timelines"`, `public."Completed Matches - Extended Timeline"`, `public.own_goals` — see `supabase/migrations/`
- **Own goal detection**: `timeline.type == "score_change" && timeline.method == "own_goal"`

---

## Running the Pipeline

### Prerequisites
- Python 3.12+ (installed via winget)

### One-shot (all steps)
```powershell
$env:PYTHONUTF8="1"
python run_all.py
```

### `run_all.py` step order
1. Fetch all games via season summaries (`step1_fetch_all_games.py`)
2. Add/refresh `recorded` flag in All Games (Supabase only; `step2_sync_recorded_flag.py`)
3. Fetch regular timelines for completed matches (`step3_fetch_regular_timelines.py`)
4. Fetch extended timelines for completed matches (`step4_fetch_extended_timelines.py`)
5. Extract own goals from regular timelines (`step5_extract_own_goals.py`)
6. Build derived tables (VAR + penalty shootouts; `step6_extract_var_and_shootouts.py`)
7. Generate all HTML reports (`step7_generate_reports.py`)

### Individual steps (useful for refreshing data)
```powershell
# Step 1 - Refresh all games only
python step1_fetch_all_games.py

# Step 3 - Fetch any new/missing regular timelines
python step3_fetch_regular_timelines.py

# Step 4 - Extended timelines only (separate runner)
# one-time full backfill for completed games missing extended timeline rows
python run_extended_timeline_pipeline.py --full-backfill
# daily kickoff-window probe for newly completed games
python run_extended_timeline_pipeline.py --daily

# Step 5 - Re-extract own goals from regular timelines
python step5_extract_own_goals.py

# Step 7 - Rebuild report HTML files
python step7_generate_reports.py

# Recordings page only (after updating recordings export JSON)
python gen_report_recordings_library_html.py
```

---

## Rate Limiting

The trial API key allows **1 request/second**. `step3_fetch_regular_timelines.py` enforces a 1.1 second delay between requests. With 261 completed matches, the first run takes ~5 minutes. Subsequent runs only fetch new matches.

---

---

## GitHub Pages & Weekly Email

Reports on GitHub Pages (same folder on `main`):

- [Own goals](https://laneashipley-create.github.io/25-26-Tier-1-Soccer-Database/report_own_goals.html)
- [Penalty shootouts](https://laneashipley-create.github.io/25-26-Tier-1-Soccer-Database/report_penalty_shootouts.html)
- [VAR events](https://laneashipley-create.github.io/25-26-Tier-1-Soccer-Database/report_var_events.html)
- [VAR unpaired review queue](https://laneashipley-create.github.io/25-26-Tier-1-Soccer-Database/report_var_unpaired.html)

The weekly/daily workflows commit these HTML files next to `report_own_goals.html` so Pages serves them automatically.

### Enabling GitHub Pages

1. Repo **Settings** → **Pages**
2. **Source**: Deploy from a branch
3. **Branch**: `main` / root (`/`)
4. **Save** — the report will be live within a few minutes

### Weekly Email (Brevo)

After the weekly workflow runs at `1:07 AM UTC` on Wednesdays, the email arrives Tuesday evening in San Francisco and is sent via Brevo.

**Setup:**
1. Add **Settings** → **Secrets and variables** → **Actions** → **New repository secret**
2. Name: `BREVO_API_KEY`, Value: your [Brevo API key](https://app.brevo.com/settings/keys/api)
3. Ensure `sportsdata@keepstagecoachfree.com` is verified in your Brevo account

---

## Data Notes

- **`data/schedule.csv`**: Includes all 393 matches (261 completed, 132 upcoming as of project start). Statuses: `closed`/`ended` = completed.
- **`data/timelines/`**: Raw JSON cached per match. Filenames are the sport event ID with colons replaced by underscores.
- **`data/own_goals.csv`**: One row per own goal with: player name, team, minute, benefiting team, score at time of OG, final score, commentary text.
- The `og_player_team` field is the team the scorer **plays for** (the unfortunate one); `benefiting_team` is who it counts as a goal for.
