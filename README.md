# EPL Own Goals Tracker — 2025/26 Season

Pulls data from the Sportradar Soccer API to identify every own goal scored in the 2025/26 English Premier League season and generates a self-contained HTML report.

---

## Project Structure

```
├── config.py                  # API key, season ID, file paths
├── step2_get_schedule.py      # Fetch full EPL schedule → data/schedule.csv
├── step3_fetch_timelines.py   # Fetch match timelines (cached) → data/timelines/
├── step4_extract_own_goals.py # Scan timelines, extract OG events → data/own_goals.csv
├── generate_report.py         # Build HTML report → report.html
├── run_all.py                 # Orchestrate all steps in sequence
├── data/
│   ├── schedule.csv           # All 393 EPL matches with status/scores
│   ├── own_goals.csv          # Extracted own goal records
│   └── timelines/             # Cached raw JSON from Sportradar (one file per match)
└── report.html                # Final output — open in browser
```

---

## API Details

- **API**: Sportradar Soccer v4 (trial)
- **Season**: `sr:season:130281` — Premier League 25/26
- **Competition**: `sr:competition:17` — English Premier League
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

### Individual steps (useful for refreshing data)
```powershell
# Refresh schedule only
python step2_get_schedule.py

# Fetch any new/missing timelines (safe to re-run — already-cached files are skipped)
python step3_fetch_timelines.py

# Re-extract own goals from cached timelines
python step4_extract_own_goals.py

# Rebuild HTML report
python generate_report.py
```

---

## Rate Limiting

The trial API key allows **1 request/second**. `step3_fetch_timelines.py` enforces a 1.1 second delay between requests. With 261 completed matches, the first run takes ~5 minutes. Subsequent runs only fetch new matches.

---

---

## GitHub Pages & Weekly Email

The report is hosted at **https://laneashipley-create.github.io/25-26-EPL_own.goals/report.html**.

### Enabling GitHub Pages

1. Repo **Settings** → **Pages**
2. **Source**: Deploy from a branch
3. **Branch**: `main` / root (`/`)
4. **Save** — the report will be live within a few minutes

### Weekly Email (Brevo)

After the weekly workflow runs at `1:00 AM UTC` on Wednesdays, the email arrives Tuesday evening in San Francisco and is sent via Brevo.

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
