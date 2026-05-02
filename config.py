"""
Central configuration for the Own Goals project.
"""

import os

# API key: env var for CI, or config_local.py for local dev (gitignored)
try:
    from config_local import API_KEY as _LOCAL_KEY
    API_KEY = _LOCAL_KEY
except ImportError:
    API_KEY = os.environ.get("SPORTRADAR_API_KEY", "")
BASE_URL = "https://api.sportradar.com/soccer/trial/v4/en"

# -----------------------------------------------------------------------------
# Competitions / seasons to monitor
# -----------------------------------------------------------------------------
# Add more entries to this list when you want to monitor additional competitions.
# Each entry must include:
#   - competition_id (sr:competition:...)
#   - competition_name (stored in public."Competitions".competition_name)
#   - season_id (sr:season:...)
#   - season_name (display label)
# Optional (stored on public."Competitions" when present):
#   - gender, country_code (ISO 3166-1 alpha-2 for territory; null for cross-border UEFA/CONMEBOL if you prefer)
#   - category_name — fallback only when no API key: should match Sportradar Competition Info
#     JSON path competition.category.name (same as XML <category name="England" .../>).
#     When SPORTRADAR_API_KEY / config_local is set, db.sync_competitions_from_config() overwrites
#     this from the live API response.
#
# Example:
# {
#     "competition_id": "sr:competition:17",
#     "competition_name": "Premier League",
#     "gender": "male",
#     "category_name": "England",
#     "country_code": "GB",
#     "season_id": "sr:season:130281",
#     "season_name": "Premier League 25/26",
# }
COMPETITIONS = [
    {
        "competition_id": "sr:competition:7",
        "competition_name": "UEFA Champions League",
        "gender": "male",
        "category_name": "Europe",
        "season_id": "sr:season:131129",
        "season_name": "UEFA Champions League 25/26",
    },
    {
        "competition_id": "sr:competition:325",
        "competition_name": "Brasileiro Serie A",
        "gender": "male",
        "category_name": "Brazil",
        "country_code": "BR",
        "season_id": "sr:season:137706",
        "season_name": "Brasileiro Serie A 2026",
    },
    {
        "competition_id": "sr:competition:35",
        "competition_name": "1. Bundesliga",
        "gender": "male",
        "category_name": "Germany",
        "country_code": "DE",
        "season_id": "sr:season:130571",
        "season_name": "Bundesliga 25/26",
    },
    {
        "competition_id": "sr:competition:17",
        "competition_name": "Premier League",
        "gender": "male",
        "category_name": "England",
        "country_code": "GB",
        "season_id": "sr:season:130281",
        "season_name": "Premier League 25/26",
    },
    {
        "competition_id": "sr:competition:196",
        "competition_name": "J.League",
        "gender": "male",
        "category_name": "Japan",
        "country_code": "JP",
        "season_id": "sr:season:138182",
        "season_name": "J1 League 2026",
    },
    {
        "competition_id": "sr:competition:8",
        "competition_name": "LaLiga",
        "gender": "male",
        "category_name": "Spain",
        "country_code": "ES",
        "season_id": "sr:season:130805",
        "season_name": "LaLiga 25/26",
    },
    {
        "competition_id": "sr:competition:34",
        "competition_name": "Ligue 1",
        "gender": "male",
        "category_name": "France",
        "country_code": "FR",
        "season_id": "sr:season:131609",
        "season_name": "Ligue 1 25/26",
    },
    {
        "competition_id": "sr:competition:242",
        "competition_name": "MLS",
        "gender": "male",
        "category_name": "United States",
        "country_code": "US",
        "season_id": "sr:season:137218",
        "season_name": "MLS 2026",
    },
    {
        "competition_id": "sr:competition:23",
        "competition_name": "Serie A",
        "gender": "male",
        "category_name": "Italy",
        "country_code": "IT",
        "season_id": "sr:season:130971",
        "season_name": "Serie A 25/26",
    },
    {
        "competition_id": "sr:competition:679",
        "competition_name": "UEFA Europa League",
        "gender": "male",
        "category_name": "Europe",
        "season_id": "sr:season:131635",
        "season_name": "UEFA Europa League 25/26",
    },
    {
        "competition_id": "sr:competition:34480",
        "competition_name": "UEFA Conference League",
        "gender": "male",
        "category_name": "Europe",
        "season_id": "sr:season:131637",
        "season_name": "UEFA Conference League 25/26",
    },
    {
        "competition_id": "sr:competition:23755",
        "competition_name": "UEFA Nations League",
        "gender": "male",
        "category_name": "Europe",
        "season_id": "sr:season:139802",
        "season_name": "UEFA Nations League 26/27",
    },
    {
        "competition_id": "sr:competition:16",
        "competition_name": "FIFA Men's World Cup",
        "gender": "male",
        "category_name": "International",
        "season_id": "sr:season:101177",
        "season_name": "World Cup 2026",
    },
    {
        "competition_id": "sr:competition:1",
        "competition_name": "UEFA Euro",
        "gender": "male",
        "category_name": "Europe",
        "season_id": "sr:season:137560",
        "season_name": "UEFA Euro 2028",
    },
    {
        "competition_id": "sr:competition:217",
        "competition_name": "DFB Pokal",
        "gender": "male",
        "category_name": "Germany",
        "country_code": "DE",
        "season_id": "sr:season:130937",
        "season_name": "DFB Pokal 25/26",
    },
    {
        "competition_id": "sr:competition:19",
        "competition_name": "FA Cup",
        "gender": "male",
        "category_name": "England",
        "country_code": "GB",
        "season_id": "sr:season:130931",
        "season_name": "FA Cup 25/26",
    },
    {
        "competition_id": "sr:competition:329",
        "competition_name": "Copa del Rey",
        "gender": "male",
        "category_name": "Spain",
        "country_code": "ES",
        "season_id": "sr:season:131970",
        "season_name": "Copa del Rey 25/26",
    },
    {
        "competition_id": "sr:competition:335",
        "competition_name": "Coupe de France",
        "gender": "male",
        "category_name": "France",
        "country_code": "FR",
        "season_id": "sr:season:132930",
        "season_name": "Coupe de France 25/26",
    },
    {
        "competition_id": "sr:competition:328",
        "competition_name": "Coppa Italia",
        "gender": "male",
        "category_name": "Italy",
        "country_code": "IT",
        "season_id": "sr:season:130975",
        "season_name": "Coppa Italia 25/26",
    },
    {
        "competition_id": "sr:competition:346",
        "competition_name": "Community Shield",
        "gender": "male",
        "category_name": "England",
        "country_code": "GB",
        "season_id": "sr:season:130941",
        "season_name": "Community Shield 2025",
    },
    {
        "competition_id": "sr:competition:465",
        "competition_name": "UEFA Super Cup",
        "gender": "male",
        "category_name": "Europe",
        "season_id": "sr:season:131121",
        "season_name": "UEFA Super Cup 2025",
    },
    {
        "competition_id": "sr:competition:384",
        "competition_name": "CONMEBOL Libertadores",
        "gender": "male",
        "category_name": "South America",
        "season_id": "sr:season:137972",
        "season_name": "CONMEBOL Libertadores 2026",
    },
    {
        "competition_id": "sr:competition:27466",
        "competition_name": "Liga MX (Apertura + Clausura)",
        "gender": "male",
        "category_name": "Mexico",
        "country_code": "MX",
        "season_id": "sr:season:137954",
        "season_name": "Liga MX, Clausura 2026",
    },
]

# Backward-compatible single-season aliases (first configured entry)
COMPETITION_ID = COMPETITIONS[0]["competition_id"]
SEASON_ID = COMPETITIONS[0]["season_id"]
SEASON_NAME = COMPETITIONS[0]["season_name"]
SEASON_LABEL = ", ".join(c["season_name"] for c in COMPETITIONS)

# Report hub + page header blurbs (keep in sync with report_hub.html)
REPORT_BLURB_LIST_OF_ALL_GAMES = (
    "Browse the complete schedule for all 24 competitions included in this report"
)
REPORT_BLURB_OWN_GOALS = (
    "Report identifies all own goals that occurred in completed games "
    "(score_change + method=own_goal)."
)
REPORT_BLURB_OWN_GOALS_NOTE = (
    "Note: Commentary only available from 4/20/26 onward due to a database refresh + "
    "commentary being removed from timelines after 14 days"
)
REPORT_BLURB_VAR_EVENTS = (
    "Review all VAR events from completed matches with type, timing, affected side, etc. etc"
)
REPORT_BLURB_VAR_UNPAIRED = (
    "Report identifies matches that have un-paired VAR events, meaning there was a "
    '"video_assistant_referee" event but no corresponding "video_assistant_referee_over" event.'
)
REPORT_BLURB_PENALTY_SHOOTOUTS = (
    "Report identifies all completed matches that ended in a penalty shootout (match_status=ap). "
    "Also calculates total number of penalty shots and if that # is greater than 10, marks the "
    "match as having ended in 'sudden death' penalty shootout"
)
REPORT_BLURB_RECORDINGS_LIBRARY = (
    "See the full list of available soccer recordings with all relevant metadata (competition name, "
    "season ID, match ID), as well as a clear breakdown of which API endpoints are included in the recording"
)

# Output files
SCHEDULE_CSV = "data/schedule.csv"
OWN_GOALS_CSV = "data/own_goals.csv"
TIMELINES_DIR = "data/timelines"       # cached raw JSON responses
REPORT_HTML = "report_own_goals.html"
# Old bookmarks / GitHub Pages links; regenerated as an instant redirect to REPORT_HTML.
REPORT_HTML_LEGACY_REDIRECT = "report.html"
REPORT_HTML_PENALTY_SHOOTOUTS = "report_penalty_shootouts.html"
REPORT_HTML_VAR_EVENTS = "report_var_events.html"
REPORT_HTML_VAR_UNPAIRED = "report_var_unpaired.html"
REPORT_HTML_RECORDINGS_LIBRARY = "report_recordings_library.html"
REPORT_HTML_MASTER_GAMES = "report_master_games.html"

# Rate limiting: Sportradar trial keys are limited to 1 request/second
REQUEST_DELAY_SECONDS = 1.1

# Only fetch timelines for matches with these statuses
COMPLETED_STATUSES = {"closed", "ended"}

# --daily: only consider missing timelines for matches whose kickoff is within this many days.
# Older backlog (e.g. first-time setup) is cleared by the weekly --full-backfill workflow, which
# runs a full schedule sync and all missing timelines.
PIPELINE_RECENT_TIMELINE_DAYS = int(os.environ.get("PIPELINE_RECENT_TIMELINE_DAYS", "14"))

# --daily: refresh Supabase games rows whose kickoff date (UTC) is within ± this many calendar
# days of today (catches newly completed fixtures + imminent kickoff/time changes). Full season
# JSON is still fetched per configured season (API shape); only matching rows are upserted.
# Weekly --full-backfill still writes the canonical data/schedule.csv and upserts every match.
PIPELINE_DAILY_SCHEDULE_WINDOW_DAYS = int(os.environ.get("PIPELINE_DAILY_SCHEDULE_WINDOW_DAYS", "5"))

# GET …/seasons/{id}/schedules.json pagination (Soccer v4). ``limit`` max is 1000; trial keys
# often cap responses smaller — use paginated requests until all rows are retrieved.
PIPELINE_SCHEDULE_PAGE_SIZE = int(os.environ.get("PIPELINE_SCHEDULE_PAGE_SIZE", "100"))

# Supabase — EPL Own Goals project (key from config_local or env)
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://yoesorfzvtbdmvrdtqoo.supabase.co")
try:
    from config_local import SUPABASE_KEY as _LOCAL_SUPABASE_KEY
    SUPABASE_KEY = _LOCAL_SUPABASE_KEY
except (ImportError, AttributeError):
    SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "") or os.environ.get("SUPABASE_ANON_KEY", "")
USE_SUPABASE = bool(SUPABASE_URL and SUPABASE_KEY)
