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
#   - competition_name (stored in public.competitions.competition_name)
#   - season_id (sr:season:...)
#   - season_name (display label)
# Optional (stored on public.competitions when present):
#   - gender, category_name, country_code (ISO 3166-1 alpha-2; null for cross-border UEFA/CONMEBOL)
#
# Example:
# {
#     "competition_id": "sr:competition:17",
#     "competition_name": "Premier League",
#     "gender": "male",
#     "category_name": "1st tier national league",
#     "country_code": "GB",
#     "season_id": "sr:season:130281",
#     "season_name": "Premier League 25/26",
# }
COMPETITIONS = [
    {
        "competition_id": "sr:competition:35",
        "competition_name": "1. Bundesliga",
        "gender": "male",
        "category_name": "1st tier national league",
        "country_code": "DE",
        "season_id": "sr:season:130571",
        "season_name": "Bundesliga 25/26",
    },
    {
        "competition_id": "sr:competition:17",
        "competition_name": "Premier League",
        "gender": "male",
        "category_name": "1st tier national league",
        "country_code": "GB",
        "season_id": "sr:season:130281",
        "season_name": "Premier League 25/26",
    },
    {
        "competition_id": "sr:competition:19",
        "competition_name": "FA Cup",
        "gender": "male",
        "category_name": "National knockout cup",
        "country_code": "GB",
        "season_id": "sr:season:130931",
        "season_name": "FA Cup 25/26",
    },
    {
        "competition_id": "sr:competition:8",
        "competition_name": "LaLiga",
        "gender": "male",
        "category_name": "1st tier national league",
        "country_code": "ES",
        "season_id": "sr:season:130805",
        "season_name": "LaLiga 25/26",
    },
    {
        "competition_id": "sr:competition:34",
        "competition_name": "Ligue 1",
        "gender": "male",
        "category_name": "1st tier national league",
        "country_code": "FR",
        "season_id": "sr:season:131609",
        "season_name": "Ligue 1 25/26",
    },
    {
        "competition_id": "sr:competition:242",
        "competition_name": "MLS",
        "gender": "male",
        "category_name": "1st tier national league",
        "country_code": "US",
        "season_id": "sr:season:137218",
        "season_name": "MLS 2026",
    },
    {
        "competition_id": "sr:competition:23",
        "competition_name": "Serie A",
        "gender": "male",
        "category_name": "1st tier national league",
        "country_code": "IT",
        "season_id": "sr:season:130971",
        "season_name": "Serie A 25/26",
    },
    {
        "competition_id": "sr:competition:679",
        "competition_name": "UEFA Europa League",
        "gender": "male",
        "category_name": "UEFA club competition",
        "season_id": "sr:season:131635",
        "season_name": "UEFA Europa League 25/26",
    },
    {
        "competition_id": "sr:competition:7",
        "competition_name": "UEFA Champions League",
        "gender": "male",
        "category_name": "UEFA club competition",
        "season_id": "sr:season:131129",
        "season_name": "UEFA Champions League 25/26",
    },
    {
        "competition_id": "sr:competition:34480",
        "competition_name": "UEFA Conference League",
        "gender": "male",
        "category_name": "UEFA club competition",
        "season_id": "sr:season:131637",
        "season_name": "UEFA Conference League 25/26",
    },
    {
        "competition_id": "sr:competition:384",
        "competition_name": "CONMEBOL Libertadores",
        "gender": "male",
        "category_name": "CONMEBOL club competition",
        "season_id": "sr:season:137972",
        "season_name": "CONMEBOL Libertadores 2026",
    },
]

# Backward-compatible single-season aliases (first configured entry)
COMPETITION_ID = COMPETITIONS[0]["competition_id"]
SEASON_ID = COMPETITIONS[0]["season_id"]
SEASON_NAME = COMPETITIONS[0]["season_name"]
SEASON_LABEL = ", ".join(c["season_name"] for c in COMPETITIONS)

# Output files
SCHEDULE_CSV = "data/schedule.csv"
OWN_GOALS_CSV = "data/own_goals.csv"
TIMELINES_DIR = "data/timelines"       # cached raw JSON responses
REPORT_HTML = "report.html"

# Rate limiting: Sportradar trial keys are limited to 1 request/second
REQUEST_DELAY_SECONDS = 1.1

# Only fetch timelines for matches with these statuses
COMPLETED_STATUSES = {"closed", "ended"}

# Supabase — EPL Own Goals project (key from config_local or env)
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://yoesorfzvtbdmvrdtqoo.supabase.co")
try:
    from config_local import SUPABASE_KEY as _LOCAL_SUPABASE_KEY
    SUPABASE_KEY = _LOCAL_SUPABASE_KEY
except (ImportError, AttributeError):
    SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "") or os.environ.get("SUPABASE_ANON_KEY", "")
USE_SUPABASE = bool(SUPABASE_URL and SUPABASE_KEY)
