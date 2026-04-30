# Supabase setup for EPL Own Goals 25/26

## MCP access

**You already have access.** The project’s `mcps` folder (under `.cursor/projects/...`) contains the Supabase MCP and other user MCPs. You do **not** need to copy a master MCP file from `C:\Users\l.shipley\.cursor` into this project.

- **Supabase MCP** is available as `user-supabase` and exposes tools such as:
  - `execute_sql`, `apply_migration`, `list_tables`, `list_migrations`, `create_branch`, etc.
- All Supabase tools take a **`project_id`** argument. That value comes from **linking your Supabase project in Cursor** (e.g. Cursor Settings → Supabase → Link project). Once linked, the agent can use `project_id` when calling these tools.

## Applying the schema

1. **Link your Supabase project in Cursor** (if not already) so `project_id` is available.
2. Apply migrations in order (baseline then renames):
   - `supabase/migrations/20250306000000_epl_own_goals_schema.sql`
   - `supabase/migrations/20260318001000_epl_own_goals_enable_rls.sql` (optional but recommended)
   - `supabase/migrations/20260417140000_competitions_games_sport_event_timelines.sql` — adds `public.competitions`, converts `seasons.competition_id` to a UUID FK, renames `schedule` → `games`, `match_timelines` → `sport_event_timelines`, `schedule_id` → `game_id`.
   - `supabase/migrations/20260418120000_competitions_columns_expand.sql` — renames `competitions.name` → `competition_name`, adds `gender`, `category_name`, `country_code`.
   - `supabase/migrations/20260430004000_rename_core_tables_to_supabase_display_names.sql` — optional final rename to dashboard labels: `Competitions`, `Seasons (current sr:season:ID)`, `All Games (sr:sport_events)`, `Completed Matches - full sport_event_timelines` (idempotent if already renamed).

   Use **Supabase SQL Editor** or MCP `apply_migration` for each file’s contents.

## Runtime (script) — reducing run time

The pipeline will be updated to:

1. **Competitions** — Rows in `public."Competitions"` (`sportradar_competition_id`, `competition_name`, optional `gender`, `category_name`, `country_code`). Each run that resolves seasons calls **`sync_competitions_from_config()`** (upsert from `config.COMPETITIONS`) so adds/edits in config propagate automatically. **`category_name`** is set from Sportradar **Competition Info** (`competition.category.name`, same as XML `<category name="…"/>`) when an API key is configured; otherwise it uses the optional `category_name` on each config entry. Rows removed from config are **not** deleted in Supabase (seasons may still reference them).
2. **Seasons** — Ensure each configured season exists in `public."Seasons (current sr:season:ID)"` (by `sportradar_season_id`), linked to `Competitions` via UUID `competition_id`.
3. **Games** — Fetch schedules from Sportradar, then **upsert** into `public."All Games (sr:sport_events)"` (one row per `sr:sport_event` per season).
4. **Timelines** — For completed games, fetch timelines only when there is no row in `public."Completed Matches - full sport_event_timelines"` for that game row’s `id` (`game_id`), unless using local JSON cache.
5. **Own goals** — Insert into `public.own_goals` from timeline data; the report reads from the DB when `USE_SUPABASE` is true.

To do this from Python, the script will use the **Supabase client** with credentials from the environment (not the MCP, which is for Cursor/agent use). Add to `.env` or `config_local.py` (gitignored):

- `SUPABASE_URL` — project URL (e.g. `https://xxxx.supabase.co`)
- `SUPABASE_SERVICE_ROLE_KEY` or `SUPABASE_ANON_KEY` — for server-side or anon access

Then install and use:

```bash
pip install supabase
```

The next step is to add Supabase URL/key to `config.py` and implement the DB-backed versions of step 2, 3, and 4 (and optionally the report from DB).
