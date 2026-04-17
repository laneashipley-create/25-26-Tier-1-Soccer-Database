-- Normalize competitions, rename schedule → games, match_timelines → sport_event_timelines.
-- Idempotent: safe to re-run after partial apply.

-- ---------------------------------------------------------------------------
-- 1) competitions
-- ---------------------------------------------------------------------------
create table if not exists public.competitions (
  id uuid primary key default gen_random_uuid(),
  sportradar_competition_id text not null unique,
  name text not null,
  created_at timestamptz not null default now()
);

insert into public.competitions (sportradar_competition_id, name) values
  ('sr:competition:35', '1. Bundesliga'),
  ('sr:competition:17', 'Premier League'),
  ('sr:competition:19', 'FA Cup'),
  ('sr:competition:8', 'LaLiga'),
  ('sr:competition:34', 'Ligue 1'),
  ('sr:competition:242', 'MLS'),
  ('sr:competition:23', 'Serie A'),
  ('sr:competition:679', 'UEFA Europa League'),
  ('sr:competition:7', 'UEFA Champions League'),
  ('sr:competition:34480', 'UEFA Conference League'),
  ('sr:competition:384', 'CONMEBOL Libertadores')
on conflict (sportradar_competition_id) do nothing;

-- ---------------------------------------------------------------------------
-- 2) seasons: competition_id text → uuid FK → public.competitions(id)
-- ---------------------------------------------------------------------------
do $$
begin
  if exists (
    select 1
    from information_schema.columns c
    where c.table_schema = 'public'
      and c.table_name = 'seasons'
      and c.column_name = 'competition_id'
      and c.data_type = 'text'
  ) then
    insert into public.competitions (sportradar_competition_id, name)
    select distinct s.competition_id, s.competition_id
    from public.seasons s
    where s.competition_id is not null
      and not exists (
        select 1 from public.competitions c2
        where c2.sportradar_competition_id = s.competition_id
      );

    alter table public.seasons add column if not exists _competition_uuid uuid
      references public.competitions(id);

    update public.seasons s
    set _competition_uuid = c.id
    from public.competitions c
    where c.sportradar_competition_id = s.competition_id;

    alter table public.seasons alter column _competition_uuid set not null;

    alter table public.seasons drop column competition_id;
    alter table public.seasons rename column _competition_uuid to competition_id;
  end if;
end $$;

-- ---------------------------------------------------------------------------
-- 3) schedule → games (parent of timelines)
-- ---------------------------------------------------------------------------
do $$
begin
  if exists (
    select 1 from information_schema.tables
    where table_schema = 'public' and table_name = 'schedule'
  ) then
    alter table public.schedule rename to games;
  end if;
end $$;

-- Rename indexes on games (names stay as idx_schedule_* until renamed)
do $$
begin
  if exists (select 1 from pg_class c join pg_namespace n on n.oid = c.relnamespace where n.nspname = 'public' and c.relname = 'idx_schedule_season_id') then
    alter index public.idx_schedule_season_id rename to idx_games_season_id;
  end if;
  if exists (select 1 from pg_class c join pg_namespace n on n.oid = c.relnamespace where n.nspname = 'public' and c.relname = 'idx_schedule_status') then
    alter index public.idx_schedule_status rename to idx_games_status;
  end if;
  if exists (select 1 from pg_class c join pg_namespace n on n.oid = c.relnamespace where n.nspname = 'public' and c.relname = 'idx_schedule_sport_event_id') then
    alter index public.idx_schedule_sport_event_id rename to idx_games_sport_event_id;
  end if;
end $$;

-- ---------------------------------------------------------------------------
-- 4) match_timelines: schedule_id → game_id, table → sport_event_timelines
-- ---------------------------------------------------------------------------
do $$
begin
  if exists (
    select 1 from information_schema.tables
    where table_schema = 'public' and table_name = 'match_timelines'
  ) then
    if exists (
      select 1 from information_schema.columns
      where table_schema = 'public' and table_name = 'match_timelines' and column_name = 'schedule_id'
    ) then
      alter table public.match_timelines rename column schedule_id to game_id;
    end if;
    alter table public.match_timelines rename to sport_event_timelines;
  end if;
end $$;

do $$
begin
  if exists (select 1 from pg_class c join pg_namespace n on n.oid = c.relnamespace where n.nspname = 'public' and c.relname = 'idx_match_timelines_schedule_id') then
    alter index public.idx_match_timelines_schedule_id rename to idx_sport_event_timelines_game_id;
  end if;
end $$;

-- ---------------------------------------------------------------------------
-- 5) RLS on competitions (service role bypasses; matches other tables)
-- ---------------------------------------------------------------------------
alter table public.competitions enable row level security;
alter table public.competitions force row level security;
