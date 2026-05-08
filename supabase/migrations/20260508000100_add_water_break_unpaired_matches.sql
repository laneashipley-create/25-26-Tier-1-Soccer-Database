-- Derived table: per-match water-break count deltas across regular + extended timelines.

create table if not exists public.water_break_unpaired_matches (
  id uuid primary key default gen_random_uuid(),
  game_id uuid not null references public.games(id) on delete cascade unique,
  sport_event_id text not null,
  match_date date,
  home_team text,
  away_team text,
  status text,
  recorded boolean,
  sportradar_competition_id text,
  competition_name text,
  sport_timeline_water_break_start int not null default 0,
  sport_timeline_water_break_end int not null default 0,
  sport_timeline_delta int not null default 0,
  sport_event_extended_timeline_water_break_start int not null default 0,
  sport_event_extended_timeline_water_break_end int not null default 0,
  sport_event_extended_timeline_delta int not null default 0,
  created_at timestamptz not null default now()
);

create index if not exists idx_water_break_unpaired_matches_match_date
  on public.water_break_unpaired_matches(match_date);
create index if not exists idx_water_break_unpaired_matches_sport_event_id
  on public.water_break_unpaired_matches(sport_event_id);
create index if not exists idx_water_break_unpaired_matches_regular_delta
  on public.water_break_unpaired_matches(sport_timeline_delta);
create index if not exists idx_water_break_unpaired_matches_extended_delta
  on public.water_break_unpaired_matches(sport_event_extended_timeline_delta);
