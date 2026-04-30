-- Derived event tables from public.sport_event_timelines (similar pattern to public.own_goals).

create table if not exists public.penalty_shootout_matches (
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
  shootout_attempts int not null default 0,
  sudden_death boolean not null default false,
  created_at timestamptz not null default now()
);

create index if not exists idx_penalty_shootout_matches_match_date
  on public.penalty_shootout_matches(match_date);
create index if not exists idx_penalty_shootout_matches_sport_event_id
  on public.penalty_shootout_matches(sport_event_id);

create table if not exists public.var_timeline_events (
  id uuid primary key default gen_random_uuid(),
  game_id uuid not null references public.games(id) on delete cascade,
  sport_event_id text not null,
  match_date date,
  home_team text,
  away_team text,
  status text,
  recorded boolean,
  sportradar_competition_id text,
  competition_name text,
  timeline_event_id bigint,
  var_event_type text not null,
  description text,
  decision text,
  match_minute int,
  stoppage_minute int,
  match_clock text,
  period_type text,
  competitor_side text,
  affected_team text,
  commentary text,
  created_at timestamptz not null default now(),
  unique(game_id, timeline_event_id, var_event_type)
);

create index if not exists idx_var_timeline_events_game_id
  on public.var_timeline_events(game_id);
create index if not exists idx_var_timeline_events_sport_event_id
  on public.var_timeline_events(sport_event_id);
create index if not exists idx_var_timeline_events_event_type
  on public.var_timeline_events(var_event_type);
