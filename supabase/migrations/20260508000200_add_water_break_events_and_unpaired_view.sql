-- Water-break event table + unpaired view (mirrors VAR pattern).

create table if not exists public.water_break_timeline_events (
  id uuid primary key default gen_random_uuid(),
  game_id uuid not null references public."All Games (sr:sport_events)"(id) on delete cascade,
  sport_event_id text not null,
  match_date date,
  home_team text,
  away_team text,
  status text,
  recorded boolean,
  sportradar_competition_id text,
  competition_name text,
  timeline_event_id bigint,
  water_break_event_type text not null,
  match_minute int,
  stoppage_minute int,
  match_clock text,
  period_type text,
  created_at timestamptz not null default now(),
  unique(game_id, timeline_event_id, water_break_event_type)
);

create index if not exists idx_water_break_timeline_events_game_id
  on public.water_break_timeline_events(game_id);
create index if not exists idx_water_break_timeline_events_sport_event_id
  on public.water_break_timeline_events(sport_event_id);
create index if not exists idx_water_break_timeline_events_event_type
  on public.water_break_timeline_events(water_break_event_type);

create or replace view public.water_break_unpaired_event_matches
with (security_invoker = true) as
with agg as (
  select
    game_id,
    sport_event_id,
    min(match_date) as match_date,
    min(home_team) as home_team,
    min(away_team) as away_team,
    min(status) as status,
    bool_or(recorded) as recorded,
    min(sportradar_competition_id) as sportradar_competition_id,
    min(competition_name) as competition_name,
    count(*) filter (where water_break_event_type = 'water_break_start') as sport_timeline_water_break_start,
    count(*) filter (where water_break_event_type = 'water_break_end') as sport_timeline_water_break_end
  from public.water_break_timeline_events
  group by game_id, sport_event_id
)
select
  game_id,
  sport_event_id,
  match_date,
  home_team,
  away_team,
  status,
  recorded,
  sportradar_competition_id,
  competition_name,
  sport_timeline_water_break_start,
  sport_timeline_water_break_end,
  (sport_timeline_water_break_start - sport_timeline_water_break_end) as sport_timeline_delta
from agg
where sport_timeline_water_break_start <> sport_timeline_water_break_end
order by match_date nulls last, competition_name nulls last, sport_event_id;
