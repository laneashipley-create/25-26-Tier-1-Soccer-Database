-- Store Soccer v4 extended timeline payloads for completed matches.
-- Mirrors public."Completed Matches - full sport_event_timelines" structure/constraints.

create table if not exists public."Completed Matches - Extended Timeline" (
  id uuid primary key default gen_random_uuid(),
  game_id uuid not null references public."All Games (sr:sport_events)"(id) on delete cascade unique,
  fetched_at timestamptz not null default now(),
  timeline_json jsonb,
  sport_event_id text,
  start_time timestamptz
);

create index if not exists idx_completed_matches_extended_timeline_game_id
  on public."Completed Matches - Extended Timeline"(game_id);

create index if not exists idx_completed_matches_extended_timeline_sport_event_id
  on public."Completed Matches - Extended Timeline"(sport_event_id);

create index if not exists idx_completed_matches_extended_timeline_start_time
  on public."Completed Matches - Extended Timeline"(start_time);

update public."Completed Matches - Extended Timeline" t
set sport_event_id = g.sport_event_id
from public."All Games (sr:sport_events)" g
where t.game_id = g.id
  and (t.sport_event_id is null or t.sport_event_id = '');

update public."Completed Matches - Extended Timeline" t
set start_time = g.start_time
from public."All Games (sr:sport_events)" g
where t.game_id = g.id
  and t.start_time is null;
