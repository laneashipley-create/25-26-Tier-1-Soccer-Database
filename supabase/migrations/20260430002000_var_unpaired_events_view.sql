-- View: matches with unpaired VAR events in public.var_timeline_events.
-- "Unpaired" here means counts of starts vs overs are not equal for a sport_event_id.

create or replace view public.var_unpaired_event_matches
with (security_invoker = true) as
with agg as (
  select
    game_id,
    sport_event_id,
    min(match_date) as match_date,
    min(home_team) as home_team,
    min(away_team) as away_team,
    min(competition_name) as competition_name,
    count(*) filter (where var_event_type = 'video_assistant_referee') as video_assistant_referee,
    count(*) filter (where var_event_type = 'video_assistant_referee_over') as video_assistant_referee_over
  from public.var_timeline_events
  group by game_id, sport_event_id
)
select
  game_id,
  sport_event_id,
  match_date,
  competition_name,
  home_team,
  away_team,
  video_assistant_referee,
  video_assistant_referee_over,
  (video_assistant_referee - video_assistant_referee_over) as unpaired_var_starts
from agg
where video_assistant_referee <> video_assistant_referee_over
order by match_date nulls last, competition_name nulls last, sport_event_id;
