-- Fast list of sport_event_id values with unpaired VAR events.
-- Uses public.var_unpaired_event_matches view.

select
  sport_event_id,
  match_date,
  competition_name,
  home_team,
  away_team,
  video_assistant_referee,
  video_assistant_referee_over,
  unpaired_var_starts
from public.var_unpaired_event_matches
order by
  unpaired_var_starts desc,
  match_date nulls last,
  sport_event_id;
