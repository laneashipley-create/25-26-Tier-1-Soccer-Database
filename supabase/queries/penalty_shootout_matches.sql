-- Penalty-shootout matches from stored timelines (Sportradar: match ended after penalties).
--
-- Rules:
--   match_status = 'ap' on timeline_json.sport_event_status (same as live "after penalties").
--   shootout_attempts: count of timeline events type=penalty_shootout, period_type=penalties.
--   sudden_death: yes if attempts > 10 (IFAB-style first phase max 10 kicks); unknown if no such events (sparse feed).
--   recorded: from public.games — true when the match is flagged as having recording/simulations.
--
-- Uses LEFT JOIN to seasons/competitions so rows are not dropped when FK data is missing
-- (inner join would undercount). Re-run in Supabase: SQL Editor → paste → Run.

select
  t.game_id,
  g.sport_event_id,
  g.start_time::date as match_date,
  g.home_team,
  g.away_team,
  g.status,
  g.recorded,
  c.sportradar_competition_id as sportradar_competition_id,
  c.competition_name as competition_name,
  coalesce(ps.attempts, 0) as shootout_attempts,
  case
    when ps.attempts is null then 'unknown'
    when ps.attempts > 10 then 'yes'
    else 'no'
  end as sudden_death
from public.sport_event_timelines t
join public.games g on g.id = t.game_id
left join public.seasons s on s.id = g.season_id
left join public.competitions c on c.id = s.competition_id
left join lateral (
  select count(*)::int as attempts
  from jsonb_array_elements(coalesce(t.timeline_json -> 'timeline', '[]'::jsonb)) e
  where e ->> 'type' = 'penalty_shootout'
    and e ->> 'period_type' = 'penalties'
) ps on true
where (t.timeline_json #>> '{sport_event_status,match_status}') = 'ap'
order by c.competition_name nulls last, g.start_time nulls last, g.sport_event_id;
