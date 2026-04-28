-- Matches whose stored timeline shows an in-match interruption (abeyance) and a later resume.
--
-- Sportradar marks the interruption with a timeline event:
--   type = period_start AND period_type = interrupted (often period_name = interrupted).
-- Play continuing after that moment is inferred as the next timeline event (by event `time`)
-- strictly after the interruption timestamp, excluding duplicate interrupted markers.
--
-- Output:
--   interrupted_at — when the interrupted period was recorded.
--   resumed_at / resume_* — the first subsequent feed event (often in-play action or period_start
--     for a normal period); for a multi-day restart this timestamp can be days later (e.g. substitution).
--   interruption_gap — wall-clock span between those two timestamps.
--   resumed_later_calendar_day — true when resume date (UTC) is after the interruption date.
--
-- Filter: uncomment the final WHERE to restrict to only pauses that cross a calendar day in UTC.
--
-- Re-run in Supabase: SQL Editor → paste → Run.

select
  t.id as sport_event_timelines_id,
  t.game_id,
  g.sport_event_id,
  g.start_time::date as match_scheduled_date,
  g.home_team,
  g.away_team,
  g.status,
  g.recorded,
  c.sportradar_competition_id,
  c.competition_name,
  (intr.e ->> 'id')::bigint as interrupt_timeline_event_id,
  (intr.e ->> 'time')::timestamptz as interrupted_at,
  intr.e ->> 'period_name' as interrupt_period_name,
  (res.e ->> 'id')::bigint as resume_timeline_event_id,
  (res.e ->> 'time')::timestamptz as resumed_at,
  res.e ->> 'type' as resume_event_type,
  res.e ->> 'period_type' as resume_period_type,
  res.e ->> 'match_time' as resume_match_minute,
  res.e ->> 'match_clock' as resume_match_clock,
  (res.e ->> 'time')::timestamptz - (intr.e ->> 'time')::timestamptz as interruption_gap,
  (res.e ->> 'time')::date > (intr.e ->> 'time')::date as resumed_later_calendar_day
from public.sport_event_timelines t
join public.games g on g.id = t.game_id
left join public.seasons s on s.id = g.season_id
left join public.competitions c on c.id = s.competition_id
cross join lateral (
  select e
  from jsonb_array_elements(coalesce(t.timeline_json -> 'timeline', '[]'::jsonb)) as e
  where e ->> 'type' = 'period_start'
    and e ->> 'period_type' = 'interrupted'
  order by (e ->> 'time')::timestamptz nulls last, (e ->> 'id')::bigint nulls last
  limit 1
) as intr
cross join lateral (
  select e2 as e
  from jsonb_array_elements(coalesce(t.timeline_json -> 'timeline', '[]'::jsonb)) as e2
  where (e2 ->> 'time')::timestamptz > (intr.e ->> 'time')::timestamptz
    and not (
      e2 ->> 'type' = 'period_start'
      and e2 ->> 'period_type' = 'interrupted'
    )
  order by (e2 ->> 'time')::timestamptz nulls last, (e2 ->> 'id')::bigint nulls last
  limit 1
) as res
-- Optional: only multi-day (or later calendar day) restarts in UTC
-- where (res.e ->> 'time')::date > (intr.e ->> 'time')::date
order by interrupted_at desc nulls last, c.competition_name nulls last, g.sport_event_id;
