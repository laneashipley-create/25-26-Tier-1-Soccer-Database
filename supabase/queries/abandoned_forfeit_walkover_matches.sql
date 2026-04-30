-- Likely abandoned / cancelled / forfeited / walkover matches.
--
-- This is a heuristic audit query, because there is not a single dedicated
-- "forfeit" flag in the current schema.
--
-- It surfaces matches when any of the following are true:
--   1) public."All Games (sr:sport_events)".status is already abnormal (cancelled, postponed)
--   2) public."All Games (sr:sport_events)".match_status is abnormal (if populated later)
--   3) timeline text/commentary contains keywords like abandoned / forfeit / walkover
--   4) the final score is 3-0 or 0-3 AND the stored timeline is sparse / unusual
--
-- Notes:
--   - A plain 3-0 score alone is NOT proof of a walkover, so this query keeps it
--     as a heuristic and labels why the row matched.
--   - Includes recorded so you can see whether the match is one of your tracked
--     recording/simulation fixtures.
--
-- Re-run in Supabase: SQL Editor -> paste -> Run.

with timeline_stats as (
  select
    t.game_id,
    count(*)::int as timeline_event_count,
    count(*) filter (
      where e ->> 'match_time' is not null
    )::int as in_play_event_count,
    min((e ->> 'time')::timestamptz) as first_timeline_at,
    max((e ->> 'time')::timestamptz) as last_timeline_at,
    string_agg(
      distinct nullif(e ->> 'type', ''),
      ', ' order by nullif(e ->> 'type', '')
    ) as timeline_event_types
  from public."Completed Matches - full sport_event_timelines" t
  cross join lateral jsonb_array_elements(coalesce(t.timeline_json -> 'timeline', '[]'::jsonb)) as e
  group by t.game_id
),
keyword_hits as (
  select
    t.game_id,
    count(*)::int as keyword_event_count,
    string_agg(
      distinct e ->> 'type',
      ', ' order by e ->> 'type'
    ) as keyword_event_types,
    min((e ->> 'time')::timestamptz) as first_keyword_at,
    string_agg(
      distinct left(
        coalesce(e ->> 'description', e -> 'commentaries' -> 0 ->> 'text', ''),
        160
      ),
      ' || ' order by left(
        coalesce(e ->> 'description', e -> 'commentaries' -> 0 ->> 'text', ''),
        160
      )
    ) as keyword_examples
  from public."Completed Matches - full sport_event_timelines" t
  cross join lateral jsonb_array_elements(coalesce(t.timeline_json -> 'timeline', '[]'::jsonb)) as e
  where lower(e::text) similar to '%(abandon|forfeit|walkover|walk_over|cancel|postpon|suspend|awarded)%'
  group by t.game_id
)
select
  g.id as game_id,
  g.sport_event_id,
  g.start_time::date as match_date,
  g.home_team,
  g.away_team,
  g.status,
  g.match_status,
  g.home_score,
  g.away_score,
  g.recorded,
  c.sportradar_competition_id,
  c.competition_name,
  ts.timeline_event_count,
  ts.in_play_event_count,
  ts.first_timeline_at,
  ts.last_timeline_at,
  ts.timeline_event_types,
  kh.keyword_event_count,
  kh.keyword_event_types,
  kh.first_keyword_at,
  kh.keyword_examples,
  case
    when g.status in ('cancelled', 'postponed') then 'games.status abnormal'
    when coalesce(g.match_status, '') not in ('', 'not_started', 'ended', 'aet', 'ap')
      then 'games.match_status abnormal'
    when kh.keyword_event_count > 0 then 'timeline keyword hit'
    when (g.home_score = 3 and g.away_score = 0) or (g.home_score = 0 and g.away_score = 3)
      then '3-0 score with sparse/odd timeline'
    else 'review'
  end as suspected_reason
from public."All Games (sr:sport_events)" g
left join public."Completed Matches - full sport_event_timelines" t on t.game_id = g.id
left join public."Seasons (current sr:season:ID)" s on s.id = g.season_id
left join public."Competitions" c on c.id = s.competition_id
left join timeline_stats ts on ts.game_id = g.id
left join keyword_hits kh on kh.game_id = g.id
where
  g.status in ('cancelled', 'postponed')
  or coalesce(g.match_status, '') not in ('', 'not_started', 'ended', 'aet', 'ap')
  or kh.keyword_event_count > 0
  or (
    ((g.home_score = 3 and g.away_score = 0) or (g.home_score = 0 and g.away_score = 3))
    and (
      coalesce(ts.in_play_event_count, 0) = 0
      or coalesce(ts.timeline_event_count, 0) <= 5
      or kh.keyword_event_count > 0
    )
  )
order by
  g.start_time nulls last,
  c.competition_name nulls last,
  g.sport_event_id;
