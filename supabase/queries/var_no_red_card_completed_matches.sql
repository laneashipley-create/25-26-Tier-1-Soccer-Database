-- VAR no-red-card reviews for completed matches with stored timelines.
--
-- Returns BOTH events for each matching review:
--   1) video_assistant_referee      (start of review)
--   2) video_assistant_referee_over (end of review with description = no_red_card)
--
-- The start row is the immediately preceding timeline event before the finish row.

with timeline_events as (
  select
    t.game_id,
    g.sport_event_id,
    g.start_time,
    g.start_time::date as match_date,
    g.home_team,
    g.away_team,
    g.status,
    g.recorded,
    c.sportradar_competition_id,
    c.competition_name,
    (e ->> 'id')::bigint as timeline_event_id,
    e ->> 'type' as event_type,
    nullif(trim(e ->> 'description'), '') as description,
    (e ->> 'match_time')::int as match_minute,
    nullif(e ->> 'stoppage_time', '')::int as stoppage_minute,
    e ->> 'match_clock' as match_clock,
    e ->> 'period_type' as period_type,
    e ->> 'competitor' as competitor_side,
    case e ->> 'competitor'
      when 'home' then g.home_team
      when 'away' then g.away_team
      else null
    end as affected_team,
    coalesce(e -> 'commentaries' -> 0 ->> 'text', '') as commentary,
    row_number() over (
      partition by t.game_id
      order by
        (e ->> 'match_time')::int nulls last,
        nullif(e ->> 'stoppage_time', '')::int nulls last,
        (e ->> 'id')::bigint nulls last
    ) as seq_in_match
  from public."Completed Matches - full sport_event_timelines" t
  join public."All Games (sr:sport_events)" g on g.id = t.game_id
  left join public."Seasons (current sr:season:ID)" s on s.id = g.season_id
  left join public."Competitions" c on c.id = s.competition_id
  cross join lateral jsonb_array_elements(coalesce(t.timeline_json -> 'timeline', '[]'::jsonb)) as e
  where g.status in ('closed', 'ended')
    and e ->> 'type' in ('video_assistant_referee', 'video_assistant_referee_over')
),
matched_reviews as (
  select
    game_id,
    timeline_event_id as finish_timeline_event_id,
    seq_in_match as finish_seq
  from timeline_events
  where event_type = 'video_assistant_referee_over'
    and lower(coalesce(description, '')) = 'no_red_card'
)
select
  te.game_id,
  te.sport_event_id,
  te.match_date,
  te.home_team,
  te.away_team,
  te.status,
  te.recorded,
  te.sportradar_competition_id,
  te.competition_name,
  mr.finish_timeline_event_id as var_review_finish_event_id,
  te.timeline_event_id,
  te.event_type,
  te.description,
  te.match_minute,
  te.stoppage_minute,
  te.match_clock,
  te.period_type,
  te.competitor_side,
  te.affected_team,
  te.commentary
from timeline_events te
join matched_reviews mr on mr.game_id = te.game_id
where
  (
    te.seq_in_match = mr.finish_seq
    and te.event_type = 'video_assistant_referee_over'
  )
  or
  (
    te.seq_in_match = mr.finish_seq - 1
    and te.event_type = 'video_assistant_referee'
  )
order by
  te.competition_name nulls last,
  te.start_time nulls last,
  te.sport_event_id,
  mr.finish_timeline_event_id,
  te.seq_in_match;
