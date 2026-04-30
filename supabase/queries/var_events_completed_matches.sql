-- VAR timeline events for completed matches that have stored timelines.
--
-- Captures both:
--   type = video_assistant_referee
--   type = video_assistant_referee_over
--
-- Includes:
--   description  -> VAR outcome (goal, no_goal, penalty, no_penalty, red_card, no_red_card);
--                  on video_assistant_referee_over this is usually where the result lives.
--   decision     -> Documented for VAR on Soccer *Extended* extended_timeline.json
--                  (pending, cancelled, upheld, overturned). Standard sport_events/.../timeline.json
--                  (what step3_fetch_timelines.py stores) typically omits `decision` on VAR events,
--                  including video_assistant_referee_over — so this column is often NULL here.
--   recorded     -> public."All Games (sr:sport_events)" flag for your recording/simulation tracking
--
-- One row per VAR event.
-- Re-run in Supabase: SQL Editor -> paste -> Run.

select
  t.game_id,
  g.sport_event_id,
  g.start_time::date as match_date,
  g.home_team,
  g.away_team,
  g.status,
  g.recorded,
  c.sportradar_competition_id,
  c.competition_name,
  (e ->> 'id')::bigint as timeline_event_id,
  e ->> 'type' as var_event_type,
  nullif(trim(e ->> 'description'), '') as description,
  nullif(trim(e ->> 'decision'), '') as decision,
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
  coalesce(e -> 'commentaries' -> 0 ->> 'text', '') as commentary
from public."Completed Matches - full sport_event_timelines" t
join public."All Games (sr:sport_events)" g on g.id = t.game_id
left join public."Seasons (current sr:season:ID)" s on s.id = g.season_id
left join public."Competitions" c on c.id = s.competition_id
cross join lateral jsonb_array_elements(coalesce(t.timeline_json -> 'timeline', '[]'::jsonb)) as e
where g.status in ('closed', 'ended')
  and e ->> 'type' in ('video_assistant_referee', 'video_assistant_referee_over')
order by
  c.competition_name nulls last,
  g.start_time nulls last,
  g.sport_event_id,
  (e ->> 'match_time')::int nulls last,
  (e ->> 'id')::bigint;
