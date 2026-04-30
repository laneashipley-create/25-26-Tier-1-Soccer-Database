-- Disciplinary cards (yellow, straight red, red-yellow / second yellow) for completed matches
-- that have a stored timeline in public."Completed Matches - full sport_event_timelines".
--
-- Rules:
--   Completed: match row status in ('closed', 'ended') (same as the app pipeline).
--   Card types from Sportradar timeline event `type`:
--     yellow_card, red_card, yellow_red_card (second yellow → sending off).
--   card_description: optional Sportradar field on the event (e.g. player_on_bench, post_match, half_time);
--     null when absent — not the same as commentary text.
--   commentary: first commentary line from the event when present (often empty for sparse card events).
--   player / team columns derived from the event JSON and games row.
--
-- One result row per card event (a match with multiple cards appears on multiple rows).
-- Uses LEFT JOIN to seasons/competitions so rows are kept if FK metadata is missing.
-- Re-run in Supabase: SQL Editor → paste → Run.

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
  e ->> 'type' as card_type,
  nullif(trim(e ->> 'card_description'), '') as card_description,
  (e ->> 'match_time')::int as match_minute,
  nullif(e ->> 'stoppage_time', '')::int as stoppage_minute,
  e ->> 'match_clock' as match_clock,
  e ->> 'period_type' as period_type,
  e ->> 'competitor' as competitor_side,
  case e ->> 'competitor'
    when 'home' then g.home_team
    when 'away' then g.away_team
    else null
  end as player_team,
  coalesce(e -> 'players' -> 0 ->> 'name', '') as player_name,
  e -> 'players' -> 0 ->> 'id' as player_id,
  coalesce(e -> 'commentaries' -> 0 ->> 'text', '') as commentary
from public."Completed Matches - full sport_event_timelines" t
join public."All Games (sr:sport_events)" g on g.id = t.game_id
left join public."Seasons (current sr:season:ID)" s on s.id = g.season_id
left join public."Competitions" c on c.id = s.competition_id
cross join lateral jsonb_array_elements(coalesce(t.timeline_json -> 'timeline', '[]'::jsonb)) as e
where g.status in ('closed', 'ended')
  and e ->> 'type' in ('yellow_card', 'red_card', 'yellow_red_card')
order by
  c.competition_name nulls last,
  g.start_time nulls last,
  g.sport_event_id,
  (e ->> 'match_time')::int nulls last,
  (e ->> 'id')::bigint;
