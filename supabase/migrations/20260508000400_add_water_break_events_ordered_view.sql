-- Ordered water-break events per sport_event_id for Excel-friendly sequencing.

create or replace view public.water_break_timeline_events_ordered
with (security_invoker = true) as
with base as (
  select
    w.*,
    case
      when w.water_break_event_type = 'water_break_start' then 1
      when w.water_break_event_type = 'water_break_end' then 2
      else 9
    end as event_sort
  from public.water_break_timeline_events w
),
ordered as (
  select
    b.*,
    row_number() over (
      partition by b.sport_event_id
      order by
        coalesce(b.timeline_event_id, 999999999),
        coalesce(b.match_minute, 9999),
        coalesce(b.stoppage_minute, 9999),
        b.event_sort,
        b.id
    ) as water_break_seq
  from base b
)
select
  o.id,
  o.game_id,
  o.sport_event_id,
  o.match_date,
  o.home_team,
  o.away_team,
  o.status,
  o.recorded,
  o.sportradar_competition_id,
  o.competition_name,
  o.timeline_event_id,
  o.water_break_event_type,
  o.match_minute,
  o.stoppage_minute,
  o.match_clock,
  o.period_type,
  o.created_at,
  o.water_break_seq,
  case
    when o.water_break_seq = 1 then '1st half - water_break_start'
    when o.water_break_seq = 2 then '1st half - water_break_end'
    when o.water_break_seq = 3 then '2nd half - water_break_start'
    when o.water_break_seq = 4 then '2nd half - water_break_end'
    else 'extra event'
  end as water_break_sequence_label
from ordered o;
