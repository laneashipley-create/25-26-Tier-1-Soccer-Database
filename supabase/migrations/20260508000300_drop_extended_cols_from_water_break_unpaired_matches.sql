-- Water-break unpaired table no longer tracks extended timeline deltas.

drop index if exists public.idx_water_break_unpaired_matches_extended_delta;

alter table if exists public.water_break_unpaired_matches
  drop column if exists sport_event_extended_timeline_water_break_start,
  drop column if exists sport_event_extended_timeline_water_break_end,
  drop column if exists sport_event_extended_timeline_delta;
