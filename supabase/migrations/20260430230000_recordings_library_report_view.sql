-- Stable REST-friendly view over "Soccer // Recordings Library" (slashes break some PostgREST URL builders).

create or replace view public.recordings_library_report as
select
  "ID" as id,
  "sr_sport_event_id" as sr_sport_event_id,
  "recording_id" as recording_id,
  "Title" as title,
  "Season" as season,
  "Competition Name" as competition_name,
  "Category" as category,
  "Sport Event Status" as sport_event_status,
  "Sport Event Start" as sport_event_start,
  "Sport Event Venue" as sport_event_venue,
  "SR Sport Event Venue ID" as sr_sport_event_venue_id,
  "Sport Event City" as sport_event_city,
  "Event Summary" as event_summary,
  "Event Statistics" as event_statistics,
  "Event Lineup" as event_lineup,
  "Event VAR" as event_var,
  "Event Commentary" as event_commentary,
  "Event Timeline" as event_timeline,
  "Event Win Prob" as event_win_prob,
  "Status" as status
from public."Soccer // Recordings Library";

comment on view public.recordings_library_report is
  'Reporting view for HTML/PGT — same rows as Soccer // Recordings Library with snake_case aliases.';
