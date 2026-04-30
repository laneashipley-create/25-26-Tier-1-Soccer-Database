-- Soccer - Recordings Library — Library Details layout (Excel column headers).
-- Source: Record/Replay export JSON (`soccer record replay list of sr sport event ids.json`).
-- Sync: python sync_recordings_library.py
-- Table is renamed to "Soccer // Recordings Library" in 20260430220000_rename_soccer_recordings_library_display_name.sql

drop view if exists public.soccer_recordings_library_details cascade;
drop table if exists public.soccer_recordings_library_apis cascade;
drop table if exists public.soccer_recordings_library cascade;

create table public.soccer_recordings_library (
  "ID" bigint generated always as identity primary key,
  "sr_sport_event_id" text not null unique,
  "recording_id" uuid not null unique,
  "Title" text,
  "Season" text,
  "Competition Name" text,
  "Category" text,
  "Sport Event Status" text,
  "Sport Event Start" timestamptz,
  "Sport Event Venue" text,
  "SR Sport Event Venue ID" text,
  "Sport Event City" text,
  "Event Summary" boolean not null default false,
  "Event Statistics" boolean not null default false,
  "Event Lineup" boolean not null default false,
  "Event VAR" boolean not null default false,
  "Event Commentary" boolean not null default false,
  "Event Timeline" boolean not null default false,
  "Event Win Prob" boolean not null default false,
  "Status" text
);

comment on table public.soccer_recordings_library is
  'Soccer - Recordings Library / Library Details — columns match spreadsheet headers.';
comment on column public.soccer_recordings_library."Season" is
  'Not present in Record/Replay JSON export; reserved for enrichment.';
comment on column public.soccer_recordings_library."Competition Name" is
  'Often populated from JSON `league` when metadata competition name is absent.';
comment on column public.soccer_recordings_library."Event Summary" is
  'True when export lists summary / summaries / extended-summary API.';
comment on column public.soccer_recordings_library."Event Timeline" is
  'True when export lists timeline or extended-timeline API.';

create index if not exists soccer_recordings_library_competition_name_idx
  on public.soccer_recordings_library ("Competition Name");
create index if not exists soccer_recordings_library_event_start_idx
  on public.soccer_recordings_library ("Sport Event Start" desc nulls last);

alter table public.soccer_recordings_library enable row level security;
alter table public.soccer_recordings_library force row level security;
