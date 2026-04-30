-- Reset "ID" identity to start at 1 after reload, and lock down access.
-- After this migration runs, re-populate from JSON: python sync_recordings_library.py
--
-- 1) Truncate clears rows and restarts the identity sequence (next insert gets ID 1).
-- 2) RLS on the base table (no policies): anon/authenticated cannot read; service_role still works.
-- 3) security_invoker on the view so Supabase evaluates the base table's RLS instead of showing the view as unrestricted.

truncate table public."Soccer // Recordings Library" restart identity;

alter table public."Soccer // Recordings Library" enable row level security;
alter table public."Soccer // Recordings Library" force row level security;

alter view public.recordings_library_report set (security_invoker = true);

comment on view public.recordings_library_report is
  'Reporting view (security_invoker): uses base table RLS. Re-sync data after truncate.';
