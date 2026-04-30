-- Rename recordings library table to dashboard-friendly quoted identifier (slashes OK in Postgres).
-- Idempotent: only runs when the legacy identifier still exists.

do $$
begin
  if to_regclass('public.soccer_recordings_library') is not null then
    alter table public.soccer_recordings_library rename to "Soccer // Recordings Library";
  end if;
end $$;
