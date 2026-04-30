-- RPC used by sync_recordings_library.py: TRUNCATE ... RESTART IDENTITY so each full reload gets IDs 1..N.

create or replace function public.truncate_soccer_recordings_library()
returns void
language plpgsql
security definer
set search_path = public
as $$
begin
  truncate table public."Soccer // Recordings Library" restart identity;
end;
$$;

revoke all on function public.truncate_soccer_recordings_library() from public;
grant execute on function public.truncate_soccer_recordings_library() to service_role;

comment on function public.truncate_soccer_recordings_library() is
  'Full replace for Record/Replay library table; resets identity. Called from sync_recordings_library.py via PostgREST RPC.';
