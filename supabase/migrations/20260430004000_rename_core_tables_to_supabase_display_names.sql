-- Rename core public tables to display names (matches Supabase Table Editor labels).
-- Idempotent: skips any source name that no longer exists (e.g. already renamed in dashboard).

do $body$
begin
  if to_regclass('public.games') is not null then
    execute format('alter table public.games rename to %I', 'All Games (sr:sport_events)');
  end if;

  if to_regclass('public.sport_event_timelines') is not null then
    execute format(
      'alter table public.sport_event_timelines rename to %I',
      'Completed Matches - full sport_event_timelines'
    );
  end if;

  if to_regclass('public.seasons') is not null then
    execute format(
      'alter table public.seasons rename to %I',
      'Seasons (current sr:season:ID)'
    );
  end if;

  if to_regclass('public.competitions') is not null then
    execute format('alter table public.competitions rename to %I', 'Competitions');
  end if;
end
$body$;
