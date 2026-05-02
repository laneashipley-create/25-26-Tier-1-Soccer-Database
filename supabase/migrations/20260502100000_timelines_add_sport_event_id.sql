-- Sportradar match id (text like sr:sport_event:…) on timeline rows,
-- so the table is usable without joining public."All Games (sr:sport_events)" only for game UUID.

do $body$
declare
  tl regclass := coalesce(
    to_regclass('public."Completed Matches - full sport_event_timelines"'),
    to_regclass('public.sport_event_timelines')
  );
  gm regclass := coalesce(
    to_regclass('public."All Games (sr:sport_events)"'),
    to_regclass('public.games')
  );
begin
  if tl is null then
    raise notice 'timeline table not found; skipping sport_event_id column migration';
    return;
  end if;

  execute format(
    'alter table %s add column if not exists sport_event_id text',
    tl::text
  );

  if gm is not null then
    execute format(
      $sql$
      update %s t
      set sport_event_id = g.sport_event_id
      from %s g
      where g.id = t.game_id
        and (t.sport_event_id is null or t.sport_event_id is distinct from g.sport_event_id)
      $sql$,
      tl::text,
      gm::text
    );
  end if;

  execute format(
    'create index if not exists idx_completed_matches_timelines_sport_event_id on %s (sport_event_id)',
    tl::text
  );
end
$body$;
