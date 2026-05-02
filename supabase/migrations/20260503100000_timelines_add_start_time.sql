-- Kickoff time duplicated on timeline rows (matches public."All Games (sr:sport_events)".start_time).

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
    raise notice 'timeline table not found; skipping start_time column migration';
    return;
  end if;

  execute format(
    'alter table %s add column if not exists start_time timestamptz',
    tl::text
  );

  if gm is not null then
    execute format(
      $sql$
      update %s t
      set start_time = g.start_time
      from %s g
      where g.id = t.game_id
        and (t.start_time is null or t.start_time is distinct from g.start_time)
      $sql$,
      tl::text,
      gm::text
    );
  end if;

  execute format(
    'create index if not exists idx_completed_matches_timelines_start_time on %s (start_time)',
    tl::text
  );
end
$body$;
