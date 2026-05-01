-- Remove Championship backfill: sr:competition:18 / sr:season:130921 and the 16 recording matches.
-- Run in Supabase SQL Editor (Production). Deletes cascades from games when season is removed,
-- after clearing tables without FK cascades from seasons.

begin;

-- 1) own_goals has no FK to games — delete explicitly
delete from public.own_goals
where sport_event_id in (
  'sr:sport_event:61491145',
  'sr:sport_event:61491147',
  'sr:sport_event:61491149',
  'sr:sport_event:61491151',
  'sr:sport_event:61491153',
  'sr:sport_event:61491155',
  'sr:sport_event:61491157',
  'sr:sport_event:61491159',
  'sr:sport_event:61491161',
  'sr:sport_event:61491165',
  'sr:sport_event:61491195',
  'sr:sport_event:61491199',
  'sr:sport_event:61491205',
  'sr:sport_event:61491209',
  'sr:sport_event:61491213',
  'sr:sport_event:68305676'
);

-- 2) Recordings library rows for those matches (optional — comment out if you want to keep Record/Replay rows)
delete from public."Soccer // Recordings Library"
where "sr_sport_event_id" in (
  'sr:sport_event:61491145',
  'sr:sport_event:61491147',
  'sr:sport_event:61491149',
  'sr:sport_event:61491151',
  'sr:sport_event:61491153',
  'sr:sport_event:61491155',
  'sr:sport_event:61491157',
  'sr:sport_event:61491159',
  'sr:sport_event:61491161',
  'sr:sport_event:61491165',
  'sr:sport_event:61491195',
  'sr:sport_event:61491199',
  'sr:sport_event:61491205',
  'sr:sport_event:61491209',
  'sr:sport_event:61491213',
  'sr:sport_event:68305676'
);

-- 3) Season deletion cascades to "All Games (sr:sport_events)", timelines, penalty_shootout_matches,
--    var_timeline_events (FK on delete cascade from games).
delete from public."Seasons (current sr:season:ID)"
where sportradar_season_id = 'sr:season:130921';

-- 4) Competition row — only if nothing references it anymore
delete from public."Competitions" c
where c.sportradar_competition_id = 'sr:competition:18'
  and not exists (
    select 1
    from public."Seasons (current sr:season:ID)" s
    where s.competition_id = c.id
  );

commit;
