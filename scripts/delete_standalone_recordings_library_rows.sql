-- Run in the **standalone recordings** Supabase project SQL Editor (not the main tier-1 DB).
-- Removes Championship recording rows matching the backfill game IDs.

delete from public."Soccer // Recordings Library"
where "sr_sport_event_id" in (
  'sr:sport_event:61491209',
  'sr:sport_event:61491213',
  'sr:sport_event:61491195',
  'sr:sport_event:61491205',
  'sr:sport_event:61491199',
  'sr:sport_event:61491159',
  'sr:sport_event:61491155',
  'sr:sport_event:61491161',
  'sr:sport_event:61491147',
  'sr:sport_event:61491151',
  'sr:sport_event:61491149',
  'sr:sport_event:61491165',
  'sr:sport_event:61491145',
  'sr:sport_event:61491153',
  'sr:sport_event:61491157',
  'sr:sport_event:68305676'
);
