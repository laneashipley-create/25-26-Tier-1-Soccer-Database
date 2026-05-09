-- Enable RLS for water-break derived tables (matches core EPL tables pattern).
-- Service role bypasses RLS; anon/authenticated have no policies → denied by default.

alter table public.water_break_timeline_events enable row level security;
alter table public.water_break_unpaired_matches enable row level security;

alter table public.water_break_timeline_events force row level security;
alter table public.water_break_unpaired_matches force row level security;
