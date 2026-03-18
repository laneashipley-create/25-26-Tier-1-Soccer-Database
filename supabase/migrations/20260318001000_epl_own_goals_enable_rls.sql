-- Enable RLS for EPL Own Goals tables
-- Service role key bypasses RLS, so the pipeline can keep working.
-- With RLS enabled and no policies, the anon/public role will be denied by default.

alter table public.seasons enable row level security;
alter table public.schedule enable row level security;
alter table public.match_timelines enable row level security;
alter table public.own_goals enable row level security;

alter table public.seasons force row level security;
alter table public.schedule force row level security;
alter table public.match_timelines force row level security;
alter table public.own_goals force row level security;

