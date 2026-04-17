-- Align public.competitions with display/metadata columns:
--   id (uuid), sportradar_competition_id, competition_name, gender, category_name, country_code
-- Idempotent.

do $$
begin
  if exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'competitions' and column_name = 'name'
  ) and not exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'competitions' and column_name = 'competition_name'
  ) then
    alter table public.competitions rename column name to competition_name;
  end if;
end $$;

alter table public.competitions add column if not exists gender text;
alter table public.competitions add column if not exists category_name text;
alter table public.competitions add column if not exists country_code text;
