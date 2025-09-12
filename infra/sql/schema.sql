-- Supabase schema for PredArb (public schema)
-- Run via Supabase SQL editor or `psql` against your project.

create table if not exists profiles (
  id uuid primary key default gen_random_uuid(),
  telegram_user_id text unique,
  username text,
  created_at timestamptz not null default now()
);

create table if not exists markets (
  id uuid primary key default gen_random_uuid(),
  source text not null, -- e.g., polymarket, limitless
  external_id text not null,
  title text not null,
  data jsonb,
  created_at timestamptz not null default now(),
  unique(source, external_id)
);

create table if not exists predictions (
  id uuid primary key default gen_random_uuid(),
  profile_id uuid references profiles(id) on delete cascade,
  market_id uuid references markets(id) on delete cascade,
  side text check (side in ('YES', 'NO')),
  price numeric,
  size numeric,
  status text default 'OPEN',
  created_at timestamptz not null default now()
);

create index if not exists idx_markets_source_ext on markets(source, external_id);
create index if not exists idx_predictions_profile on predictions(profile_id);

