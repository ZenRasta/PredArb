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

-- Table storing discovered arbitrage opportunities
create table if not exists arb_opportunities (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),
  opp_hash text not null,
  opp_type text not null,
  group_id text,
  legs jsonb not null,
  params jsonb not null default '{}'::jsonb,
  metrics jsonb not null default '{}'::jsonb
);

-- Deduplicate by hash
create unique index if not exists idx_arb_opportunities_hash on arb_opportunities(opp_hash);

-- Queue for user alerts referencing arbitrage rows
create table if not exists alerts_queue (
  id uuid primary key default gen_random_uuid(),
  user_id text not null,
  arb_id uuid references arb_opportunities(id) on delete cascade,
  status text not null default 'pending',
  created_at timestamptz not null default now()
);

-- Platform level fees used when computing opportunities
create table if not exists platform_fees (
  platform text primary key,
  taker_bps numeric not null,
  withdrawal_fee_usd numeric not null,
  gas_estimate_usd numeric not null,
  updated_at timestamptz not null default now()
);

-- Seed initial fee settings
insert into platform_fees (platform, taker_bps, withdrawal_fee_usd, gas_estimate_usd) values
  ('polymarket', 20, 5, 1),
  ('limitless', 0, 0, 0)
on conflict (platform) do nothing;

