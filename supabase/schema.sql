-- =============================================================================
--  AETHERON CORE  —  supabase/schema.sql
--  -----------------------------------------------------------------------------
--  Run this once in the Supabase SQL Editor (or `psql -f supabase/schema.sql`).
--  It is idempotent: re-running it is safe.
--
--  SECURITY MODEL
--  --------------
--  Every table has RLS enabled and NO permissive policy for `anon`. The engine
--  talks to Supabase with the service-role key from the server process only, so
--  it bypasses RLS by design. The Android app never holds that key: it sends
--  its user JWT to Aetheron Core, which validates it and then does the database
--  work server-side. Nothing in this schema is reachable from a public token.
-- =============================================================================

begin;

-- ---------------------------------------------------------------- extensions
create extension if not exists pgcrypto;

-- =============================================================================
--  candles — permanent OHLC history (the point of the whole exercise)
-- =============================================================================
--  `timeframe` holds the interval in SECONDS (300 / 900 / 3600 / 14400 / 86400),
--  matching src/config/db.js, so the application key and the primary key agree.
create table if not exists public.candles (
  symbol      text        not null,
  timeframe   integer     not null check (timeframe > 0),
  ts          bigint      not null,               -- epoch seconds, grid-aligned
  open        double precision not null,
  high        double precision not null,
  low         double precision not null,
  close       double precision not null,
  tick_volume bigint      not null default 0,
  closed      boolean     not null default true,  -- false = still forming
  updated_at  timestamptz not null default now(),
  constraint candles_pkey primary key (symbol, timeframe, ts),
  constraint candles_ohlc_sane check (high >= low and high >= open and high >= close and low <= open and low <= close)
);

-- The bootstrap query is "newest N closed candles for a series".
create index if not exists candles_lookup_idx
  on public.candles (symbol, timeframe, closed, ts desc);

-- =============================================================================
--  profiles — plan, sizing parameters and the per-account trading switch
-- =============================================================================
create table if not exists public.profiles (
  user_id         uuid primary key references auth.users (id) on delete cascade,
  email           text,
  plan            text        not null default 'FREE' check (plan in ('FREE', 'PRO')),
  account_balance numeric(18,2) not null default 10000.00,
  -- 0.01 == 1% of balance risked per trade
  risk_percentage numeric(8,6)  not null default 0.010000
                  check (risk_percentage > 0 and risk_percentage <= 1),
  enabled         boolean     not null default true,
  symbols         text[]      not null default '{}',
  created_at      timestamptz not null default now(),
  updated_at      timestamptz not null default now()
);

-- =============================================================================
--  daily_executions — the freemium counter
-- =============================================================================
create table if not exists public.daily_executions (
  user_id    uuid    not null references auth.users (id) on delete cascade,
  day        date    not null,
  count      integer not null default 0 check (count >= 0),
  updated_at timestamptz not null default now(),
  primary key (user_id, day)
);

-- =============================================================================
--  executions — immutable audit log of everything the engine decided
-- =============================================================================
--  `id` is the signal id, which doubles as an idempotency key: a retried broker
--  call cannot produce a second row, so a fill can never be double-counted.
create table if not exists public.executions (
  id           uuid primary key,
  signal_id    uuid,
  user_id      uuid,
  symbol       text        not null,
  side         text check (side in ('BUY', 'SELL')),
  volume       numeric(12,4),
  entry_price  double precision,
  exit_price   double precision,
  stop_loss    double precision,
  status       text        not null check (status in ('EXECUTED', 'CLOSED', 'SIGNAL_ONLY', 'FAILED', 'REJECTED')),
  reason       text,
  mode         text        not null default 'paper',
  profit       numeric(14,2),
  atr          double precision,
  risk_amount  numeric(14,2),
  legs         jsonb,
  created_at   timestamptz not null default now()
);

create index if not exists executions_user_idx on public.executions (user_id, created_at desc);

-- =============================================================================
--  chat_events — chat-ops history, replayed to reconnecting clients
-- =============================================================================
create table if not exists public.chat_events (
  id         uuid primary key,
  user_id    uuid,
  type       text        not null,
  severity   text        not null default 'info' check (severity in ('info', 'success', 'warning', 'error')),
  message    text        not null,
  meta       jsonb,
  created_at timestamptz not null default now()
);

create index if not exists chat_events_user_idx on public.chat_events (user_id, created_at desc);
create index if not exists chat_events_time_idx on public.chat_events (created_at desc);

-- =============================================================================
--  RPC: atomic quota increment
-- =============================================================================
--  Reading the counter and writing it back from the app would race across two
--  instances. This does the whole read-modify-write inside ONE transaction,
--  which is what makes "exactly 3 executions per day" actually hold.
create or replace function public.aetheron_consume_execution(
  p_user  uuid,
  p_day   date,
  p_limit integer default 0
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_count integer;
begin
  insert into public.daily_executions (user_id, day, count, updated_at)
  values (p_user, p_day, 1, now())
  on conflict (user_id, day) do update
    set count      = public.daily_executions.count + 1,
        updated_at = now()
  returning count into v_count;

  -- `exceeded` is advisory: the caller has already asked `aetheron_can_execute`
  -- semantics via this row. p_limit = 0 means PRO (unlimited).
  return jsonb_build_object(
    'user',     p_user,
    'day',      p_day,
    'used',     v_count,
    'limit',    p_limit,
    'exceeded', case when p_limit > 0 and v_count > p_limit then true else false end,
    'at',       now()
  );
end;
$$;

-- A read-only variant for /api/user/quota (no increment side effect).
create or replace function public.aetheron_daily_usage(p_user uuid, p_day date default current_date)
returns integer
language sql
stable
security definer
set search_path = public
as $$
  select coalesce((select count from public.daily_executions
                    where user_id = p_user and day = p_day), 0);
$$;

-- Trivial connectivity probe used by /health.
create or replace function public.aetheron_ping()
returns timestamptz
language sql
stable
as $$ select now() $$;

-- =============================================================================
--  ROW LEVEL SECURITY
-- =============================================================================
--  Enabled with zero policies: the ONLY client that can read or write these
--  tables is the service role (the Node process). Even a compromised user JWT
--  cannot touch another account's quota or executions.
alter table public.candles          enable row level security;
alter table public.profiles         enable row level security;
alter table public.daily_executions enable row level security;
alter table public.executions       enable row level security;
alter table public.chat_events      enable row level security;

-- (Intentionally no policies. Uncomment the pair below only if you want a
--  user to manage their own plan from the app — never for candles or quota.)

-- create policy "own profile" on public.profiles
--   for all using (auth.uid() = user_id) with check (auth.uid() = user_id);

-- =============================================================================
--  NEW-USER TRIGGER — every auth user gets a FREE profile automatically
-- =============================================================================
create or replace function public.aetheron_handle_new_user()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  insert into public.profiles (user_id, email)
  values (new.id, new.email)
  on conflict (user_id) do nothing;
  return new;
end;
$$;

drop trigger if exists on_auth_user_created on auth.users;
create trigger on_auth_user_created
  after insert on auth.users
  for each row execute function public.aetheron_handle_new_user();

-- =============================================================================
--  HOUSEKEEPING — keep the database from growing without bound
-- =============================================================================
--  Intraday bars are only useful for chart history; daily bars are the strategy's
--  permanent record and are never pruned.
create or replace function public.aetheron_prune_candles(p_keep_seconds integer[] default array[300,900,3600,14400], p_keep_rows integer default 3000)
returns void
language sql
security definer
set search_path = public
as $$
  delete from public.candles c
   where c.timeframe = any (p_keep_seconds)
     and c.closed
     and c.ts < (
       select min(ts) from (
         select ts from public.candles
          where symbol = c.symbol and timeframe = c.timeframe and closed
          order by ts desc limit p_keep_rows
       ) t
     );
$$;

-- Schedule it nightly (pg_cron). Optional — comment out if the extension is off.
-- select cron.schedule('aetheron-prune-candles', '17 4 * * *', $$select public.aetheron_prune_candles()$$);

commit;

-- =============================================================================
--  VERIFY
-- =============================================================================
--  select count(*) from public.candles;
--  select public.aetheron_ping();
--  select * from public.aetheron_consume_execution('<your-uuid>', current_date, 3);
-- =============================================================================