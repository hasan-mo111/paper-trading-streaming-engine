-- =============================================================================
--  AETHERON CORE  —  admin console, APK memberships and the AI trade ledger
-- -----------------------------------------------------------------------------
--  Run this ONCE in Supabase → SQL Editor if your project already has the
--  original tables from schema.sql. It is idempotent, so running it again is
--  harmless. The same definitions are included in schema.sql for fresh
--  installations.
--
--  Tables created:
--    app_users            one row per APK installation (device id, plan, ban)
--    membership_requests  upgrade requests submitted from the APK
--    ai_trades            permanent ledger of the AI analyst's paper trades
--
--  RLS is enabled with NO policies: only the server's service_role key can
--  read or write these rows. The APK talks to the Node API, never to Supabase.
-- =============================================================================

begin;

-- ---------------------------------------------------------------- app_users
create table if not exists public.app_users (
  id              uuid primary key default gen_random_uuid(),
  device_id       text        not null unique,
  display_name    text,
  contact_method  text        check (contact_method is null or contact_method in ('telegram','whatsapp','email','other')),
  contact_value   text,
  plan            text        not null default 'FREE' check (plan in ('FREE','MONTHLY','YEARLY')),
  status          text        not null default 'ACTIVE' check (status in ('ACTIVE','BANNED')),
  plan_started_at timestamptz,
  plan_expires_at timestamptz,
  note            text,
  last_seen_at    timestamptz not null default now(),
  created_at      timestamptz not null default now(),
  updated_at      timestamptz not null default now()
);

create index if not exists app_users_status_idx    on public.app_users (status, plan);
create index if not exists app_users_last_seen_idx on public.app_users (last_seen_at desc);

-- ------------------------------------------------------- membership_requests
create table if not exists public.membership_requests (
  id             uuid primary key default gen_random_uuid(),
  user_id        uuid references public.app_users (id) on delete cascade,
  device_id      text        not null,
  requested_plan text        not null check (requested_plan in ('MONTHLY','YEARLY')),
  contact_method text,
  contact_value  text,
  message        text,
  status         text        not null default 'PENDING' check (status in ('PENDING','APPROVED','REJECTED')),
  admin_note     text,
  resolved_by    text,
  resolved_at    timestamptz,
  created_at     timestamptz not null default now()
);

create index if not exists membership_requests_status_idx
  on public.membership_requests (status, created_at desc);
create index if not exists membership_requests_device_idx
  on public.membership_requests (device_id, created_at desc);
create unique index if not exists membership_requests_one_pending
  on public.membership_requests (device_id) where status = 'PENDING';

-- ----------------------------------------------------------------- ai_trades
create table if not exists public.ai_trades (
  id              text primary key,
  symbol          text        not null,
  action          text        not null check (action in ('BUY','SELL')),
  entry_price     double precision not null,
  stop_loss       double precision,
  take_profit_1   double precision,
  take_profit_2   double precision,
  rr              double precision,
  confidence      double precision,
  reasoning       text,
  model           text,
  stage           text        not null default 'open' check (stage in ('open','tp1')),
  status          text        not null default 'OPEN' check (status in ('OPEN','CLOSED')),
  opened_at       timestamptz not null default now(),
  opened_ms       bigint,
  closed_at       timestamptz,
  closed_ms       bigint,
  exit_price      double precision,
  exit_reason     text,
  pnl_points      double precision,
  pnl_pct         double precision,
  last_price      double precision,
  mfe             double precision not null default 0,
  mae             double precision not null default 0,
  meta            jsonb       not null default '{}'::jsonb,
  created_at      timestamptz not null default now(),
  updated_at      timestamptz not null default now()
);

create unique index if not exists ai_trades_one_open_per_symbol
  on public.ai_trades (symbol) where status = 'OPEN';
create index if not exists ai_trades_status_idx on public.ai_trades (status, opened_at desc);
create index if not exists ai_trades_symbol_idx on public.ai_trades (symbol, opened_at desc);
create index if not exists ai_trades_closed_idx on public.ai_trades (closed_at desc) where status = 'CLOSED';

-- ----------------------------------------------------------------------- RLS
alter table public.app_users           enable row level security;
alter table public.membership_requests enable row level security;
alter table public.ai_trades           enable row level security;

commit;

-- Verify:
--   select count(*) from public.app_users;
--   select count(*) from public.membership_requests where status = 'PENDING';
--   select symbol, action, entry_price, opened_at from public.ai_trades where status = 'OPEN';
