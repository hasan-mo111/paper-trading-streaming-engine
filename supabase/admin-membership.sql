-- =============================================================================
--  AETHERON CORE  —  admin console, APK memberships and permanent trade books
-- -----------------------------------------------------------------------------
--  Run this ONCE in Supabase → SQL Editor if your project already has the
--  original tables from schema.sql. It is idempotent, so running it again is
--  harmless. The same definitions are included in schema.sql for fresh
--  installations.
--
--  Tables created/updated:
--    app_users            one account per email (or per device when no email)
--    app_user_devices     every APK installation linked to that account
--    membership_requests  upgrade requests submitted from the APK
--    ai_trades            permanent ledger of the AI analyst's paper trades
--    strategy_trades      durable open/closed book of the strategy engine
--
--  IDENTITY
--  --------
--  A device id is the transport identity. An email is the stronger account
--  identity: two installations that send the same email resolve to one
--  app_users row and share plan/status/history. Legacy rows whose contact
--  method is `email` are backfilled, and duplicate emails are merged onto the
--  best plan before the unique index is installed.
--
--  RLS is enabled with NO policies: only the server's service_role key can
--  read or write these rows. The APK talks to the Node API, never to Supabase.
-- =============================================================================

begin;

-- ---------------------------------------------------------------- app_users
create table if not exists public.app_users (
  id              uuid primary key default gen_random_uuid(),
  device_id       text        not null unique,
  email           text,
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

-- Existing installations created before email became an account identity.
alter table public.app_users add column if not exists email text;

create index if not exists app_users_status_idx    on public.app_users (status, plan);
create index if not exists app_users_last_seen_idx on public.app_users (last_seen_at desc);
create index if not exists app_users_email_idx     on public.app_users (lower(email)) where email is not null;

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

-- ----------------------------------------------------------- strategy_trades
-- One row per strategy position. The legs, stop and remaining volume are
-- updated in place; a close flips status to CLOSED and leaves the row forever.
create table if not exists public.strategy_trades (
  id              text primary key,
  signal_id       text,
  user_id         uuid,
  symbol          text        not null,
  side            text        not null check (side in ('LONG','SHORT')),
  volume          numeric(14,4) not null default 0,
  entry_price     double precision not null,
  stop_loss       double precision,
  atr             double precision,
  risk_amount     numeric(14,2),
  legs            jsonb       not null default '[]'::jsonb,
  status          text        not null default 'OPEN' check (status in ('OPEN','CLOSED')),
  exit_price      double precision,
  exit_reason     text,
  profit          numeric(14,2),
  broker_id       text,
  mode            text,
  opened_at       timestamptz not null default now(),
  opened_ms       bigint,
  closed_at       timestamptz,
  closed_ms       bigint,
  meta            jsonb       not null default '{}'::jsonb,
  created_at      timestamptz not null default now(),
  updated_at      timestamptz not null default now()
);

create unique index if not exists strategy_trades_one_open_per_symbol
  on public.strategy_trades (symbol) where status = 'OPEN';
create index if not exists strategy_trades_status_idx
  on public.strategy_trades (status, opened_at desc);
create index if not exists strategy_trades_symbol_idx
  on public.strategy_trades (symbol, opened_at desc);
create index if not exists strategy_trades_closed_idx
  on public.strategy_trades (closed_at desc) where status = 'CLOSED';

-- ------------------------------------------------------------ app_user_devices
-- A second installation with the same email is linked here. The app_users row
-- remains the single account that owns the plan, ban state and history.
create table if not exists public.app_user_devices (
  device_id    text primary key,
  user_id      uuid not null references public.app_users (id) on delete cascade,
  created_at   timestamptz not null default now(),
  last_seen_at timestamptz not null default now()
);

create index if not exists app_user_devices_user_idx
  on public.app_user_devices (user_id, created_at);

-- ------------------------------------------------------- identity backfill
-- Turn the optional email contact into the canonical identity column.
update public.app_users
   set email = lower(trim(contact_value))
 where email is null
   and contact_method = 'email'
   and contact_value ~* '^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$';

update public.app_users
   set email = lower(trim(email))
 where email is not null
   and email <> lower(trim(email));

-- Every primary device belongs to its account. Existing rows are the source.
insert into public.app_user_devices (device_id, user_id, created_at, last_seen_at)
select device_id, id, created_at, last_seen_at
  from public.app_users
on conflict (device_id) do update
  set user_id = excluded.user_id,
      last_seen_at = greatest(public.app_user_devices.last_seen_at, excluded.last_seen_at);

-- Merge legacy duplicate emails before the unique index is created. Keep the
-- row with the best plan/expiry, carry a ban across, move every device and
-- request to the winner, then delete the duplicate account rows.
do $$
declare
  dup record;
  keep_id uuid;
  best_plan text;
  best_start timestamptz;
  best_exp timestamptz;
  banned boolean;
begin
  for dup in
    select lower(email) as email_key
      from public.app_users
     where email is not null and length(trim(email)) > 0
     group by lower(email)
    having count(*) > 1
  loop
    select u.plan, u.plan_started_at, u.plan_expires_at
      into best_plan, best_start, best_exp
      from public.app_users u
     where lower(u.email) = dup.email_key
     order by case u.plan when 'YEARLY' then 3 when 'MONTHLY' then 2 else 1 end desc,
              coalesce(u.plan_expires_at, 'epoch'::timestamptz) desc,
              u.created_at asc
     limit 1;

    select u.id
      into keep_id
      from public.app_users u
     where lower(u.email) = dup.email_key
     order by case u.plan when 'YEARLY' then 3 when 'MONTHLY' then 2 else 1 end desc,
              coalesce(u.plan_expires_at, 'epoch'::timestamptz) desc,
              u.created_at asc
     limit 1;

    select exists(
      select 1 from public.app_users u
       where lower(u.email) = dup.email_key and u.status = 'BANNED'
    ) into banned;

    update public.app_users
       set plan = best_plan,
           plan_started_at = best_start,
           plan_expires_at = best_exp,
           status = case when banned then 'BANNED' else 'ACTIVE' end,
           email = dup.email_key,
           updated_at = now()
     where id = keep_id;

    update public.membership_requests
       set user_id = keep_id
     where user_id in (
       select id from public.app_users
        where lower(email) = dup.email_key and id <> keep_id
     );

    update public.app_user_devices
       set user_id = keep_id
     where user_id in (
       select id from public.app_users
        where lower(email) = dup.email_key and id <> keep_id
     );

    delete from public.app_users
     where lower(email) = dup.email_key and id <> keep_id;
  end loop;
end $$;

create unique index if not exists app_users_email_unique
  on public.app_users (lower(email)) where email is not null;

-- ----------------------------------------------------------------------- RLS
alter table public.app_users           enable row level security;
alter table public.app_user_devices    enable row level security;
alter table public.membership_requests enable row level security;
alter table public.ai_trades           enable row level security;
alter table public.strategy_trades     enable row level security;

commit;

-- Verify:
--   select count(*) from public.app_users;
--   select count(*) from public.app_user_devices;
--   select count(*) from public.membership_requests where status = 'PENDING';
--   select symbol, action, entry_price, opened_at from public.ai_trades where status = 'OPEN';
--   select symbol, side, volume, opened_at from public.strategy_trades where status = 'OPEN';
