-- TrendPlus: replace Supabase Auth (auth.users / user_profiles) with a manual
-- Admin Approval auth system, backed by our own `users` table.
--
-- Confirmed via information_schema that current data is test/dummy data only
-- (no real accounts to preserve), so this migration wipes and rebuilds rather
-- than migrating passwords we can't read anyway (auth.users stores its own
-- hash format we have no access to and are dropping entirely).
--
-- Safe to run once. Back up first if you're unsure whether any of the
-- existing rows in user_profiles / portfolios / holdings matter to you.

create extension if not exists pgcrypto; -- provides gen_random_uuid()

-- ── Wipe existing test data tied to Supabase Auth ─────────────────────
delete from holdings;
delete from portfolios;
delete from user_profiles;
-- Requires sufficient privileges (service role). If this errors out due to
-- permissions, it's harmless to skip — auth.users will simply sit unused.
delete from auth.users;

-- ── Drop obsolete OTP infrastructure, if present from an earlier branch ──
drop table if exists user_email_otps;

-- ── Our own users table (replaces auth.users + user_profiles) ─────────
-- Keeps a uuid primary key so existing portfolios/holdings FK *types*
-- don't need to change — only what they point to.
create table if not exists users (
    id              uuid primary key default gen_random_uuid(),
    full_name       text not null,
    email           text not null,
    phone           text not null,
    password_hash   text not null,
    active          boolean not null default true,
    approved_at     timestamptz,
    created_at      timestamptz not null default now(),
    last_login_at   timestamptz
);

create unique index if not exists uq_users_email on users (lower(email));
create unique index if not exists uq_users_phone on users (phone);

-- ── Re-point portfolios/holdings from auth.users to our users table ───
alter table portfolios drop constraint if exists portfolios_user_id_fkey;
alter table portfolios
    add constraint portfolios_user_id_fkey
    foreign key (user_id) references users(id) on delete cascade;

alter table holdings drop constraint if exists holdings_user_id_fkey;
alter table holdings
    add constraint holdings_user_id_fkey
    foreign key (user_id) references users(id) on delete cascade;

-- ── user_profiles is now redundant (folded into `users`) ──────────────
drop table if exists user_profiles;

-- ── Pending users awaiting admin approval ─────────────────────────────
create table if not exists pending_users (
    id              bigserial primary key,
    full_name       text not null,
    email           text not null,
    phone           text not null,
    password_hash   text not null,
    status          text not null default 'pending' check (status in ('pending', 'rejected')),
    created_at      timestamptz not null default now()
);

create unique index if not exists uq_pending_users_email on pending_users (lower(email));
create unique index if not exists uq_pending_users_phone on pending_users (phone);
create index if not exists idx_pending_users_status on pending_users (status, created_at desc);

-- ── Admin accounts (fully separate from the user table) ───────────────
create table if not exists admins (
    id              bigserial primary key,
    username        text not null unique,
    password_hash   text not null,
    created_at      timestamptz not null default now()
);
