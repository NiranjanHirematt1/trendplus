-- TrendPlus portfolio management and authentication expansion

create table if not exists users (
    id              bigserial primary key,
    email           text not null unique,
    password_hash   text not null,
    created_at      timestamptz not null default now(),
    last_login_at   timestamptz
);

create table if not exists user_email_otps (
    id          bigserial primary key,
    email       text not null,
    otp_code    text not null,
    created_at  timestamptz not null default now(),
    expires_at  timestamptz not null,
    consumed_at timestamptz
);

create index if not exists idx_user_email_otps_email_created
    on user_email_otps (email, created_at desc);

create table if not exists portfolios (
    id              bigserial primary key,
    user_id         bigint not null references users(id) on delete cascade,
    portfolio_name  text not null default 'My Portfolio',
    created_at      timestamptz not null default now(),
    updated_at      timestamptz not null default now()
);

create index if not exists idx_portfolios_user
    on portfolios (user_id, created_at);

create table if not exists holdings (
    id                      bigserial primary key,
    portfolio_id            bigint not null references portfolios(id) on delete cascade,
    symbol                  text not null references symbols(symbol),
    quantity                numeric(18,4) not null check (quantity > 0),
    avg_buy_price           numeric(14,4) not null check (avg_buy_price > 0),
    buy_date                date,
    status                  text not null default 'ACTIVE' check (status in ('ACTIVE','SOLD','ARCHIVED')),
    sell_date               date,
    sell_price              numeric(14,4) check (sell_price is null or sell_price > 0),
    requires_confirmation   boolean not null default false,
    import_source           text,
    created_at              timestamptz not null default now(),
    updated_at              timestamptz not null default now(),
    constraint chk_sold_details check (
        (status <> 'SOLD') or (sell_date is not null and sell_price is not null)
    )
);

create unique index if not exists uq_holdings_active_symbol
    on holdings (portfolio_id, symbol)
    where status = 'ACTIVE';

create index if not exists idx_holdings_portfolio_status
    on holdings (portfolio_id, status, created_at desc);

create index if not exists idx_holdings_symbol_status
    on holdings (symbol, status);
