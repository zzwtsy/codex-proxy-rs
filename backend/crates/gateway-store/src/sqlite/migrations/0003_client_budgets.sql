-- SQLite 中无损保存客户端 Key 预算；金额是 numeric(20, 10) 的固定宽度缩放整数文本。
create table client_api_keys (
  id text primary key,
  enabled integer not null default 1 check (enabled in (0, 1)),
  daily_limit_usd text not null default '00000000000000000000'
    check (length(daily_limit_usd) = 20 and daily_limit_usd not glob '*[^0-9]*'),
  weekly_limit_usd text not null default '00000000000000000000'
    check (length(weekly_limit_usd) = 20 and weekly_limit_usd not glob '*[^0-9]*')
);

create table client_key_budget_windows (
  client_api_key_id text primary key references client_api_keys(id) on delete cascade,
  daily_start_us integer not null,
  daily_end_us integer not null,
  weekly_start_us integer not null,
  weekly_end_us integer not null,
  daily_used_usd text not null default '00000000000000000000'
    check (length(daily_used_usd) = 20 and daily_used_usd not glob '*[^0-9]*'),
  weekly_used_usd text not null default '00000000000000000000'
    check (length(weekly_used_usd) = 20 and weekly_used_usd not glob '*[^0-9]*'),
  check (daily_start_us <= daily_end_us and weekly_start_us <= weekly_end_us)
);

create table client_key_charge_events (
  request_id text primary key,
  client_api_key_id text not null references client_api_keys(id) on delete cascade,
  amount_usd text not null
    check (length(amount_usd) = 20 and amount_usd not glob '*[^0-9]*'),
  completed_at_us integer not null
);
create index client_key_charge_events_key_time_idx
  on client_key_charge_events (client_api_key_id, completed_at_us);
