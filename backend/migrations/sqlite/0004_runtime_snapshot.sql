-- 首次启动快照需要的配置与路由事实；默认值与 PostgreSQL 空库初始状态一致。
create table runtime_settings (
  id integer primary key check (id = 1),
  config_revision integer not null default 1 check (config_revision > 0),
  admin_api_key text,
  refresh_margin_seconds integer not null default 3600 check (refresh_margin_seconds > 0),
  refresh_concurrency integer not null default 2 check (refresh_concurrency > 0),
  max_concurrent_per_account integer not null default 3 check (max_concurrent_per_account > 0),
  request_interval_ms integer not null default 50 check (request_interval_ms >= 0),
  rotation_strategy text not null default 'smart'
    check (rotation_strategy in ('smart', 'quota_reset_priority', 'round_robin', 'sticky')),
  model_mappings_json text not null default '{}' check (json_valid(model_mappings_json)),
  usage_retention_days integer not null default 31 check (usage_retention_days >= 31),
  ops_event_retention_days integer not null default 30 check (ops_event_retention_days > 0),
  audit_retention_days integer not null default 90 check (audit_retention_days > 0),
  updated_at_us integer not null,
  min_codex_desktop_version text,
  min_codex_cli_version text,
  max_waiting_per_key integer not null default 0 check (max_waiting_per_key between 0 and 1000),
  max_waiting_per_account integer not null default 0 check (max_waiting_per_account between 0 and 1000),
  concurrency_wait_timeout_seconds integer not null default 30
    check (concurrency_wait_timeout_seconds between 1 and 120),
  openai_guardian_reserved_concurrency integer not null default 0
    check (openai_guardian_reserved_concurrency between 0 and 4294967295),
  request_location_json text not null default
    '{"country":"US","region":"Ohio","city":"Piketon","timezone":"America/New_York"}'
    check (json_valid(request_location_json)),
  request_location_enabled integer not null default 0 check (request_location_enabled in (0, 1)),
  responses_max_decompressed_body_bytes integer not null default 67108864
    check (responses_max_decompressed_body_bytes > 0),
  provider_request_profiles_json text not null default '{}'
    check (json_valid(provider_request_profiles_json)),
  smart_scheduling_json text not null default
    '{"loadWeight":10.0,"quotaWeight":8.0,"healthWeight":10.0,"latencyWeight":5.0,"resetWeight":0.0,"queueWeight":0.0,"preferHigherWeight":false}'
    check (json_valid(smart_scheduling_json)),
  pricing_overrides_json text not null default '{}' check (json_valid(pricing_overrides_json)),
  pricing_synced_json text not null default '{}' check (json_valid(pricing_synced_json)),
  pricing_synced_at_us integer,
  account_auto_freeze_enabled integer not null default 0 check (account_auto_freeze_enabled in (0, 1)),
  account_auto_freeze_threshold integer not null default 12 check (account_auto_freeze_threshold between 2 and 1000),
  account_auto_freeze_window_seconds integer not null default 600 check (account_auto_freeze_window_seconds between 60 and 3600),
  account_auto_freeze_duration_seconds integer not null default 7200 check (account_auto_freeze_duration_seconds between 300 and 604800),
  account_auto_freeze_probe_enabled integer not null default 1 check (account_auto_freeze_probe_enabled in (0, 1)),
  account_auto_freeze_probe_model text,
  account_auto_freeze_adaptive_concurrency integer not null default 1 check (account_auto_freeze_adaptive_concurrency in (0, 1)),
  account_warmup_enabled integer not null default 0 check (account_warmup_enabled in (0, 1)),
  account_warmup_schedule_time text not null default '08:00',
  account_warmup_model text,
  account_warmup_cursor_us integer,
  constraint runtime_settings_warmup_model_ck check (account_warmup_enabled = 0 or account_warmup_model is not null)
);

insert into runtime_settings (id, updated_at_us)
values (1, cast(strftime('%s', 'now') as integer) * 1000000);

alter table client_api_keys add column name text not null default '';
alter table client_api_keys add column label text;
alter table client_api_keys add column key text not null default '';
alter table client_api_keys add column max_concurrency integer not null default 0 check (max_concurrency >= 0);
alter table client_api_keys add column requests_per_minute integer not null default 0 check (requests_per_minute >= 0);
alter table client_api_keys add column provider_request_profiles_json text not null default '{}'
  check (json_valid(provider_request_profiles_json));
alter table client_api_keys add column created_at_us integer not null default 0;
alter table client_api_keys add column updated_at_us integer not null default 0;
create unique index client_api_keys_key_uq on client_api_keys (key);

create table account_groups (
  id text primary key,
  name text not null,
  description text,
  enabled integer not null default 1 check (enabled in (0, 1)),
  disable_fast integer not null default 0 check (disable_fast in (0, 1)),
  created_at_us integer not null,
  updated_at_us integer not null,
  color text not null default '#6B7280FF',
  check (length(trim(name)) between 1 and 100 and name = trim(name)),
  check (created_at_us <= updated_at_us)
);

create table account_group_accounts (
  account_group_id text not null,
  provider_account_id text not null,
  created_at_us integer not null,
  primary key (account_group_id, provider_account_id),
  foreign key (account_group_id) references account_groups(id) on update restrict on delete cascade,
  foreign key (provider_account_id) references provider_accounts(id) on update restrict on delete cascade
);
create index account_group_accounts_account_idx
  on account_group_accounts (provider_account_id, account_group_id);

create table client_api_key_groups (
  client_api_key_id text not null,
  account_group_id text not null,
  created_at_us integer not null,
  primary key (client_api_key_id, account_group_id),
  foreign key (client_api_key_id) references client_api_keys(id) on update restrict on delete cascade,
  foreign key (account_group_id) references account_groups(id) on update restrict on delete restrict
);
create index client_api_key_groups_group_idx
  on client_api_key_groups (account_group_id, client_api_key_id);
