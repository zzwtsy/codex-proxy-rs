-- SQLite 持久化的跨进程 Provider 协调状态。
create table credential_lease_counters (
  scope text not null check (scope in ('provider', 'account', 'refresh-capacity', 'refresh', 'task')),
  resource_fingerprint text not null,
  fencing_token integer not null default 0 check (fencing_token >= 0),
  last_started_at_us integer,
  primary key (scope, resource_fingerprint)
);

create table credential_leases (
  scope text not null,
  resource_fingerprint text not null,
  lease_id text not null,
  owner_fingerprint text not null,
  fencing_token integer not null check (fencing_token > 0),
  expires_at_us integer not null,
  primary key (scope, resource_fingerprint, lease_id),
  foreign key (scope, resource_fingerprint)
    references credential_lease_counters (scope, resource_fingerprint)
    on delete restrict
);

create table provider_cooldowns (
  account_id text primary key,
  credential_revision integer not null check (credential_revision > 0),
  until_us integer not null,
  kind text not null check (kind in ('rate_limit', 'capacity_freeze', 'capacity_freeze_probe')),
  generation text not null
);

create table provider_scoped_cooldowns (
  account_id text not null,
  scope_kind text not null,
  scope_value text not null,
  credential_revision integer not null check (credential_revision > 0),
  until_us integer not null,
  primary key (account_id, scope_kind, scope_value)
);

create table provider_capacity_failures (
  account_id text primary key,
  failure_count integer not null check (failure_count > 0),
  failures_expires_at_us integer not null,
  peak_in_flight integer check (peak_in_flight is null or peak_in_flight >= 0),
  peak_expires_at_us integer
);

create table cpr_store_write_lock (
  id integer primary key check (id = 1),
  revision integer not null default 0
);
insert into cpr_store_write_lock (id, revision) values (1, 0);

create table credential_refresh_backoff (
  account_id text primary key,
  retry_at_us integer not null,
  failure_count integer not null check (failure_count > 0),
  updated_at_us integer not null
);

create table provider_session_affinity (
  session_fingerprint text primary key,
  account_id text not null,
  revision integer not null check (revision > 0),
  expires_at_us integer not null
);

create table provider_session_exclusions (
  session_fingerprint text not null,
  account_id text not null,
  revision text not null,
  expires_at_us integer not null,
  primary key (session_fingerprint, account_id)
);

create index credential_leases_expiry_idx
  on credential_leases (scope, resource_fingerprint, expires_at_us);
create index provider_cooldowns_expiry_idx
  on provider_cooldowns (kind, until_us);
create index provider_scoped_cooldowns_expiry_idx
  on provider_scoped_cooldowns (until_us);
create index provider_capacity_failures_expiry_idx
  on provider_capacity_failures (failures_expires_at_us, peak_expires_at_us);
create index credential_refresh_backoff_expiry_idx
  on credential_refresh_backoff (retry_at_us);
create index provider_session_affinity_expiry_idx
  on provider_session_affinity (expires_at_us);
create index provider_session_exclusions_expiry_idx
  on provider_session_exclusions (expires_at_us);
