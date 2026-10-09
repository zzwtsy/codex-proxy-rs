-- Guardian 使用独立容量池，保留现有 fencing 与账号请求间隔
create temporary table reserved_lease_backup as select * from credential_leases;
create temporary table reserved_counter_backup as select * from credential_lease_counters;
drop table credential_leases;
drop table credential_lease_counters;
create table credential_lease_counters (
  scope text not null check (scope in ('provider', 'account', 'account-reserved', 'refresh-capacity', 'refresh', 'task')),
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
insert into credential_lease_counters select * from reserved_counter_backup;
insert into credential_leases select * from reserved_lease_backup;
drop table reserved_counter_backup;
drop table reserved_lease_backup;
create index credential_leases_expiry_idx on credential_leases (scope, resource_fingerprint, expires_at_us);
