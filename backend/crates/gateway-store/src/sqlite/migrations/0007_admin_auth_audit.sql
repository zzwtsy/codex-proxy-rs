-- 管理员密码与持久安全审计事实，时间使用 UTC epoch 微秒。
create table admin_users (
  id text primary key,
  password_hash text not null,
  created_at_us integer not null,
  updated_at_us integer not null,
  check (created_at_us <= updated_at_us)
);

create table admin_audit_events (
  id text primary key,
  actor_kind text not null
    check (actor_kind in ('admin_session', 'admin_api_key', 'system', 'anonymous')),
  actor_admin_user_id text,
  actor_ref text not null,
  admin_request_id text,
  action text not null,
  entity_kind text not null,
  entity_ref text not null,
  config_revision integer check (config_revision is null or config_revision > 0),
  changed_fields_json text not null default '[]'
    check (json_valid(changed_fields_json) and json_type(changed_fields_json) = 'array'
      and json_array_length(changed_fields_json) <= 64),
  created_at_us integer not null,
  check (actor_admin_user_id is null or
    (actor_kind = 'admin_session' and actor_ref = 'admin:' || actor_admin_user_id)),
  foreign key (actor_admin_user_id) references admin_users(id)
    on update restrict on delete set null
);
create index admin_audit_events_actor_idx on admin_audit_events (actor_admin_user_id)
  where actor_admin_user_id is not null;
create index admin_audit_events_created_idx on admin_audit_events (created_at_us desc, id desc);
