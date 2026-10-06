-- SQLite 部署把旧的默认刷新提前量收窄到 Codex 基线；管理员自定义值保持不变。
update runtime_settings
   set refresh_margin_seconds = 300
 where id = 1
   and refresh_margin_seconds = 3600;

-- 把旧的 Fast 禁用布尔值转换为三态策略，旧列暂留以兼容已有数据库结构。
alter table account_groups
  add column fast_mode text not null default 'default'
  check (fast_mode in ('default', 'enabled', 'disabled'));

update account_groups
   set fast_mode = 'disabled'
 where disable_fast = 1;

-- 旧会话记录的整型修订号不满足新的不透明版本合同；重建表以使用 TEXT 亲和性。
create table provider_session_affinity_new (
  session_fingerprint text primary key,
  account_id text not null,
  revision text not null check (length(revision) = 32),
  expires_at_us integer not null
);

insert into provider_session_affinity_new
  (session_fingerprint, account_id, revision, expires_at_us)
select session_fingerprint, account_id,
       'a' || substr(lower(hex(randomblob(16))), 2), expires_at_us
  from provider_session_affinity;

drop table provider_session_affinity;
alter table provider_session_affinity_new rename to provider_session_affinity;
create index provider_session_affinity_expiry_idx
  on provider_session_affinity (expires_at_us);

create table provider_session_aliases (
  alias_fingerprint text primary key,
  session_key text not null,
  follow_only integer not null check (follow_only in (0, 1)),
  expires_at_us integer not null
);

create index provider_session_aliases_expiry_idx
  on provider_session_aliases (expires_at_us);
