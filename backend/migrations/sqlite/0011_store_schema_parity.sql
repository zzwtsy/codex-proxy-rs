-- 当前 PostgreSQL schema 中可持久化的备注与全局 Fast 限制字段。
alter table provider_accounts add column notes text
  check (notes is null or length(notes) <= 500);

alter table runtime_settings add column disable_fast integer not null default 0
  check (disable_fast in (0, 1));
