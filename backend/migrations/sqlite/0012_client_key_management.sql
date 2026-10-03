-- 名称键由 Rust 使用 Unicode 规范化生成，唯一索引在回填后创建。
drop index if exists account_groups_name_uq;
drop index if exists client_api_keys_name_uq;
alter table account_groups add column name_key text not null default '';
alter table client_api_keys add column name_key text not null default '';
create index client_api_keys_created_idx on client_api_keys (created_at_us, id);
create index client_api_keys_last_used_idx on client_api_keys (last_used_at_us, id);
