-- 名称键经 Rust 回填后才建立唯一约束；空键只用于兼容测试和历史的未命名记录。
create unique index account_groups_name_key_uq
  on account_groups (name_key) where name_key <> '';
create unique index client_api_keys_name_key_uq
  on client_api_keys (name_key) where name_key <> '';
