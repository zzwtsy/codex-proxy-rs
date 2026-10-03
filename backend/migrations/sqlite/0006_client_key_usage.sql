-- 将 Client API Key 的最近成功使用时间纳入 SQLite 持久状态。
alter table client_api_keys add column last_used_at_us integer;
