-- 受管代理保存连接信息、位置偏好与最近探测结果；时间均为 UTC epoch 微秒。
create table outbound_proxies (
  id text primary key,
  name text not null check (length(name) between 1 and 100),
  proxy_url text not null,
  revision integer not null default 1 check (revision > 0),
  last_test_at_us integer,
  last_test_success integer check (last_test_success is null or last_test_success in (0, 1)),
  last_test_latency_ms integer check (last_test_latency_ms is null or last_test_latency_ms >= 0),
  last_test_ip text,
  last_test_ipv4 text,
  last_test_ipv6 text,
  last_test_message text,
  created_at_us integer not null,
  updated_at_us integer not null,
  location_country text,
  location_region text,
  location_city text,
  location_timezone text,
  auto_location integer not null default 0 check (auto_location in (0, 1)),
  detected_location_json text check (
    detected_location_json is null or
    (json_valid(detected_location_json) and json_type(detected_location_json) = 'object')
  ),
  last_location_detection_json text not null default '{"status":"notRequested"}'
    check (json_valid(last_location_detection_json) and json_type(last_location_detection_json) = 'object'),
  check (created_at_us <= updated_at_us),
  check (
    (location_country is null and location_region is null and location_city is null and location_timezone is null)
    or (location_country is not null and location_region is not null and location_city is not null
      and location_timezone is not null and location_country glob '[A-Z][A-Z]'
      and length(trim(location_region)) between 1 and 128
      and length(trim(location_city)) between 1 and 128 and length(location_timezone) > 0)
  )
);

create index outbound_proxies_name_idx on outbound_proxies (name collate nocase, id);

alter table provider_accounts add column outbound_proxy_id text
  references outbound_proxies(id) on update restrict on delete restrict;
create index provider_accounts_outbound_proxy_id_idx on provider_accounts (outbound_proxy_id);
