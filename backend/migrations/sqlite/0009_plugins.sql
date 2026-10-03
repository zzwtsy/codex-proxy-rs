-- 插件来源、不可变制品、实例配置与私有状态；UUID 使用规范文本，JSON 使用校验文本。
create table plugin_update_sources (
  plugin_id text primary key check (
    length(plugin_id) between 3 and 64 and
    plugin_id not glob '*[^a-z0-9.-]*' and
    substr(plugin_id, 1, 1) glob '[a-z0-9]' and
    instr(plugin_id, '.') > 1 and
    instr(substr(plugin_id, instr(plugin_id, '.') + 1), '.') = 0 and
    substr(plugin_id, instr(plugin_id, '.') - 1, 1) glob '[a-z0-9]' and
    substr(plugin_id, instr(plugin_id, '.') + 1, 1) glob '[a-z0-9]' and
    substr(plugin_id, -1, 1) glob '[a-z0-9]'
  ),
  source_json text not null check (json_valid(source_json) and json_type(source_json) = 'object'),
  policy_json text not null default '{"kind":"manual"}'
    check (json_valid(policy_json) and json_type(policy_json) = 'object'),
  outbound_proxy_id text references outbound_proxies(id) on update restrict on delete restrict
);
create index plugin_update_sources_outbound_proxy_id_idx on plugin_update_sources (outbound_proxy_id);

create table plugin_artifacts (
  sha256 text primary key check (length(sha256) = 64 and sha256 not glob '*[^0-9a-f]*'),
  plugin_id text not null references plugin_update_sources(plugin_id) on update restrict on delete restrict,
  version text not null check (length(version) between 1 and 128),
  metadata_json text not null check (json_valid(metadata_json) and json_type(metadata_json) = 'object'),
  source_json text not null check (json_valid(source_json) and json_type(source_json) = 'object'),
  outbound_proxy_id text references outbound_proxies(id) on update restrict on delete restrict,
  archive blob not null check (length(archive) between 1 and 33554432),
  installed_at_us integer not null,
  accepted_at_us integer
);
create index plugin_artifacts_plugin_id_idx on plugin_artifacts (plugin_id);
create index plugin_artifacts_outbound_proxy_id_idx on plugin_artifacts (outbound_proxy_id);

create table plugin_artifact_platforms (
  plugin_id text not null,
  version text not null,
  platform text not null,
  artifact_sha256 text not null references plugin_artifacts(sha256) on update restrict on delete cascade,
  primary key (plugin_id, version, platform)
);
create index plugin_artifact_platforms_artifact_sha256_idx on plugin_artifact_platforms (artifact_sha256);

create table plugin_source_credentials (
  id text primary key,
  info_json text not null check (json_valid(info_json) and json_type(info_json) = 'object'),
  secret_json text not null check (json_valid(secret_json) and json_type(secret_json) = 'object')
);

create table plugin_artifact_credentials (
  artifact_sha256 text not null references plugin_artifacts(sha256) on update restrict on delete cascade,
  credential_id text not null references plugin_source_credentials(id) on update restrict on delete restrict,
  primary key (artifact_sha256, credential_id)
);
create index plugin_artifact_credentials_credential_id_idx on plugin_artifact_credentials (credential_id);

create table plugin_instances (
  id text primary key,
  artifact_sha256 text not null references plugin_artifacts(sha256) on update restrict on delete restrict,
  name text not null check (length(name) between 1 and 128),
  enabled integer not null check (enabled in (0, 1)),
  configuration_json text not null check (
    json_valid(configuration_json) and json_type(configuration_json) = 'object' and length(configuration_json) <= 65536
  ),
  bindings_json text not null check (json_valid(bindings_json) and json_type(bindings_json) = 'array'),
  revision integer not null check (revision > 0)
);
create index plugin_instances_artifact_sha256_idx on plugin_instances (artifact_sha256);

create table plugin_instance_secrets (
  instance_id text primary key references plugin_instances(id) on update restrict on delete cascade,
  secrets_json text not null check (
    json_valid(secrets_json) and json_type(secrets_json) = 'object' and length(secrets_json) <= 65536
  )
);

create table plugin_version_configurations (
  instance_id text not null references plugin_instances(id) on update restrict on delete cascade,
  artifact_sha256 text not null references plugin_artifacts(sha256) on update restrict on delete cascade,
  configuration_json text not null check (json_valid(configuration_json) and json_type(configuration_json) = 'object'),
  secrets_json text not null check (json_valid(secrets_json) and json_type(secrets_json) = 'object'),
  bindings_json text not null check (json_valid(bindings_json) and json_type(bindings_json) = 'array'),
  primary key (instance_id, artifact_sha256)
);

create table plugin_state_generations (
  id text primary key,
  transition_id text,
  instance_id text not null references plugin_instances(id) on update restrict on delete cascade,
  namespace text not null check (length(namespace) between 1 and 64 and substr(namespace, 1, 1) glob '[a-z]'
      and namespace not glob '*[^a-z0-9_.-]*'),
  artifact_sha256 text not null references plugin_artifacts(sha256) on update restrict on delete restrict,
  schema_version integer not null check (schema_version between 1 and 4294967295),
  schema_sha256 text not null check (length(schema_sha256) = 64 and schema_sha256 not glob '*[^0-9a-f]*'),
  maximum_records integer not null check (maximum_records between 1 and 10000),
  maximum_bytes integer not null check (maximum_bytes between 1 and 16777216),
  maximum_value_bytes integer not null check (
    maximum_value_bytes between 1 and 262144 and maximum_value_bytes <= maximum_bytes
  ),
  instance_revision integer not null check (instance_revision > 0),
  fence text not null,
  status text not null check (status in ('staging', 'active')),
  source_generation_id text references plugin_state_generations(id) on update restrict on delete restrict,
  next_record_version integer not null default 1 check (next_record_version > 0),
  record_count integer not null default 0 check (record_count between 0 and maximum_records),
  total_bytes integer not null default 0 check (total_bytes between 0 and maximum_bytes),
  migration_cursor text,
  migration_complete integer not null default 0 check (migration_complete in (0, 1)),
  created_at_us integer not null,
  promoted_at_us integer,
  unique (instance_id, namespace, status),
  unique (transition_id, namespace),
  check (source_generation_id is null or source_generation_id <> id),
  check (
    (status = 'staging' and transition_id is not null)
    or (status = 'active' and transition_id is null and source_generation_id is null
      and migration_cursor is null and migration_complete = 1 and promoted_at_us is not null)
  )
);
create index plugin_state_generations_artifact_sha256_idx on plugin_state_generations (artifact_sha256);
create index plugin_state_generations_source_generation_id_idx on plugin_state_generations (source_generation_id);

create table plugin_state_records (
  generation_id text not null references plugin_state_generations(id) on update restrict on delete cascade,
  state_key text collate binary not null check (
    length(state_key) between 1 and 256 and length(cast(state_key as blob)) <= 512
      and state_key not glob '*[' || char(1) || '-' || char(31) || ']*'
  ),
  value_json text not null check (json_valid(value_json)),
  value_bytes integer not null check (value_bytes between 1 and 262144),
  record_version integer not null check (record_version > 0),
  primary key (generation_id, state_key)
);

create table authorization_receipts (
  provider_kind text not null,
  flow_digest text not null check (length(flow_digest) = 64 and flow_digest not glob '*[^0-9a-f]*'),
  owner_digest text not null check (length(owner_digest) = 64 and owner_digest not glob '*[^0-9a-f]*'),
  config_revision integer not null check (config_revision > 0),
  accounts_json text not null check (
    json_valid(accounts_json) and json_type(accounts_json) = 'array'
      and json_array_length(accounts_json) between 1 and 200 and length(accounts_json) <= 65536
  ),
  created_at_us integer not null,
  expires_at_us integer not null,
  primary key (provider_kind, flow_digest),
  check (expires_at_us > created_at_us)
);
create index authorization_receipts_expiry_idx on authorization_receipts (expires_at_us);

create table plugin_group_resources (
  instance_id text not null references plugin_instances(id) on update restrict on delete cascade,
  resource_key text not null check (length(resource_key) between 1 and 64 and substr(resource_key, 1, 1) glob '[a-z0-9]'
    and resource_key not glob '*[^a-z0-9_.-]*'),
  group_id text not null unique references account_groups(id) on update restrict on delete cascade,
  primary key (instance_id, resource_key)
);

create table plugin_key_resources (
  instance_id text not null references plugin_instances(id) on update restrict on delete cascade,
  resource_key text not null check (length(resource_key) between 1 and 64 and substr(resource_key, 1, 1) glob '[a-z0-9]'
    and resource_key not glob '*[^a-z0-9_.-]*'),
  key_id text not null unique references client_api_keys(id) on update restrict on delete cascade,
  primary key (instance_id, resource_key)
);
