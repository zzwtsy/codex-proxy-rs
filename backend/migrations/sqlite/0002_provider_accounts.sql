-- Core Provider 账号事实。时间统一使用 UTC epoch 微秒，JSON 使用校验过的文本。
create table provider_accounts (
  id text primary key,
  provider_kind text not null,
  name text not null,
  email text,
  upstream_user_id text,
  upstream_account_id text,
  plan_type text,
  authentication_kind text not null,
  provider_credentials_json text not null check (json_valid(provider_credentials_json)),
  credential_revision integer not null check (credential_revision > 0),
  has_refresh_token integer not null check (has_refresh_token in (0, 1)),
  access_token_expires_at_us integer,
  next_refresh_at_us integer,
  enabled integer not null check (enabled in (0, 1)),
  concurrency_limit integer check (concurrency_limit is null or concurrency_limit between 1 and 4294967295),
  weight integer not null check (weight between 1 and 100),
  model_access_json text not null check (json_valid(model_access_json)),
  credential_state text not null check (credential_state in ('unknown', 'ready', 'expired', 'banned', 'invalid')),
  provider_quota_json text check (provider_quota_json is null or json_valid(provider_quota_json)),
  credential_observed_at_us integer not null,
  quota_observed_at_us integer,
  quota_access_state text not null check (quota_access_state in ('unknown', 'allowed', 'exhausted')),
  quota_evidence text check (quota_evidence is null or quota_evidence in ('provider_denied', 'account_limit_reached', 'usage_limit_reached', 'payment_required')),
  quota_access_observed_at_us integer,
  quota_reset_at_us integer,
  last_error_reason text check (last_error_reason is null or last_error_reason in ('account_unverified', 'access_token_expired', 'credential_expired', 'credential_invalid', 'account_banned')),
  last_error_message text,
  outbound_proxy_url text,
  created_at_us integer not null,
  updated_at_us integer not null,
  check (has_refresh_token = 1 or next_refresh_at_us is null),
  check ((provider_quota_json is null) = (quota_observed_at_us is null)),
  check (
    (quota_access_state = 'unknown' and quota_evidence is null and quota_reset_at_us is null)
    or (quota_access_state = 'allowed' and quota_evidence is null and quota_access_observed_at_us is not null and quota_reset_at_us is null)
    or (quota_access_state = 'exhausted' and quota_evidence is not null and quota_access_observed_at_us is not null)
  ),
  check (created_at_us <= updated_at_us and credential_observed_at_us <= updated_at_us)
);

create unique index provider_accounts_upstream_identity_uq
  on provider_accounts (provider_kind, upstream_user_id, coalesce(upstream_account_id, ''));
create index provider_accounts_runtime_idx
  on provider_accounts (provider_kind, enabled, id);
create index provider_accounts_refresh_idx
  on provider_accounts (provider_kind, credential_state, access_token_expires_at_us, id)
  where has_refresh_token = 1;
