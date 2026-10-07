alter table model_requests rename column raw_upstream_error to error_details;
alter table ops_events rename column raw_upstream_error to error_details;

alter table provider_session_aliases
  add column root_session_key text;

alter table runtime_settings
  add column openai_account_affinity text not null default 'strict'
    check (openai_account_affinity in ('relaxed', 'preferred', 'strict'));

alter table runtime_settings
  add column max_account_rotations integer not null default 3
    check (max_account_rotations between 0 and 31);

alter table runtime_settings
  add column openai_session_affinity_ttl_hours integer not null default 24
    check (openai_session_affinity_ttl_hours between 1 and 720);

update runtime_settings
   set openai_account_affinity = 'relaxed'
 where config_revision > 1;
