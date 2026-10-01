alter table runtime_settings
  add column openai_guardian_reserved_concurrency bigint not null default 0
    check (openai_guardian_reserved_concurrency between 0 and 4294967295);
