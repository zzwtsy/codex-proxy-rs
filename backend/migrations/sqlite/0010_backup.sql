-- S3 备份配置与任务状态；时间均为 UTC epoch 微秒。
create table backup_settings (
  id integer primary key check (id = 1),
  storage_revision integer not null default 1 check (storage_revision > 0),
  endpoint text,
  region text,
  bucket text,
  access_key_id text,
  secret_access_key text,
  prefix text,
  force_path_style integer not null default 0 check (force_path_style in (0, 1)),
  schedule_enabled integer not null default 0 check (schedule_enabled in (0, 1)),
  cron_expression text,
  schedule_timezone text default 'Asia/Shanghai',
  retention_days integer not null default 0 check (retention_days >= 0),
  retention_count integer not null default 0 check (retention_count >= 0),
  next_run_at_us integer,
  last_verified_at_us integer,
  updated_at_us integer not null,
  check ((schedule_enabled = 0 and next_run_at_us is null) or (schedule_enabled = 1 and next_run_at_us is not null))
);
insert into backup_settings (id, updated_at_us)
values (1, cast(strftime('%s', 'now') as integer) * 1000000);

create table backup_records (
  id text primary key,
  trigger_kind text not null check (trigger_kind in ('manual', 'scheduled')),
  status text not null check (status in ('queued', 'dumping', 'uploading', 'completed', 'failed', 'deleting')),
  scheduled_at_us integer,
  object_key text not null,
  size_bytes integer,
  sha256 text,
  expires_at_us integer,
  attempt_count integer not null default 0 check (attempt_count >= 0),
  error_code text,
  error_message text,
  started_at_us integer,
  completed_at_us integer,
  created_at_us integer not null,
  updated_at_us integer not null,
  check ((trigger_kind = 'manual' and scheduled_at_us is null) or (trigger_kind = 'scheduled' and scheduled_at_us is not null)),
  check (
    (status = 'queued' and started_at_us is null and completed_at_us is null)
    or (status in ('dumping', 'uploading') and started_at_us is not null and completed_at_us is null)
    or (status in ('completed', 'failed', 'deleting') and started_at_us is not null and completed_at_us is not null)
  ),
  check (status not in ('uploading', 'completed') or (size_bytes is not null and sha256 is not null)),
  check (size_bytes is null or size_bytes >= 0),
  check (sha256 is null or (length(sha256) = 64 and sha256 not glob '*[^0-9a-f]*')),
  check ((error_code is null) = (error_message is null)),
  check (status <> 'failed' or (error_code is not null and error_message is not null)),
  check (status not in ('queued', 'dumping', 'uploading', 'completed') or error_code is null),
  check (expires_at_us is null or expires_at_us >= created_at_us),
  check (completed_at_us is null or completed_at_us >= started_at_us),
  check (created_at_us <= updated_at_us and (started_at_us is null or started_at_us >= created_at_us)
    and (completed_at_us is null or completed_at_us >= created_at_us))
);
create index backup_records_created_idx on backup_records (created_at_us desc, id desc);
create index backup_records_status_idx on backup_records (status, created_at_us, id);
create unique index backup_records_scheduled_uq on backup_records (scheduled_at_us)
  where trigger_kind = 'scheduled' and scheduled_at_us is not null;
create unique index backup_records_active_uq on backup_records ((1))
  where status in ('queued', 'dumping', 'uploading');
create unique index backup_records_object_key_uq on backup_records (object_key);
create index backup_records_scheduled_completed_idx on backup_records (completed_at_us desc, id desc)
  where trigger_kind = 'scheduled' and status = 'completed';
create index backup_records_retention_idx on backup_records (expires_at_us)
  where expires_at_us is not null and status in ('completed', 'failed');
