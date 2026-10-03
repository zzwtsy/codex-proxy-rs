//! `VACUUM INTO` SQLite 一致性快照与本地暂存适配器。

use std::sync::Arc;

use async_trait::async_trait;
use sha2::{Digest as _, Sha256};
use tokio::io::AsyncReadExt as _;

use gateway_admin::model::backup::{BackupArchiveFormat, BackupError, code};
use gateway_admin::ports::backup::{DatabaseDumpPort, DumpArtifact, DumpRequest, StagedArtifact};

use super::staging::StagingArea;

const SQLITE_EXTENSION: &str = "sqlite3";

/// SQLite 快照适配器。
pub struct SqliteDumpAdapter {
    pool: sqlx::SqlitePool,
    staging: Arc<StagingArea>,
}

impl SqliteDumpAdapter {
    /// 组合 SQLite 连接池与受控暂存区。
    #[must_use]
    pub fn new(pool: sqlx::SqlitePool, staging: Arc<StagingArea>) -> Self {
        Self { pool, staging }
    }
}

#[async_trait]
impl DatabaseDumpPort for SqliteDumpAdapter {
    fn archive_format(&self) -> BackupArchiveFormat {
        BackupArchiveFormat::Sqlite
    }

    async fn dump(&self, request: DumpRequest) -> Result<DumpArtifact, BackupError> {
        if request.cancellation.is_cancelled() {
            self.staging
                .cleanup_with_extension(&request.backup_id, SQLITE_EXTENSION);
            return Err(cancelled());
        }
        self.staging
            .ensure_capacity()
            .map_err(|_| staging_space_exhausted())?;
        let partial = self
            .staging
            .partial_path_with_extension(&request.backup_id, SQLITE_EXTENSION);
        let final_path = self
            .staging
            .final_path_with_extension(&request.backup_id, SQLITE_EXTENSION);
        self.staging
            .cleanup_with_extension(&request.backup_id, SQLITE_EXTENSION);

        let destination = partial.to_string_lossy().into_owned();
        let pool = self.pool.clone();
        let mut snapshot = tokio::spawn(async move {
            sqlx::query("VACUUM INTO ?")
                .bind(destination)
                .execute(&pool)
                .await
                .map(|_| ())
        });
        let snapshot_result = tokio::select! {
            result = &mut snapshot => match result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(_)) => Err(sqlite_snapshot_failed("创建 SQLite 一致性快照失败")),
                Err(_) => Err(sqlite_snapshot_failed("SQLite 快照任务异常退出")),
            },
            _ = request.cancellation.cancelled() => {
                // VACUUM 运行期间不可中断；等待连接释放后再删除文件，避免留下后台写入。
                let _ = snapshot.await;
                self.staging
                    .cleanup_with_extension(&request.backup_id, SQLITE_EXTENSION);
                return Err(cancelled());
            }
        };
        if let Err(error) = snapshot_result {
            self.staging
                .cleanup_with_extension(&request.backup_id, SQLITE_EXTENSION);
            return Err(error);
        }

        let result = async {
            let file = tokio::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&partial)
                .await
                .map_err(|_| sqlite_snapshot_failed("无法打开 SQLite 快照文件"))?;
            file.sync_all()
                .await
                .map_err(|_| sqlite_snapshot_failed("同步 SQLite 快照文件失败"))?;
            let metadata = file
                .metadata()
                .await
                .map_err(|_| sqlite_snapshot_failed("读取 SQLite 快照文件信息失败"))?;
            if !metadata.is_file() || metadata.len() > self.staging.max_archive_bytes() {
                return Err(staging_space_exhausted());
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;
                tokio::fs::set_permissions(&partial, std::fs::Permissions::from_mode(0o600))
                    .await
                    .map_err(|_| sqlite_snapshot_failed("设置 SQLite 快照权限失败"))?;
            }
            let sha256 = hash_file(&partial).await?;
            let size_bytes = metadata.len();
            tokio::fs::rename(&partial, &final_path)
                .await
                .map_err(|_| sqlite_snapshot_failed("发布 SQLite 快照失败"))?;
            Ok(DumpArtifact {
                path: final_path.clone(),
                size_bytes,
                sha256,
            })
        }
        .await;
        if result.is_err() {
            self.staging
                .cleanup_with_extension(&request.backup_id, SQLITE_EXTENSION);
        }
        result
    }

    async fn inspect_staging(
        &self,
        backup_id: &str,
    ) -> Result<Option<StagedArtifact>, BackupError> {
        let path = self
            .staging
            .final_path_with_extension(backup_id, SQLITE_EXTENSION);
        let metadata = match tokio::fs::metadata(&path).await {
            Ok(metadata) if metadata.is_file() => metadata,
            _ => return Ok(None),
        };
        let sha256 = hash_file(&path).await?;
        Ok(Some(StagedArtifact {
            path,
            size_bytes: metadata.len(),
            sha256,
        }))
    }

    async fn cleanup_staging(&self, backup_id: &str) -> Result<(), BackupError> {
        self.staging
            .cleanup_with_extension(backup_id, SQLITE_EXTENSION);
        Ok(())
    }
}

async fn hash_file(path: &std::path::Path) -> Result<String, BackupError> {
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|_| sqlite_snapshot_failed("打开 SQLite 快照文件失败"))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let size = file
            .read(&mut buffer)
            .await
            .map_err(|_| sqlite_snapshot_failed("读取 SQLite 快照文件失败"))?;
        if size == 0 {
            break;
        }
        hasher.update(&buffer[..size]);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn sqlite_snapshot_failed(message: &'static str) -> BackupError {
    BackupError::new(code::SQLITE_SNAPSHOT_FAILED, message.to_owned())
}

fn staging_space_exhausted() -> BackupError {
    BackupError::new(
        code::STAGING_SPACE_EXHAUSTED,
        "暂存磁盘空间不足或快照超过单任务上限".to_owned(),
    )
}

fn cancelled() -> BackupError {
    BackupError::new(code::CANCELLED, "备份快照已取消".to_owned())
}
