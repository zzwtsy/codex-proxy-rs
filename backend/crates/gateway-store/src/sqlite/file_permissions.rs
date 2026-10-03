//! 在 SQLite 打开数据库前收紧权限，避免主库与伴随文件暴露凭据。

use std::{
    fs::{self, File, OpenOptions},
    io,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

use crate::StoreResult;

use super::sqlite_unavailable;

pub(super) fn prepare(path: &Path) -> StoreResult<PathBuf> {
    match OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
    {
        Ok(file) => restrict(&file)?,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(_) => return Err(sqlite_unavailable("create private SQLite database file")),
    }
    // SQLite 使用真实数据库路径定位 WAL/SHM；别名路径不能留下未保护的伴随文件。
    let path =
        fs::canonicalize(path).map_err(|_| sqlite_unavailable("resolve SQLite database file"))?;
    secure_existing(&path, false)?;
    for suffix in ["-wal", "-shm"] {
        let mut companion = path.as_os_str().to_os_string();
        companion.push(suffix);
        secure_existing(Path::new(&companion), true)?;
    }
    Ok(path)
}

fn secure_existing(path: &Path, optional: bool) -> StoreResult<()> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if optional && error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(_) => return Err(sqlite_unavailable("inspect SQLite file permissions")),
    };
    if !metadata.is_file() {
        return Err(sqlite_unavailable("SQLite path must be a regular file"));
    }
    let file = match File::open(path) {
        Ok(file) => file,
        // 已有连接关闭时 SQLite 可自行移除伴随文件。
        Err(error) if optional && error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(_) => return Err(sqlite_unavailable("open SQLite file for permission repair")),
    };
    restrict(&file)
}

fn restrict(file: &File) -> StoreResult<()> {
    if !file
        .metadata()
        .map_err(|_| sqlite_unavailable("inspect opened SQLite file"))?
        .is_file()
    {
        return Err(sqlite_unavailable("SQLite path must be a regular file"));
    }
    file.set_permissions(fs::Permissions::from_mode(0o600))
        .map_err(|_| sqlite_unavailable("restrict SQLite file permissions"))
}
