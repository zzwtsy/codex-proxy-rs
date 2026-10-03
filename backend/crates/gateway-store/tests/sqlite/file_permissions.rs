use std::{
    fs,
    os::unix::fs::{PermissionsExt, symlink},
    path::{Path, PathBuf},
    process::Command,
};

use gateway_store::{SqliteStoreConfig, sqlite};

fn files(path: &Path) -> [PathBuf; 3] {
    let companion = |suffix: &str| {
        let mut name = path.as_os_str().to_os_string();
        name.push(suffix);
        PathBuf::from(name)
    };
    [path.to_path_buf(), companion("-wal"), companion("-shm")]
}

fn mode(path: &Path) -> u32 {
    fs::metadata(path).unwrap().permissions().mode() & 0o777
}

#[tokio::test]
async fn sqlite_files_are_private_under_permissive_umask() {
    const CHILD: &str = "CPR_TEST_SQLITE_PERMISSIONS_CHILD";
    if std::env::var_os(CHILD).is_none() {
        // umask 属于进程级状态，只在独立测试进程修改，避免影响并行测试。
        let status = Command::new("sh")
            .args(["-c", "umask 022; exec \"$@\"", "sqlite-permissions"])
            .arg(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "sqlite::file_permissions::sqlite_files_are_private_under_permissive_umask",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .status()
            .unwrap();
        assert!(status.success());
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("data/private.sqlite3");
    let config = SqliteStoreConfig::default();
    let pool = sqlite::connect_and_migrate(&path, &config).await.unwrap();
    for file in files(&path) {
        assert_eq!(mode(&file), 0o600);
    }
    sqlx::query("create table fixture (value text not null)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("insert into fixture values ('preserved')")
        .execute(&pool)
        .await
        .unwrap();

    for file in files(&path) {
        fs::set_permissions(file, fs::Permissions::from_mode(0o644)).unwrap();
    }
    let inspection = sqlite::connect_read_only(&path, &config).await.unwrap();
    for file in files(&path) {
        assert_eq!(mode(&file), 0o644);
    }
    inspection.close().await;

    let alias = root.path().join("alias.sqlite3");
    symlink(&path, &alias).unwrap();
    for _ in 0..2 {
        let reopened = sqlite::connect_and_migrate(&alias, &config).await.unwrap();
        for file in files(&path) {
            assert_eq!(mode(&file), 0o600);
        }
        let value: String = sqlx::query_scalar("select value from fixture")
            .fetch_one(&reopened)
            .await
            .unwrap();
        assert_eq!(value, "preserved");
        reopened.close().await;
    }
    pool.close().await;
}

#[tokio::test]
async fn sqlite_rejects_non_regular_files_before_migration() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("blocked.sqlite3");
    fs::File::create(&path).unwrap();
    fs::create_dir(files(&path)[1].clone()).unwrap();
    let result = sqlite::connect_and_migrate(&path, &SqliteStoreConfig::default()).await;
    assert!(result.is_err());
    assert_eq!(fs::metadata(&path).unwrap().len(), 0);
    assert!(!files(&path)[2].exists());
    assert!(
        sqlite::connect_and_migrate(root.path(), &SqliteStoreConfig::default())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn sqlite_permission_failure_does_not_open_database() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("unreadable.sqlite3");
    fs::File::create(&path).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o000)).unwrap();
    // root 可绕过权限检查；此场景只适用于普通运行用户。
    if fs::File::open(&path).is_ok() {
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        return;
    }
    let result = sqlite::connect_and_migrate(&path, &SqliteStoreConfig::default()).await;
    assert!(result.is_err());
    assert_eq!(fs::metadata(&path).unwrap().len(), 0);
    assert!(!files(&path)[1].exists());
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
}
