use std::collections::BTreeMap;

use sqlx::{Row, SqlitePool, sqlite::SqliteRow};

use super::{name_key::normalize_name_key, sqlite_unavailable};
use crate::{StoreError, StoreResult};

const NAME_KEY_INDEX_VERSION: i64 = 13;

#[derive(Clone, Copy)]
struct NameTable {
    name: &'static str,
    columns_sql: &'static str,
    select_sql: &'static str,
    update_sql: &'static str,
}

const NAME_TABLES: [NameTable; 2] = [
    NameTable {
        name: "account groups",
        columns_sql: "pragma table_info(account_groups)",
        select_sql: "select id, name from account_groups order by id",
        update_sql: "update account_groups set name_key = ?2 where id = ?1",
    },
    NameTable {
        name: "Client API Keys",
        columns_sql: "pragma table_info(client_api_keys)",
        select_sql: "select id, name from client_api_keys order by id",
        update_sql: "update client_api_keys set name_key = ?2 where id = ?1",
    },
];

struct StoredName {
    id: String,
    name: String,
}

pub(super) async fn is_pending(pool: &SqlitePool) -> StoreResult<bool> {
    let has_migrations_table = sqlx::query_scalar::<_, i64>(
        "select exists(
           select 1 from sqlite_master where type = 'table' and name = '_sqlx_migrations'
         )",
    )
    .fetch_one(pool)
    .await
    .map_err(|_| sqlite_unavailable("check SQLite name-key migration state"))?;
    if has_migrations_table == 0 {
        return Ok(true);
    }
    let applied = sqlx::query_scalar::<_, i64>(
        "select exists(
           select 1 from _sqlx_migrations where version = ?1 and success = 1
         )",
    )
    .bind(NAME_KEY_INDEX_VERSION)
    .fetch_one(pool)
    .await
    .map_err(|_| sqlite_unavailable("check SQLite name-key migration state"))?;
    Ok(applied == 0)
}

pub(super) async fn preflight(pool: &SqlitePool) -> StoreResult<()> {
    let mut conflicts = Vec::new();
    for table in NAME_TABLES {
        let Some(rows) = read_table_names(pool, table).await? else {
            continue;
        };
        conflicts.extend(find_conflicts(table.name, &rows));
    }
    if conflicts.is_empty() {
        Ok(())
    } else {
        Err(conflict_error(conflicts))
    }
}

pub(super) async fn backfill(pool: &SqlitePool) -> StoreResult<()> {
    let mut transaction = pool
        .begin()
        .await
        .map_err(|_| sqlite_unavailable("begin SQLite name-key backfill"))?;
    let mut table_rows = Vec::with_capacity(NAME_TABLES.len());
    let mut conflicts = Vec::new();
    for table in NAME_TABLES {
        let rows = sqlx::query(table.select_sql)
            .fetch_all(&mut *transaction)
            .await
            .map_err(|_| sqlite_unavailable("read SQLite names for backfill"))?;
        let rows = decode_names(rows)?;
        conflicts.extend(find_conflicts(table.name, &rows));
        table_rows.push((table, rows));
    }
    if !conflicts.is_empty() {
        transaction
            .rollback()
            .await
            .map_err(|_| sqlite_unavailable("rollback SQLite name-key backfill"))?;
        return Err(conflict_error(conflicts));
    }
    for (table, rows) in table_rows {
        for row in rows {
            sqlx::query(table.update_sql)
                .bind(row.id)
                .bind(normalize_name_key(&row.name))
                .execute(&mut *transaction)
                .await
                .map_err(|_| sqlite_unavailable("write SQLite normalized name"))?;
        }
    }
    transaction
        .commit()
        .await
        .map_err(|_| sqlite_unavailable("commit SQLite name-key backfill"))
}

async fn read_table_names(
    pool: &SqlitePool,
    table: NameTable,
) -> StoreResult<Option<Vec<StoredName>>> {
    let columns = sqlx::query(table.columns_sql)
        .fetch_all(pool)
        .await
        .map_err(|_| sqlite_unavailable("inspect SQLite name table"))?;
    let has_name = columns.iter().any(|row| {
        row.try_get::<String, _>("name")
            .is_ok_and(|column| column == "name")
    });
    if !has_name {
        return Ok(None);
    }
    let rows = sqlx::query(table.select_sql)
        .fetch_all(pool)
        .await
        .map_err(|_| sqlite_unavailable("read SQLite names before migration"))?;
    decode_names(rows).map(Some)
}

fn decode_names(rows: Vec<SqliteRow>) -> StoreResult<Vec<StoredName>> {
    rows.into_iter()
        .map(|row| {
            Ok(StoredName {
                id: row
                    .try_get("id")
                    .map_err(|_| sqlite_unavailable("read SQLite name ID"))?,
                name: row
                    .try_get("name")
                    .map_err(|_| sqlite_unavailable("read SQLite name"))?,
            })
        })
        .collect()
}

fn find_conflicts(entity: &str, rows: &[StoredName]) -> Vec<String> {
    let mut normalized_names = BTreeMap::<String, Vec<(&str, &str)>>::new();
    for row in rows {
        let key = normalize_name_key(&row.name);
        if !key.is_empty() {
            normalized_names
                .entry(key)
                .or_default()
                .push((&row.id, &row.name));
        }
    }
    normalized_names
        .into_values()
        .filter(|records| records.len() > 1)
        .map(|records| {
            let records = records
                .into_iter()
                .map(|(id, name)| format!("{id} ({name:?})"))
                .collect::<Vec<_>>()
                .join(", ");
            format!("{entity}: {records}")
        })
        .collect()
}

fn conflict_error(conflicts: Vec<String>) -> StoreError {
    StoreError::InvalidData {
        entity: "SQLite migration",
        message: format!(
            "Unicode-normalized names conflict; no names were changed. Rename these records and retry: {}",
            conflicts.join("; ")
        ),
    }
}
