//! 与存储后端无关的插件私有状态键和 schema 校验规则。

use std::collections::{BTreeMap, BTreeSet};

use gateway_admin::model::plugins::state::{PluginStateConfiguration, PluginStateSchema};

pub(crate) fn valid_key(key: &str) -> bool {
    !key.is_empty()
        && key.chars().count() <= 256
        && key.len() <= 512
        && !key.chars().any(char::is_control)
}

pub(crate) fn configuration_is_valid(configuration: &PluginStateConfiguration) -> bool {
    if configuration.namespaces.len() > 16 {
        return false;
    }
    let mut namespaces = BTreeSet::new();
    for schema in &configuration.namespaces {
        let Ok(schema_bytes) = serde_json::to_vec(&schema.schema) else {
            return false;
        };
        let migrations = schema
            .migrates_from
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if !valid_namespace(&schema.namespace)
            || !namespaces.insert(&schema.namespace)
            || schema.schema_version == 0
            || schema.schema_sha256.len() != 64
            || !schema
                .schema_sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || !schema.schema.is_object()
            || schema_bytes.len() > 32 * 1024
            || schema.maximum_records == 0
            || schema.maximum_records > 10_000
            || schema.maximum_bytes == 0
            || schema.maximum_bytes > 16 * 1024 * 1024
            || schema.maximum_value_bytes == 0
            || schema.maximum_value_bytes > 256 * 1024
            || u64::from(schema.maximum_value_bytes) > schema.maximum_bytes
            || migrations.len() != schema.migrates_from.len()
            || migrations.contains(&0)
            || migrations.contains(&schema.schema_version)
        {
            return false;
        }
    }
    true
}

pub(crate) fn target_map(
    configuration: &PluginStateConfiguration,
) -> BTreeMap<&str, &PluginStateSchema> {
    configuration
        .namespaces
        .iter()
        .map(|schema| (schema.namespace.as_str(), schema))
        .collect()
}

fn valid_namespace(namespace: &str) -> bool {
    !namespace.is_empty()
        && namespace.len() <= 64
        && namespace.as_bytes()[0].is_ascii_lowercase()
        && namespace.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"-_.".contains(&byte)
        })
}
