mod tests {
    use std::collections::BTreeMap;

    use gateway_admin::model::pricing::{PricingChange, StoredPricing, UpdatePricing};
    use gateway_admin::model::settings::ReplaceRuntimeSettings;
    use gateway_admin::model::{MutationActor, MutationContext, Revision};
    use gateway_core::metering::ModelPriceOverride;
    use gateway_store::{SqliteStoreConfig, sqlite};

    fn context(request_id: &str) -> MutationContext {
        MutationContext {
            actor: MutationActor::System,
            request_id: request_id.to_owned(),
        }
    }

    fn runtime_update(expected_revision: u64) -> ReplaceRuntimeSettings {
        ReplaceRuntimeSettings {
            expected_revision: Revision::new(expected_revision).unwrap(),
            request_profile_updates: BTreeMap::new(),
            request_location_enabled: false,
            request_location: Default::default(),
            model_mappings: BTreeMap::new(),
            refresh_margin_seconds: 3_600,
            refresh_concurrency: 2,
            max_concurrent_per_account: 3,
            request_interval_ms: 50,
            max_waiting_per_key: 0,
            max_waiting_per_account: 0,
            concurrency_wait_timeout_seconds: 30,
            openai_guardian_reserved_concurrency: 0,
            responses_max_decompressed_body_bytes: 64 * 1024 * 1024,
            smart_scheduling: Default::default(),
            rotation_strategy: gateway_core::account::RotationStrategy::Smart,
            min_codex_desktop_version: None,
            min_codex_cli_version: None,
            usage_retention_days: 31,
            ops_event_retention_days: 30,
            audit_retention_days: 90,
            account_auto_freeze_enabled: false,
            account_auto_freeze_threshold: 12,
            account_auto_freeze_window_seconds: 600,
            account_auto_freeze_duration_seconds: 7_200,
            account_auto_freeze_probe_enabled: true,
            account_auto_freeze_probe_model: None,
            account_auto_freeze_adaptive_concurrency: true,
            account_warmup_enabled: false,
            account_warmup_schedule_time: "08:00".to_owned(),
            account_warmup_model: None,
        }
    }

    fn sample_price() -> ModelPriceOverride {
        serde_json::from_value(serde_json::json!({
            "multiplierBps": 20000,
            "bands": {
                "standard": {
                    "input": "1.25",
                    "output": "2",
                    "cacheRead": "0.2",
                    "cacheWrite": "0.1"
                }
            }
        }))
        .expect("valid model price")
    }

    #[tokio::test]
    async fn pricing_and_admin_key_changes_advance_revision_and_audit_atomically() {
        let root = tempfile::tempdir().expect("SQLite directory");
        let pool = sqlite::connect_and_migrate(
            &root.path().join("admin-settings.sqlite3"),
            &SqliteStoreConfig::default(),
        )
        .await
        .expect("migrate SQLite database");
        let repository = sqlite::admin_settings_store(pool.clone());

        assert_eq!(
            repository.load_pricing().await.unwrap(),
            StoredPricing::default()
        );
        let price = sample_price();
        let revision = repository
            .update_pricing(
                UpdatePricing {
                    provider: "openai".to_owned(),
                    models: vec!["model-a".to_owned()],
                    change: PricingChange::Replace(price.clone()),
                },
                &context("pricing.update"),
            )
            .await
            .unwrap();
        assert_eq!(revision.get(), 2);
        assert_eq!(
            repository.load_pricing().await.unwrap().overrides["openai"]["model-a"],
            price
        );

        let mut provider_changes = BTreeMap::new();
        provider_changes.insert("model-b".to_owned(), Some(sample_price()));
        let mut changes = BTreeMap::new();
        changes.insert("openai".to_owned(), provider_changes);
        let revision = repository
            .sync_pricing(changes, &context("pricing.sync"))
            .await
            .unwrap();
        assert_eq!(revision.get(), 3);
        let stored = repository.load_pricing().await.unwrap();
        assert!(stored.synced.contains_key("openai"));
        assert!(stored.synced_at.is_some());

        let revision = repository
            .replace_admin_api_key(
                gateway_admin::model::settings::AdminApiKey::new("test-key"),
                &context("admin_api_key.changed"),
            )
            .await
            .unwrap();
        assert_eq!(revision.config_revision.get(), 4);
        assert!(repository.admin_api_key_exists().await.unwrap());

        let audited: Vec<(String, i64)> = sqlx::query_as(
            "select action, config_revision from admin_audit_events order by created_at_us, id",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(audited.len(), 3);
        assert_eq!(
            audited
                .iter()
                .map(|(_, revision)| *revision)
                .collect::<Vec<_>>(),
            [2, 3, 4]
        );
        pool.close().await;
    }

    #[tokio::test]
    async fn runtime_settings_replacement_uses_revision_cas_and_rejects_overflow() {
        let root = tempfile::tempdir().expect("SQLite directory");
        let pool = sqlite::connect_and_migrate(
            &root.path().join("admin-settings-cas.sqlite3"),
            &SqliteStoreConfig::default(),
        )
        .await
        .expect("migrate SQLite database");
        let repository = sqlite::admin_settings_store(pool.clone());

        let saved = repository
            .replace_runtime_settings(runtime_update(1), &context("settings.replace"))
            .await
            .unwrap();
        assert_eq!(saved.config_revision.get(), 2);

        let stale = repository
            .replace_runtime_settings(runtime_update(1), &context("settings.replace"))
            .await
            .unwrap_err();
        assert_eq!(
            stale.kind(),
            gateway_admin::ports::store::AdminStoreErrorKind::StaleRevision
        );
        let events: i64 = sqlx::query_scalar("select count(*) from admin_audit_events")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(events, 1);

        sqlx::query(
            "update runtime_settings set config_revision = 9223372036854775807 where id = 1",
        )
        .execute(&pool)
        .await
        .unwrap();
        let overflow = repository
            .replace_runtime_settings(
                runtime_update(i64::MAX as u64),
                &context("settings.replace"),
            )
            .await
            .unwrap_err();
        assert_eq!(
            overflow.kind(),
            gateway_admin::ports::store::AdminStoreErrorKind::Invalid
        );
        pool.close().await;
    }

    #[tokio::test]
    async fn invalid_pricing_rolls_back_without_advancing_revision() {
        let root = tempfile::tempdir().expect("SQLite directory");
        let pool = sqlite::connect_and_migrate(
            &root.path().join("admin-settings-invalid.sqlite3"),
            &SqliteStoreConfig::default(),
        )
        .await
        .expect("migrate SQLite database");
        let repository = sqlite::admin_settings_store(pool.clone());
        let error = repository
            .update_pricing(
                UpdatePricing {
                    provider: "openai".to_owned(),
                    models: vec!["model-a".to_owned()],
                    change: PricingChange::Replace(ModelPriceOverride {
                        multiplier_bps: 1_000_001,
                        bands: BTreeMap::new(),
                    }),
                },
                &context("pricing.update"),
            )
            .await
            .unwrap_err();
        assert_eq!(
            error.kind(),
            gateway_admin::ports::store::AdminStoreErrorKind::Invalid
        );
        let revision: i64 =
            sqlx::query_scalar("select config_revision from runtime_settings where id = 1")
                .fetch_one(&pool)
                .await
                .unwrap();
        let audits: i64 = sqlx::query_scalar("select count(*) from admin_audit_events")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(revision, 1);
        assert_eq!(audits, 0);
        pool.close().await;
    }
}
