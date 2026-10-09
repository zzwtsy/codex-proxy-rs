//! 验证候选复用与最后一个代次持有者释放后的回收

use gateway_admin::model::Revision;
use gateway_admin::ports::plugins::PluginPreparation;
use gateway_core::routing::ConfigRevision;
use gateway_core::routing::extensions::ExtensionPreparationPort;

#[tokio::test]
async fn candidate_is_reused_after_commit_and_released_when_no_generation_owner_remains() {
    let (cache, store, runtime) = super::super::setup().await;
    let mut snapshot = store.snapshot.lock().unwrap().clone();
    snapshot.instances[0].configuration =
        serde_json::from_str(r#"{"z":1,"a":{"z":2,"a":3}}"#).unwrap();
    let candidate = PluginPreparation::prepare(&runtime, snapshot.clone())
        .await
        .unwrap();
    assert!(
        runtime.observer_registry().resolve(&candidate).is_none(),
        "没有观察绑定时不应编译派发计划"
    );
    snapshot.instances[0].configuration =
        serde_json::from_str(r#"{"a":{"a":3,"z":2},"z":1}"#).unwrap();
    snapshot.config_revision = Revision::new(2).unwrap();
    *store.snapshot.lock().unwrap() = snapshot;
    let published = ExtensionPreparationPort::prepare(&runtime, ConfigRevision::new(2).unwrap())
        .await
        .unwrap();
    assert_eq!(candidate.id(), published.id());
    assert!(published.is_ready());
    drop(candidate);
    assert_eq!(std::fs::read_dir(cache.path()).unwrap().count(), 1);
    drop(published);
    super::super::wait_until_empty(cache.path()).await;
}

fn starts(path: &std::path::Path) -> usize {
    std::fs::read_to_string(path).unwrap().lines().count()
}

#[tokio::test]
async fn updating_b_reuses_a_and_c_until_their_last_set_is_released() {
    // 两种释放顺序都必须保住另一集合仍使用的实例
    for release_old_first in [true, false] {
        let (cache, store, runtime) = super::super::setup().await;
        let markers = tempfile::tempdir().unwrap();
        let mut snapshot = store.snapshot.lock().unwrap().clone();
        let template = snapshot.instances[0].clone();
        snapshot.instances = ["a", "b", "c"]
            .into_iter()
            .map(|id| {
                let mut instance = template.clone();
                instance.id = id.into();
                instance.configuration =
                    serde_json::json!({"startup_marker": markers.path().join(id)});
                instance
            })
            .collect();
        let old = PluginPreparation::prepare(&runtime, snapshot.clone())
            .await
            .unwrap();
        snapshot.config_revision = Revision::new(2).unwrap();
        snapshot.instances[1].revision = snapshot.config_revision;
        let new = PluginPreparation::prepare(&runtime, snapshot)
            .await
            .unwrap();
        assert_ne!(old.id(), new.id());
        assert_eq!(std::fs::read_dir(cache.path()).unwrap().count(), 4);
        assert_eq!(
            [
                starts(&markers.path().join("a")),
                starts(&markers.path().join("b")),
                starts(&markers.path().join("c"))
            ],
            [1, 2, 1]
        );
        assert!(old.is_ready());
        assert!(new.is_ready());
        let retained = if release_old_first {
            drop(old);
            new
        } else {
            drop(new);
            old
        };
        // 被释放集合的独用 B 关闭并移除缓存，A/C 与另一份 B 仍在
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            while std::fs::read_dir(cache.path()).unwrap().count() != 3 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(retained.is_ready());
        drop(retained);
        super::super::wait_until_empty(cache.path()).await;
    }
}

#[tokio::test]
async fn failed_partial_candidate_does_not_close_reused_instances() {
    let (cache, store, runtime) =
        super::super::setup_with_contributions(gateway_plugin_sdk::Contributions::from([
            crate::support::contribution(
                gateway_plugin_sdk::Capability::Observer,
                vec![gateway_plugin_sdk::Stage::Observation],
                vec![],
                vec![],
            ),
        ]))
        .await;
    let markers = tempfile::tempdir().unwrap();
    let mut snapshot = store.snapshot.lock().unwrap().clone();
    let marker = markers.path().join("a");
    snapshot.instances[0].configuration = serde_json::json!({"startup_marker": marker});
    let old = PluginPreparation::prepare(&runtime, snapshot.clone())
        .await
        .unwrap();
    let mut other = snapshot.instances[0].clone();
    other.id = "b".into();
    other.configuration = serde_json::json!({"registration_mismatch": true});
    other.revision = Revision::new(2).unwrap();
    snapshot.config_revision = other.revision;
    snapshot.instances.push(other);
    assert!(
        PluginPreparation::prepare(&runtime, snapshot.clone())
            .await
            .is_err()
    );
    assert_eq!(starts(&marker), 1);
    assert!(old.is_ready());
    snapshot.instances[1].configuration = serde_json::json!({});
    let repaired = PluginPreparation::prepare(&runtime, snapshot)
        .await
        .unwrap();
    assert_eq!(starts(&marker), 1);
    drop(old);
    assert!(repaired.is_ready());
    drop(repaired);
    super::super::wait_until_empty(cache.path()).await;
}

#[tokio::test]
async fn targeted_quiescence_stops_the_shared_revision_without_stopping_its_replacement() {
    use gateway_admin::ports::plugins::PluginStateLifecycle;
    let (cache, store, runtime) = super::super::setup().await;
    let mut snapshot = store.snapshot.lock().unwrap().clone();
    let previous = snapshot.instances[0].clone();
    let old = PluginPreparation::prepare(&runtime, snapshot.clone())
        .await
        .unwrap();
    let mut other = previous.clone();
    other.id = "other".into();
    other.revision = Revision::new(2).unwrap();
    snapshot.config_revision = other.revision;
    snapshot.instances.push(other);
    let shared = PluginPreparation::prepare(&runtime, snapshot.clone())
        .await
        .unwrap();
    snapshot.config_revision = Revision::new(3).unwrap();
    snapshot.instances[0].revision = snapshot.config_revision;
    let new = PluginPreparation::prepare(&runtime, snapshot.clone())
        .await
        .unwrap();
    runtime
        .quiesce_instance(&previous.id, &previous.artifact_sha256, previous.revision)
        .await;
    assert!(!old.is_ready());
    assert!(!shared.is_ready());
    assert!(new.is_ready());
    runtime.shutdown().await;
    assert!(!new.is_ready());
    drop(old);
    drop(shared);
    drop(new);
    super::super::wait_until_empty(cache.path()).await;
}
