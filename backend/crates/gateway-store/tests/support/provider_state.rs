//! 两种存储后端共用的容量冻结清理合同

use std::time::{Duration, SystemTime};

use gateway_core::{
    account::{CredentialRevision, ProviderAccountId},
    provider_ports::{ProviderCooldown, ProviderCooldownKind, ProviderCooldownPort},
};

pub async fn success_cleanup_contract(repository: &dyn ProviderCooldownPort) {
    let through = CredentialRevision::new(1).unwrap();
    for (index, kind, revision, preserve) in [
        (0, Some(ProviderCooldownKind::CapacityFreeze), 1, true),
        (1, Some(ProviderCooldownKind::CapacityFreezeProbe), 1, true),
        (2, Some(ProviderCooldownKind::CapacityFreeze), 2, true),
        (3, Some(ProviderCooldownKind::RateLimit), 2, true),
        (4, Some(ProviderCooldownKind::RateLimit), 1, false),
        (5, None, 1, false),
    ] {
        let account = ProviderAccountId::new(format!("acct_cleanup_{index}")).unwrap();
        repository
            .record_capacity_failure(&account, Duration::from_secs(600), 20)
            .await
            .unwrap();
        if let Some(kind) = kind {
            repository
                .put_if_later(ProviderCooldown::new_with_kind(
                    account.clone(),
                    CredentialRevision::new(revision).unwrap(),
                    SystemTime::now() + Duration::from_secs(600),
                    kind,
                ))
                .await
                .unwrap();
        }
        repository
            .clear_after_success(&account, through)
            .await
            .unwrap();
        assert_eq!(
            repository.capacity_peak_in_flight(&account).await.unwrap(),
            preserve.then_some(20),
            "case {index}"
        );
        assert_eq!(
            repository.read(&account).await.unwrap().is_some(),
            preserve,
            "case {index}"
        );
        assert_eq!(
            repository
                .record_capacity_failure(&account, Duration::from_secs(600), 1)
                .await
                .unwrap(),
            if preserve { 2 } else { 1 },
            "case {index}"
        );
    }
}
