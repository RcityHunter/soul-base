#![cfg(feature = "stores-surreal")]

use soulbase_auth::prelude::*;
use soulbase_storage::surreal::{SurrealConfig, SurrealDatastore};
use soulbase_storage::Datastore;
use soulbase_types::prelude::*;

async fn connect_datastore() -> SurrealDatastore {
    SurrealDatastore::connect(SurrealConfig::default())
        .await
        .expect("connect surreal")
}

#[tokio::test]
async fn surreal_consent_store_roundtrip() {
    let datastore = connect_datastore().await;
    let store = SurrealConsentStore::new(&datastore);
    let verifier = ConsentVerifierWithStore::new(store.clone());

    let request = AuthzRequest {
        subject: Subject {
            kind: SubjectKind::User,
            subject_id: Id("user-1".into()),
            tenant: TenantId("tenant-x".into()),
            claims: serde_json::Map::new(),
        },
        resource: ResourceUrn("urn:demo".into()),
        action: Action::Read,
        attrs: serde_json::Value::Null,
        consent: None,
        correlation_id: None,
    };
    let consent = Consent {
        scopes: vec![Scope {
            resource: "urn:demo".into(),
            action: "Read".into(),
            attrs: serde_json::Map::new(),
        }],
        expires_at: None,
        purpose: Some("demo".into()),
    };

    assert!(verifier.verify(&consent, &request).await.unwrap());
    let stored = store
        .load(&TenantId("tenant-x".into()), "user-1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.purpose.as_deref(), Some("demo"));

    datastore.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn surreal_quota_store_enforces_limit() {
    let datastore = connect_datastore().await;
    let store = SurrealQuotaStore::new(&datastore, QuotaConfig { default_limit: 5 });

    let key = QuotaKey {
        tenant: TenantId("tenant-q".into()),
        subject_id: Id("user-q".into()),
        resource: ResourceUrn("urn:quota".into()),
        action: Action::Invoke,
    };

    assert!(matches!(
        store.check_and_consume(&key, 3).await.unwrap(),
        QuotaOutcome::Allowed
    ));
    assert!(matches!(
        store.check_and_consume(&key, 3).await.unwrap(),
        QuotaOutcome::BudgetExceeded
    ));

    datastore.shutdown().await.expect("shutdown");
}
