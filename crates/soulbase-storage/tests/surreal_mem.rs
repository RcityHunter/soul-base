#![cfg(feature = "surreal")]

use serde::{Deserialize, Serialize};
use serde_json::json;
use soulbase_storage::model::{Entity, QueryParams};
use soulbase_storage::spi::migrate::MigrationScript;
use soulbase_storage::surreal::{SurrealConfig, SurrealDatastore, SurrealRepository};
use soulbase_storage::{Datastore, HealthCheck, Migrator, Repository, Session, Transaction};
use soulbase_types::prelude::TenantId;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct Doc {
    id: String,
    tenant: TenantId,
    title: String,
    ver: u32,
}

impl Entity for Doc {
    const TABLE: &'static str = "doc";

    fn id(&self) -> &str {
        &self.id
    }

    fn tenant(&self) -> &TenantId {
        &self.tenant
    }
}

#[tokio::test]
async fn surreal_session_smoke() {
    let datastore = SurrealDatastore::connect(SurrealConfig::default())
        .await
        .expect("connect surreal");

    datastore.ping().await.expect("ping");

    let session = datastore.session().await.expect("session");
    let mut tx = session.begin().await.expect("begin tx");
    tx.commit().await.expect("commit");

    datastore.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn surreal_migrator_tracks_versions() {
    let datastore = SurrealDatastore::connect(SurrealConfig::default())
        .await
        .expect("connect surreal");
    let migrator = datastore.migrator();

    let version = "2025-09-20T12-00-00__surreal_test".to_string();
    let script = MigrationScript {
        version: version.clone(),
        up_sql: "DEFINE TABLE migtest SCHEMALESS".into(),
        down_sql: "REMOVE TABLE migtest".into(),
        checksum: "sha256:surreal-check".into(),
    };

    assert_eq!(migrator.current_version().await.unwrap(), "none");
    migrator
        .apply_up(std::slice::from_ref(&script))
        .await
        .unwrap();
    assert_eq!(migrator.current_version().await.unwrap(), version);
    migrator.apply_down(&[script]).await.unwrap();
    assert_eq!(migrator.current_version().await.unwrap(), "none");

    datastore.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn surreal_repository_crud_flow() {
    let datastore = SurrealDatastore::connect(SurrealConfig::default())
        .await
        .expect("connect surreal");
    let repo: SurrealRepository<Doc> = SurrealRepository::new(&datastore);
    let tenant = TenantId("tenant-surreal".into());

    let doc = Doc {
        id: "doc:tenant-surreal_001".into(),
        tenant: tenant.clone(),
        title: "hello surreal".into(),
        ver: 1,
    };

    repo.create(&tenant, &doc).await.expect("create");

    let fetched = repo
        .get(&tenant, &doc.id)
        .await
        .expect("get")
        .expect("doc present");
    assert_eq!(fetched.title, "hello surreal");

    let updated = repo
        .upsert(
            &tenant,
            &doc.id,
            json!({ "title": "hello updated", "ver": 2 }),
            None,
        )
        .await
        .expect("upsert");
    assert_eq!(updated.title, "hello updated");
    assert_eq!(updated.ver, 2);

    let params = QueryParams {
        filter: json!({ "tenant": tenant.0.clone() }),
        order_by: Some("id ASC".into()),
        limit: Some(10),
        cursor: None,
    };
    let page = repo.select(&tenant, params).await.expect("select");
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].id, doc.id);

    repo.delete(&tenant, &doc.id).await.expect("delete");
    let gone = repo.get(&tenant, &doc.id).await.expect("get after delete");
    assert!(gone.is_none());

    datastore.shutdown().await.expect("shutdown");
}
