#![cfg(feature = "surreal")]

use soulbase_storage::spi::migrate::MigrationScript;
use soulbase_storage::surreal::{SurrealConfig, SurrealDatastore};
use soulbase_storage::{Datastore, HealthCheck, Migrator, Session, Transaction};

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
