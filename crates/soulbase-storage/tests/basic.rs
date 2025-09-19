use serde::{Deserialize, Serialize};
use serde_json::json;
use soulbase_storage::mock::{
    InMemoryGraph, InMemoryMigrator, InMemoryRepository, InMemoryVector, MockDatastore,
};
use soulbase_storage::model::{Entity, Page, QueryParams};
use soulbase_storage::prelude::*;
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

fn query_with_tenant(tenant: &TenantId) -> QueryParams {
    QueryParams {
        filter: json!({ "tenant": tenant.0 }),
        ..Default::default()
    }
}

#[tokio::test]
async fn crud_and_select() {
    let datastore = MockDatastore::new();
    let repo: InMemoryRepository<Doc> = InMemoryRepository::new(&datastore);
    let tenant = TenantId("tenantA".into());

    let d1 = Doc {
        id: "doc:tenantA_001".into(),
        tenant: tenant.clone(),
        title: "hello".into(),
        ver: 1,
    };
    let d2 = Doc {
        id: "doc:tenantA_002".into(),
        tenant: tenant.clone(),
        title: "hi".into(),
        ver: 1,
    };

    repo.create(&tenant, &d1).await.unwrap();
    repo.create(&tenant, &d2).await.unwrap();

    let fetched = repo.get(&tenant, &d1.id).await.unwrap().unwrap();
    assert_eq!(fetched.title, "hello");

    let page: Page<Doc> = repo
        .select(&tenant, query_with_tenant(&tenant))
        .await
        .unwrap();
    assert_eq!(page.items.len(), 2);

    let updated = repo
        .upsert(
            &tenant,
            &d1.id,
            json!({ "title": "hello2", "ver": 2 }),
            None,
        )
        .await
        .unwrap();
    assert_eq!(updated.title, "hello2");
    assert_eq!(updated.ver, 2);

    repo.delete(&tenant, &d2.id).await.unwrap();
    let remaining = repo
        .select(&tenant, query_with_tenant(&tenant))
        .await
        .unwrap();
    assert_eq!(remaining.items.len(), 1);
}

#[tokio::test]
async fn graph_and_vector() {
    let datastore = MockDatastore::new();
    let repo: InMemoryRepository<Doc> = InMemoryRepository::new(&datastore);
    let graph: InMemoryGraph = InMemoryGraph::new::<Doc>(&datastore);
    let vectors: InMemoryVector = InMemoryVector::new::<Doc>(&datastore);
    let tenant = TenantId("tenantA".into());

    let a = Doc {
        id: "doc:tenantA_a".into(),
        tenant: tenant.clone(),
        title: "cat sat".into(),
        ver: 1,
    };
    let b = Doc {
        id: "doc:tenantA_b".into(),
        tenant: tenant.clone(),
        title: "cat on mat".into(),
        ver: 1,
    };

    repo.create(&tenant, &a).await.unwrap();
    repo.create(&tenant, &b).await.unwrap();

    graph
        .relate(&tenant, &a.id, "edge_like", &b.id, json!({ "at": 1 }))
        .await
        .unwrap();
    let outbound: Vec<Doc> = graph.out(&tenant, &a.id, "edge_like", 10).await.unwrap();
    assert_eq!(outbound.len(), 1);
    assert_eq!(outbound[0].id, b.id);

    vectors
        .upsert_vec(&tenant, &a.id, &[1.0, 0.0, 0.0])
        .await
        .unwrap();
    vectors
        .upsert_vec(&tenant, &b.id, &[1.0, 0.1, 0.0])
        .await
        .unwrap();
    let hits: Vec<(Doc, f32)> = vectors
        .knn(&tenant, &[1.0, 0.05, 0.0], 1, None)
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].0.id, b.id);
}

#[tokio::test]
async fn tenant_isolation() {
    let datastore = MockDatastore::new();
    let repo: InMemoryRepository<Doc> = InMemoryRepository::new(&datastore);
    let t1 = TenantId("t1".into());
    let t2 = TenantId("t2".into());

    let doc = Doc {
        id: "doc:t1_a".into(),
        tenant: t1.clone(),
        title: "A".into(),
        ver: 1,
    };

    repo.create(&t1, &doc).await.unwrap();
    let page = repo.select(&t2, query_with_tenant(&t2)).await.unwrap();
    assert!(page.items.is_empty());
}

#[tokio::test]
async fn migration_tracks_versions() {
    let migrator = InMemoryMigrator::new();
    assert_eq!(migrator.current_version().await.unwrap(), "none");

    let scripts = vec![soulbase_storage::spi::migrate::MigrationScript {
        version: "2025-09-12T15-30-00__init".into(),
        up_sql: "DEFINE TABLE doc SCHEMALESS;".into(),
        down_sql: "REMOVE TABLE doc;".into(),
        checksum: "sha256:abc".into(),
    }];
    migrator.apply_up(&scripts).await.unwrap();
    assert_eq!(
        migrator.current_version().await.unwrap(),
        "2025-09-12T15-30-00__init"
    );

    migrator.apply_down(&scripts).await.unwrap();
    assert_eq!(migrator.current_version().await.unwrap(), "none");
}
