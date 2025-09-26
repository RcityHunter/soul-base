use bytes::Bytes;
use soulbase_blob::prelude::*;
use tempfile::tempdir;
use tokio::time::Duration;

#[tokio::test]
async fn put_get_delete_roundtrip_fs() {
    let tmp = tempdir().unwrap();
    let store = FsBlobStore::new(tmp.path(), "dev-secret");

    let bucket = "artifacts";
    let key = "tenantA/reports/202501/01/u1-report.json";

    let put = store
        .put(
            bucket,
            key,
            Bytes::from_static(b"{\"ok\":true}"),
            PutOpts {
                content_type: Some("application/json".into()),
                ..Default::default()
            },
        )
        .await
        .expect("put");
    assert_eq!(put.bucket, bucket);
    assert_eq!(put.key, key);

    let head = store.head(bucket, key).await.expect("head");
    assert_eq!(head.ref_.content_type, "application/json");
    assert_eq!(head.ref_.key, key);

    let bytes = store
        .get(bucket, key, GetOpts::default())
        .await
        .expect("get");
    assert_eq!(&bytes[..], b"{\"ok\":true}");

    store.delete(bucket, key).await.expect("delete");
    let err = store
        .get(bucket, key, GetOpts::default())
        .await
        .expect_err("deleted");
    assert!(format!("{err}").contains("Object not found"));

    let snapshot = store.metrics().snapshot();
    assert_eq!(snapshot.puts, 1);
    assert_eq!(snapshot.gets, 1);
    assert_eq!(snapshot.deletes, 1);
}

#[tokio::test]
async fn presign_get_expires() {
    let tmp = tempdir().unwrap();
    let store = FsBlobStore::new(tmp.path(), "dev-secret");

    let url = store
        .presign_get(
            "b",
            "tenantB/data/202501/01/x.bin",
            PresignGetOpts { expire_secs: 1 },
        )
        .await
        .expect("presign");

    let parsed = url::Url::parse(&url.replace("fs:", "http:")).expect("parse presign");
    let exp: i64 = parsed
        .query_pairs()
        .find(|(k, _)| k == "exp")
        .map(|(_, v)| v.parse().expect("exp"))
        .expect("exp present");
    let sig = parsed
        .query_pairs()
        .find(|(k, _)| k == "sig")
        .map(|(_, v)| v.to_string())
        .expect("sig present");

    assert!(soulbase_blob::fs::presign::verify_url(
        "dev-secret",
        "GET",
        "b",
        "tenantB/data/202501/01/x.bin",
        exp,
        None,
        None,
        &sig,
    ));

    tokio::time::sleep(Duration::from_secs(2)).await;

    assert!(!soulbase_blob::fs::presign::verify_url(
        "dev-secret",
        "GET",
        "b",
        "tenantB/data/202501/01/x.bin",
        exp,
        None,
        None,
        &sig,
    ));

    let snapshot = store.metrics().snapshot();
    assert_eq!(snapshot.puts, 0);
    assert_eq!(snapshot.gets, 0);
    assert_eq!(snapshot.deletes, 0);
}

#[tokio::test]
async fn retention_rule_immediate() {
    let tmp = tempdir().unwrap();
    let store = FsBlobStore::new(tmp.path(), "dev-secret");

    let bucket = "evidence";
    let k1 = "tenantZ/screens/202501/01/a.png";
    let k2 = "tenantZ/screens/202501/01/b.png";

    store
        .put(bucket, k1, Bytes::from_static(b"1111"), PutOpts::default())
        .await
        .expect("put a");
    store
        .put(bucket, k2, Bytes::from_static(b"2222"), PutOpts::default())
        .await
        .expect("put b");

    let exec = FsRetentionExec::new(tmp.path());
    let rule = RetentionRule {
        bucket: bucket.into(),
        class: RetentionClass::Cold,
        selector: Selector {
            tenant: "tenantZ".into(),
            namespace: Some("screens".into()),
            tags: Default::default(),
        },
        ttl_days: 0,
        archive_to: None,
        version_hash: "v1".into(),
    };

    let removed = exec.apply_rule(&rule).await.expect("apply");
    assert!(removed >= 2);

    assert!(store.get(bucket, k1, GetOpts::default()).await.is_err());
    assert!(store.get(bucket, k2, GetOpts::default()).await.is_err());

    let snapshot = store.metrics().snapshot();
    assert_eq!(snapshot.puts, 2);
    assert_eq!(snapshot.gets, 0);
    assert_eq!(snapshot.deletes, 0);
}
