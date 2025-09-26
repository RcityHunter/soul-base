#![cfg(feature = "s3-test-suite")]

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::Client;
use bytes::Bytes;
use chrono::Utc;
use soulbase_blob::prelude::*;
use std::collections::BTreeMap;
use tokio::time::Duration;

fn test_bucket() -> Option<String> {
    std::env::var("AWS_S3_TEST_BUCKET")
        .ok()
        .filter(|s| !s.is_empty())
}

fn test_prefix() -> Option<String> {
    std::env::var("AWS_S3_TEST_PREFIX")
        .ok()
        .filter(|s| !s.is_empty())
}

async fn build_client() -> Client {
    let region = RegionProviderChain::default_provider().or_else("us-east-1");
    let config = aws_config::from_env().region(region).load().await;
    Client::new(&config)
}

#[tokio::test]
async fn s3_put_head_get_delete_roundtrip() {
    let Some(bucket) = test_bucket() else {
        eprintln!("skipping s3_put_head_get_delete_roundtrip; set AWS_S3_TEST_BUCKET to run");
        return;
    };

    let client = build_client().await;
    let mut config = S3Config::default();
    config.key_prefix = test_prefix();

    let store = S3BlobStore::new(client).with_config(config);

    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let key = format!(
        "tenant-it/tests/{}/{:x}",
        Utc::now().timestamp_millis(),
        nonce
    );

    let payload = Bytes::from_static(b"{\"hello\":true}");
    let put = store
        .put(
            &bucket,
            &key,
            payload.clone(),
            PutOpts {
                content_type: Some("application/json".into()),
                user_tags: Some(BTreeMap::from([("env".into(), "ci".into())])),
                ..Default::default()
            },
        )
        .await
        .expect("put");
    assert_eq!(put.bucket, bucket);
    assert_eq!(put.key, key);

    let head = store.head(&bucket, &key).await.expect("head");
    assert_eq!(head.ref_.content_type, "application/json");
    assert_eq!(head.ref_.key, key);

    let err = store
        .get(
            &bucket,
            &key,
            GetOpts {
                range: None,
                if_none_match: Some(head.ref_.etag.clone()),
            },
        )
        .await
        .expect_err("should respond not modified");
    assert!(format!("{err}").contains("not modified"));

    let full = store
        .get(
            &bucket,
            &key,
            GetOpts {
                range: None,
                if_none_match: None,
            },
        )
        .await
        .expect("get");
    assert_eq!(payload, full);

    store.delete(&bucket, &key).await.expect("delete");

    let presign = store
        .presign_get(&bucket, &key, PresignGetOpts { expire_secs: 60 })
        .await
        .expect("presign get");
    assert!(presign.contains(&bucket));

    tokio::time::sleep(Duration::from_millis(50)).await;
}
