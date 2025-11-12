use std::sync::Arc;

use soulbase_blob::FsBlobStore;
use soulbase_config::prelude::ConfigSnapshot;
use soulbase_types::tenant::TenantId;

use crate::config::{self, BlobBackendKind, BlobInfraConfig, FsBlobConfig};
use crate::errors::InfraError;

#[cfg(feature = "s3")]
use aws_sdk_s3::config::Region;
#[cfg(feature = "s3")]
use aws_sdk_s3::Client as S3Client;
#[cfg(feature = "s3")]
use soulbase_blob::s3::{S3BlobStore, S3Config};

#[derive(Clone)]
pub enum BlobStoreHandle {
    Fs(Arc<FsBlobStore>),
    #[cfg(feature = "s3")]
    S3(Arc<S3BlobStore>),
}

impl BlobStoreHandle {
    pub fn as_fs(&self) -> Option<&Arc<FsBlobStore>> {
        match self {
            BlobStoreHandle::Fs(store) => Some(store),
            #[cfg(feature = "s3")]
            BlobStoreHandle::S3(_) => None,
        }
    }

    #[cfg(feature = "s3")]
    pub fn as_s3(&self) -> Option<&Arc<S3BlobStore>> {
        match self {
            BlobStoreHandle::Fs(_) => None,
            BlobStoreHandle::S3(store) => Some(store),
        }
    }
}

#[derive(Clone)]
pub struct BlobHandle {
    pub backend: BlobBackendKind,
    pub bucket: String,
    pub key_prefix: Option<String>,
    pub store: BlobStoreHandle,
}

pub async fn build_blob_store(
    snapshot: &ConfigSnapshot,
    tenant: &TenantId,
) -> Result<BlobHandle, InfraError> {
    let infra = config::load(snapshot)?;
    build_blob_store_for(infra.blob.unwrap_or_default(), tenant).await
}

pub async fn build_blob_store_for(
    blob_cfg: BlobInfraConfig,
    tenant: &TenantId,
) -> Result<BlobHandle, InfraError> {
    match blob_cfg.backend {
        BlobBackendKind::Fs => build_fs(blob_cfg.fs.unwrap_or_default(), tenant),
        BlobBackendKind::S3 => build_s3(blob_cfg, tenant).await,
    }
}

fn build_fs(fs_cfg: FsBlobConfig, tenant: &TenantId) -> Result<BlobHandle, InfraError> {
    let store = FsBlobStore::new(&fs_cfg.root, &fs_cfg.presign_secret);
    let bucket = config::tenant_replace(&fs_cfg.bucket, tenant);
    let key_prefix = fs_cfg
        .key_prefix
        .as_ref()
        .map(|tpl| config::tenant_replace(tpl, tenant));
    Ok(BlobHandle {
        backend: BlobBackendKind::Fs,
        bucket,
        key_prefix,
        store: BlobStoreHandle::Fs(Arc::new(store)),
    })
}

#[cfg(feature = "s3")]
async fn build_s3(blob_cfg: BlobInfraConfig, tenant: &TenantId) -> Result<BlobHandle, InfraError> {
    let s3_cfg = blob_cfg
        .s3
        .clone()
        .ok_or_else(|| InfraError::config("infra.blob.s3 missing for S3 backend"))?;

    let region = Region::new(s3_cfg.region.clone());
    let shared = aws_config::from_env().region(region).load().await;
    let client = S3Client::new(&shared);

    let key_prefix = s3_cfg
        .key_prefix
        .as_ref()
        .map(|tpl| config::tenant_replace(tpl, tenant));

    let store = S3BlobStore::new(client).with_config(S3Config {
        key_prefix: key_prefix.clone(),
        enable_sse: s3_cfg.enable_sse,
    });

    Ok(BlobHandle {
        backend: BlobBackendKind::S3,
        bucket: config::tenant_replace(&s3_cfg.bucket, tenant),
        key_prefix,
        store: BlobStoreHandle::S3(Arc::new(store)),
    })
}

#[cfg(not(feature = "s3"))]
async fn build_s3(
    _blob_cfg: BlobInfraConfig,
    _tenant: &TenantId,
) -> Result<BlobHandle, InfraError> {
    Err(InfraError::feature_disabled(
        "s3",
        "enable the 's3' feature on soulbase-infra to use the S3 blob backend",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use soulbase_config::model::ProvenanceEntry;
    use soulbase_config::prelude::{ConfigSnapshot, SnapshotVersion};
    use soulbase_types::tenant::TenantId;

    fn fs_snapshot() -> ConfigSnapshot {
        let tree = serde_json::json!({
            "infra": {
                "blob": {
                    "backend": "fs",
                    "fs": {
                        "root": "var/blob",
                        "bucket": "soulbase",
                        "presign_secret": "dev-secret",
                        "key_prefix": "t/{tenant}"
                    }
                }
            }
        });
        ConfigSnapshot::from_tree(
            tree,
            SnapshotVersion("blob-test".into()),
            Vec::<ProvenanceEntry>::new(),
            None,
        )
    }

    #[tokio::test]
    async fn builds_fs_blob_store_by_default() {
        let snapshot = fs_snapshot();
        let tenant = TenantId("acme".into());
        let handle = build_blob_store(&snapshot, &tenant).await.expect("blob");
        assert_eq!(handle.backend, BlobBackendKind::Fs);
        assert_eq!(handle.bucket, "soulbase");
        assert_eq!(handle.key_prefix.unwrap(), "t/acme");
    }
}
