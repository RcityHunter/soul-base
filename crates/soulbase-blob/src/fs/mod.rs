pub mod presign;

use crate::{
    errors::BlobError,
    key::ensure_key,
    metrics::BlobStats,
    model::{
        BlobMeta, BlobRef, Digest, GetOpts, MultipartInit, MultipartPutOpts, PresignGetOpts,
        PresignPutOpts, PutOpts,
    },
    r#trait::BlobStore,
};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use futures_util::stream::StreamExt;
use serde::{Deserialize, Serialize};
use soulbase_crypto::{DefaultDigester, Digester as _};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

#[derive(Clone)]
pub struct FsBlobStore {
    pub root: PathBuf,
    pub presign_secret: String,
    metrics: BlobStats,
}

impl FsBlobStore {
    pub fn new(root: impl Into<PathBuf>, secret: &str) -> Self {
        Self {
            root: root.into(),
            presign_secret: secret.to_string(),
            metrics: BlobStats::default(),
        }
    }

    pub fn with_metrics(mut self, metrics: BlobStats) -> Self {
        self.metrics = metrics;
        self
    }

    pub fn metrics(&self) -> &BlobStats {
        &self.metrics
    }

    fn object_path(&self, bucket: &str, key: &str) -> PathBuf {
        self.root.join(bucket).join(key)
    }

    fn meta_path(&self, bucket: &str, key: &str) -> PathBuf {
        self.root.join(bucket).join(format!("{key}.meta.json"))
    }

    fn ensure_dirs(path: &Path) -> Result<(), BlobError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|err| BlobError::provider_unavailable(&format!("mkdirs: {err}")))?;
        }
        Ok(())
    }

    fn digest(bytes: &[u8]) -> Result<(String, Digest), BlobError> {
        let digester = DefaultDigester::default();
        let digest = digester
            .sha256(bytes)
            .map_err(|err| BlobError::unknown(&format!("digest failed: {err}")))?;
        let hex = digest
            .as_bytes()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        Ok((hex, Digest::from(digest)))
    }

    fn read_meta(path: &Path) -> Option<FsMeta> {
        fs::read(path)
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct FsMeta {
    content_type: String,
    created_at_ms: i64,
    size: u64,
    etag: String,
    digest: Option<Digest>,
    envelope_id: Option<String>,
}

impl FsMeta {
    fn new(
        content_type: String,
        created_at_ms: i64,
        size: u64,
        etag: String,
        digest: Digest,
        envelope_id: Option<String>,
    ) -> Self {
        Self {
            content_type,
            created_at_ms,
            size,
            etag,
            digest: Some(digest),
            envelope_id,
        }
    }
}

#[async_trait]
impl BlobStore for FsBlobStore {
    async fn put(
        &self,
        bucket: &str,
        key: &str,
        body: Bytes,
        opts: PutOpts,
    ) -> Result<BlobRef, BlobError> {
        let tenant = key.split('/').next().unwrap_or_default();
        ensure_key(tenant, key).map_err(|err| BlobError::schema(&err))?;

        let object_path = self.object_path(bucket, key);
        let meta_path = self.meta_path(bucket, key);

        Self::ensure_dirs(object_path.as_path())?;
        Self::ensure_dirs(meta_path.as_path())?;

        let temp_path = object_path.with_extension("uploading");
        {
            let mut file = fs::File::create(&temp_path)
                .map_err(|err| BlobError::provider_unavailable(&format!("create: {err}")))?;
            file.write_all(&body)
                .map_err(|err| BlobError::provider_unavailable(&format!("write: {err}")))?;
            let _ = file.sync_all();
        }
        fs::rename(&temp_path, &object_path)
            .map_err(|err| BlobError::provider_unavailable(&format!("rename: {err}")))?;

        let (etag, digest) = Self::digest(&body)?;
        let content_type = opts
            .content_type
            .clone()
            .unwrap_or_else(|| "application/octet-stream".into());
        let created_at_ms = Utc::now().timestamp_millis();

        let fs_meta = FsMeta::new(
            content_type.clone(),
            created_at_ms,
            body.len() as u64,
            etag.clone(),
            digest,
            opts.envelope_id,
        );
        let meta_bytes = serde_json::to_vec(&fs_meta)
            .map_err(|err| BlobError::unknown(&format!("serialize meta: {err}")))?;
        fs::write(&meta_path, meta_bytes)
            .map_err(|err| BlobError::provider_unavailable(&format!("write meta: {err}")))?;

        self.metrics.record_put();

        Ok(BlobRef {
            bucket: bucket.to_string(),
            key: key.to_string(),
            etag,
            size: body.len() as u64,
            content_type,
            created_at_ms,
        })
    }

    async fn put_stream<S>(
        &self,
        bucket: &str,
        key: &str,
        mut stream: S,
        _content_len: Option<u64>,
        opts: PutOpts,
    ) -> Result<BlobRef, BlobError>
    where
        S: futures_core::Stream<Item = Result<Bytes, BlobError>> + Send + Unpin + 'static,
    {
        let mut buffer = Vec::new();
        while let Some(chunk) = stream.next().await {
            buffer.extend_from_slice(&chunk?);
        }
        self.put(bucket, key, Bytes::from(buffer), opts).await
    }

    async fn get(&self, bucket: &str, key: &str, opts: GetOpts) -> Result<Bytes, BlobError> {
        let object_path = self.object_path(bucket, key);
        let data = fs::read(&object_path).map_err(|_| BlobError::not_found("object not found"))?;

        if let Some(if_none_match) = opts.if_none_match {
            let (etag, _) = Self::digest(&data)?;
            if etag == if_none_match {
                return Err(BlobError::provider_unavailable("not modified (dev stub)"));
            }
        }

        let bytes = if let Some((start, end)) = opts.range {
            let len = data.len() as u64;
            if start >= len {
                Bytes::new()
            } else {
                let safe_end = end.min(len.saturating_sub(1)) as usize;
                let safe_start = start.min(safe_end as u64) as usize;
                Bytes::from(data[safe_start..=safe_end].to_vec())
            }
        } else {
            Bytes::from(data)
        };

        self.metrics.record_get();
        Ok(bytes)
    }

    async fn head(&self, bucket: &str, key: &str) -> Result<BlobMeta, BlobError> {
        let object_path = self.object_path(bucket, key);
        let meta_path = self.meta_path(bucket, key);
        let bytes = fs::read(&object_path).map_err(|_| BlobError::not_found("object not found"))?;
        let (etag, digest) = Self::digest(&bytes)?;

        let meta = Self::read_meta(&meta_path).unwrap_or_else(|| FsMeta {
            content_type: "application/octet-stream".into(),
            created_at_ms: Utc::now().timestamp_millis(),
            size: bytes.len() as u64,
            etag: etag.clone(),
            digest: Some(digest.clone()),
            envelope_id: None,
        });

        Ok(BlobMeta {
            ref_: BlobRef {
                bucket: bucket.to_string(),
                key: key.to_string(),
                etag,
                size: bytes.len() as u64,
                content_type: meta.content_type,
                created_at_ms: meta.created_at_ms,
            },
            md5_b64: meta.digest.as_ref().map(|d| d.b64.clone()),
            user_tags: None,
            storage_class: None,
        })
    }

    async fn delete(&self, bucket: &str, key: &str) -> Result<(), BlobError> {
        let object_path = self.object_path(bucket, key);
        let meta_path = self.meta_path(bucket, key);
        fs::remove_file(&object_path).map_err(|_| BlobError::not_found("object not found"))?;
        let _ = fs::remove_file(&meta_path);
        self.metrics.record_delete();
        Ok(())
    }

    async fn presign_get(
        &self,
        bucket: &str,
        key: &str,
        opts: PresignGetOpts,
    ) -> Result<String, BlobError> {
        presign::presign_get(&self.presign_secret, bucket, key, opts.expire_secs)
    }

    async fn presign_put(
        &self,
        bucket: &str,
        key: &str,
        opts: PresignPutOpts,
    ) -> Result<String, BlobError> {
        presign::presign_put(
            &self.presign_secret,
            bucket,
            key,
            opts.expire_secs,
            opts.content_type,
            opts.size_hint,
        )
    }

    async fn multipart_begin(
        &self,
        bucket: &str,
        key: &str,
        _opts: MultipartPutOpts,
    ) -> Result<MultipartInit, BlobError> {
        let hint = self
            .head(bucket, key)
            .await
            .unwrap_or_else(|_| BlobMeta {
                ref_: BlobRef {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    etag: String::new(),
                    size: 0,
                    content_type: "application/octet-stream".into(),
                    created_at_ms: Utc::now().timestamp_millis(),
                },
                md5_b64: None,
                user_tags: None,
                storage_class: None,
            })
            .ref_;
        Ok(MultipartInit {
            upload_id: format!("mp-{}", hint.key),
            ref_hint: hint,
        })
    }
}

#[allow(dead_code)]
fn _assert_send_sync() {
    fn assert_traits<T: Send + Sync>() {}
    assert_traits::<FsBlobStore>();
}
