#[cfg(feature = "backend-s3")]
mod real {
    use crate::errors::BlobError;
    use crate::key::ensure_key;
    use crate::metrics::BlobStats;
    use crate::model::{
        BlobMeta, BlobRef, Digest, GetOpts, MultipartInit, MultipartPutOpts, PresignGetOpts,
        PresignPutOpts, PutOpts,
    };
    use crate::r#trait::BlobStore;
    use async_trait::async_trait;
    use aws_sdk_s3::operation::get_object::builders::GetObjectFluentBuilder;
    use aws_sdk_s3::operation::put_object::builders::PutObjectFluentBuilder;
    use aws_sdk_s3::presigning::PresigningConfig;
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::ServerSideEncryption;
    use aws_sdk_s3::Client;
    use bytes::Bytes;
    use chrono::Utc;
    use futures_util::StreamExt;
    use soulbase_crypto::{DefaultDigester, Digester as _};
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;
    use std::time::Duration as StdDuration;
    use urlencoding::encode;

    #[derive(Clone, Debug, Default)]
    pub struct S3Config {
        pub key_prefix: Option<String>,
        pub enable_sse: bool,
    }

    #[derive(Clone)]
    pub struct S3BlobStore {
        client: Client,
        config: Arc<S3Config>,
        metrics: BlobStats,
    }

    impl S3BlobStore {
        pub fn new(client: Client) -> Self {
            Self {
                client,
                config: Arc::new(S3Config::default()),
                metrics: BlobStats::default(),
            }
        }

        pub fn with_config(mut self, config: S3Config) -> Self {
            self.config = Arc::new(config);
            self
        }

        pub fn with_metrics(mut self, metrics: BlobStats) -> Self {
            self.metrics = metrics;
            self
        }

        pub fn metrics(&self) -> &BlobStats {
            &self.metrics
        }

        fn full_key(&self, key: &str) -> String {
            match &self.config.key_prefix {
                Some(prefix) if !prefix.is_empty() => {
                    let normalized = prefix.trim_matches('/');
                    if normalized.is_empty() {
                        key.to_string()
                    } else {
                        format!("{normalized}/{key}")
                    }
                }
                _ => key.to_string(),
            }
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

        fn build_metadata(
            digest: &Digest,
            opts: &PutOpts,
            created_at_ms: i64,
        ) -> HashMap<String, String> {
            let mut metadata = HashMap::new();
            metadata.insert("created_at_ms".into(), created_at_ms.to_string());
            metadata.insert("sha256_b64".into(), digest.b64.clone());
            if let Some(envelope) = &opts.envelope_id {
                metadata.insert("envelope_id".into(), envelope.clone());
            }
            metadata
        }

        fn serialize_tags(tags: &Option<BTreeMap<String, String>>) -> Option<String> {
            tags.as_ref().map(|map| {
                map.iter()
                    .map(|(k, v)| format!("{k}={}", encode(v)))
                    .collect::<Vec<_>>()
                    .join("&")
            })
        }

        fn sanitize_etag(etag: Option<&str>) -> Option<String> {
            etag.map(|value| value.trim_matches('"').to_string())
        }

        fn normalize_if_none_match(if_none_match: &Option<String>) -> Option<String> {
            if_none_match
                .as_ref()
                .map(|v| v.trim_matches('"').to_string())
        }

        async fn presign_get_uri(
            &self,
            request: GetObjectFluentBuilder,
            expire_secs: u32,
        ) -> Result<String, BlobError> {
            let config =
                PresigningConfig::expires_in(StdDuration::from_secs(expire_secs as u64))
                    .map_err(|err| BlobError::schema(&format!("invalid presign config: {err}")))?;
            let presigned = request.presigned(config).await.map_err(|err| {
                BlobError::provider_unavailable(&format!("presign failed: {err}"))
            })?;
            Ok(presigned.uri().to_string())
        }

        async fn presign_put_uri(
            &self,
            request: PutObjectFluentBuilder,
            expire_secs: u32,
        ) -> Result<String, BlobError> {
            let config =
                PresigningConfig::expires_in(StdDuration::from_secs(expire_secs as u64))
                    .map_err(|err| BlobError::schema(&format!("invalid presign config: {err}")))?;
            let presigned = request.presigned(config).await.map_err(|err| {
                BlobError::provider_unavailable(&format!("presign failed: {err}"))
            })?;
            Ok(presigned.uri().to_string())
        }
    }

    #[async_trait]
    impl BlobStore for S3BlobStore {
        async fn put(
            &self,
            bucket: &str,
            key: &str,
            body: Bytes,
            opts: PutOpts,
        ) -> Result<BlobRef, BlobError> {
            let tenant = key.split('/').next().unwrap_or_default();
            ensure_key(tenant, key).map_err(|err| BlobError::schema(&err))?;

            let full_key = self.full_key(key);
            let (etag_hex, digest) = Self::digest(&body)?;
            let created_at_ms = Utc::now().timestamp_millis();
            let content_type = opts
                .content_type
                .clone()
                .unwrap_or_else(|| "application/octet-stream".into());

            let metadata = Self::build_metadata(&digest, &opts, created_at_ms);

            let mut request = self
                .client
                .put_object()
                .bucket(bucket)
                .key(full_key)
                .body(ByteStream::from(body.clone().to_vec()))
                .content_type(content_type.clone());

            for (k, v) in metadata {
                request = request.metadata(k, v);
            }

            if let Some(tagging) = Self::serialize_tags(&opts.user_tags) {
                request = request.tagging(tagging);
            }

            if self.config.enable_sse && opts.encrypt {
                request = request.server_side_encryption(ServerSideEncryption::AwsKms);
            }

            request.send().await.map_err(|err| {
                BlobError::provider_unavailable(&format!("put_object failed: {err}"))
            })?;

            self.metrics.record_put();

            Ok(BlobRef {
                bucket: bucket.to_string(),
                key: key.to_string(),
                etag: etag_hex,
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
            content_len: Option<u64>,
            opts: PutOpts,
        ) -> Result<BlobRef, BlobError>
        where
            S: futures_core::Stream<Item = Result<Bytes, BlobError>> + Send + Unpin + 'static,
        {
            let mut buffer = Vec::with_capacity(content_len.unwrap_or(0) as usize);
            while let Some(chunk) = stream.next().await {
                buffer.extend_from_slice(&chunk?);
            }
            self.put(bucket, key, Bytes::from(buffer), opts).await
        }

        async fn get(&self, bucket: &str, key: &str, opts: GetOpts) -> Result<Bytes, BlobError> {
            let full_key = self.full_key(key);
            let mut request = self
                .client
                .get_object()
                .bucket(bucket)
                .key(full_key.clone());

            if let Some((start, end)) = opts.range {
                request = request.range(format!("bytes={start}-{end}"));
            }

            let response = request.send().await.map_err(|err| {
                BlobError::provider_unavailable(&format!("get_object failed: {err}"))
            })?;

            if let Some(expected) = Self::normalize_if_none_match(&opts.if_none_match) {
                if let Some(etag) = Self::sanitize_etag(response.e_tag()) {
                    if etag == expected {
                        return Err(BlobError::provider_unavailable("not modified"));
                    }
                }
            }

            let collected = response.body.collect().await.map_err(|err| {
                BlobError::provider_unavailable(&format!("read body failed: {err}"))
            })?;

            self.metrics.record_get();
            Ok(collected.into_bytes())
        }

        async fn head(&self, bucket: &str, key: &str) -> Result<BlobMeta, BlobError> {
            let full_key = self.full_key(key);
            let response = self
                .client
                .head_object()
                .bucket(bucket)
                .key(full_key)
                .send()
                .await
                .map_err(|err| {
                    BlobError::provider_unavailable(&format!("head_object failed: {err}"))
                })?;

            let size = response.content_length().unwrap_or_default() as u64;
            let content_type = response
                .content_type()
                .map(|s| s.to_string())
                .unwrap_or_else(|| "application/octet-stream".into());
            let etag = Self::sanitize_etag(response.e_tag()).unwrap_or_default();
            let metadata = response.metadata();
            let created_at_ms = metadata
                .and_then(|m| m.get("created_at_ms"))
                .and_then(|v| v.parse::<i64>().ok())
                .unwrap_or_else(|| Utc::now().timestamp_millis());

            Ok(BlobMeta {
                ref_: BlobRef {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    etag,
                    size,
                    content_type,
                    created_at_ms,
                },
                md5_b64: metadata.and_then(|m| m.get("sha256_b64")).cloned(),
                user_tags: None,
                storage_class: response.storage_class().map(|sc| sc.as_ref().to_string()),
            })
        }

        async fn delete(&self, bucket: &str, key: &str) -> Result<(), BlobError> {
            let full_key = self.full_key(key);
            self.client
                .delete_object()
                .bucket(bucket)
                .key(full_key)
                .send()
                .await
                .map_err(|err| {
                    BlobError::provider_unavailable(&format!("delete_object failed: {err}"))
                })?;
            self.metrics.record_delete();
            Ok(())
        }

        async fn presign_get(
            &self,
            bucket: &str,
            key: &str,
            opts: PresignGetOpts,
        ) -> Result<String, BlobError> {
            let full_key = self.full_key(key);
            let request = self.client.get_object().bucket(bucket).key(full_key);
            self.presign_get_uri(request, opts.expire_secs).await
        }

        async fn presign_put(
            &self,
            bucket: &str,
            key: &str,
            opts: PresignPutOpts,
        ) -> Result<String, BlobError> {
            let full_key = self.full_key(key);
            let mut request = self.client.put_object().bucket(bucket).key(full_key);
            if let Some(ct) = &opts.content_type {
                request = request.content_type(ct.clone());
            }
            self.presign_put_uri(request, opts.expire_secs).await
        }

        async fn multipart_begin(
            &self,
            _bucket: &str,
            _key: &str,
            _opts: MultipartPutOpts,
        ) -> Result<MultipartInit, BlobError> {
            Err(BlobError::provider_unavailable(
                "multipart upload not yet implemented for S3 adapter",
            ))
        }
    }
}

#[cfg(feature = "backend-s3")]
pub use real::{S3BlobStore, S3Config};

#[cfg(not(feature = "backend-s3"))]
mod stub {
    use crate::errors::BlobError;
    use crate::metrics::BlobStats;
    use crate::model::{
        BlobMeta, BlobRef, GetOpts, MultipartInit, MultipartPutOpts, PresignGetOpts,
        PresignPutOpts, PutOpts,
    };
    use crate::r#trait::BlobStore;
    use async_trait::async_trait;
    use bytes::Bytes;

    #[derive(Clone, Debug, Default)]
    pub struct S3Config {
        pub key_prefix: Option<String>,
        pub enable_sse: bool,
    }

    #[derive(Clone, Debug, Default)]
    pub struct S3BlobStore {
        metrics: BlobStats,
    }

    impl S3BlobStore {
        pub fn with_metrics(mut self, metrics: BlobStats) -> Self {
            self.metrics = metrics;
            self
        }

        pub fn metrics(&self) -> &BlobStats {
            &self.metrics
        }
    }

    #[async_trait]
    impl BlobStore for S3BlobStore {
        async fn put(
            &self,
            _bucket: &str,
            _key: &str,
            _body: Bytes,
            _opts: PutOpts,
        ) -> Result<BlobRef, BlobError> {
            Err(BlobError::provider_unavailable(
                "S3 adapter compiled in stub mode (enable backend-s3 feature)",
            ))
        }

        async fn put_stream<S>(
            &self,
            _bucket: &str,
            _key: &str,
            _stream: S,
            _content_len: Option<u64>,
            _opts: PutOpts,
        ) -> Result<BlobRef, BlobError>
        where
            S: futures_core::Stream<Item = Result<Bytes, BlobError>> + Send + Unpin + 'static,
        {
            Err(BlobError::provider_unavailable(
                "S3 adapter compiled in stub mode (enable backend-s3 feature)",
            ))
        }

        async fn get(&self, _bucket: &str, _key: &str, _opts: GetOpts) -> Result<Bytes, BlobError> {
            Err(BlobError::provider_unavailable(
                "S3 adapter compiled in stub mode (enable backend-s3 feature)",
            ))
        }

        async fn head(&self, _bucket: &str, _key: &str) -> Result<BlobMeta, BlobError> {
            Err(BlobError::provider_unavailable(
                "S3 adapter compiled in stub mode (enable backend-s3 feature)",
            ))
        }

        async fn delete(&self, _bucket: &str, _key: &str) -> Result<(), BlobError> {
            Err(BlobError::provider_unavailable(
                "S3 adapter compiled in stub mode (enable backend-s3 feature)",
            ))
        }

        async fn presign_get(
            &self,
            _bucket: &str,
            _key: &str,
            _opts: PresignGetOpts,
        ) -> Result<String, BlobError> {
            Err(BlobError::provider_unavailable(
                "S3 adapter compiled in stub mode (enable backend-s3 feature)",
            ))
        }

        async fn presign_put(
            &self,
            _bucket: &str,
            _key: &str,
            _opts: PresignPutOpts,
        ) -> Result<String, BlobError> {
            Err(BlobError::provider_unavailable(
                "S3 adapter compiled in stub mode (enable backend-s3 feature)",
            ))
        }

        async fn multipart_begin(
            &self,
            _bucket: &str,
            _key: &str,
            _opts: MultipartPutOpts,
        ) -> Result<MultipartInit, BlobError> {
            Err(BlobError::provider_unavailable(
                "S3 adapter compiled in stub mode (enable backend-s3 feature)",
            ))
        }
    }
}

#[cfg(not(feature = "backend-s3"))]
pub use stub::{S3BlobStore, S3Config};
