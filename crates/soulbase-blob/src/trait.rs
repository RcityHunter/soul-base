use crate::{errors::BlobError, model::*, retention::RetentionRule};
use async_trait::async_trait;
use bytes::Bytes;
use futures_core::Stream;

#[async_trait]
pub trait BlobStore: Send + Sync {
    async fn put(
        &self,
        bucket: &str,
        key: &str,
        body: Bytes,
        opts: PutOpts,
    ) -> Result<BlobRef, BlobError>;

    async fn put_stream<S>(
        &self,
        bucket: &str,
        key: &str,
        stream: S,
        content_len: Option<u64>,
        opts: PutOpts,
    ) -> Result<BlobRef, BlobError>
    where
        S: Stream<Item = Result<Bytes, BlobError>> + Send + Unpin + 'static;

    async fn get(&self, bucket: &str, key: &str, opts: GetOpts) -> Result<Bytes, BlobError>;

    async fn head(&self, bucket: &str, key: &str) -> Result<BlobMeta, BlobError>;

    async fn delete(&self, bucket: &str, key: &str) -> Result<(), BlobError>;

    async fn presign_get(
        &self,
        bucket: &str,
        key: &str,
        opts: PresignGetOpts,
    ) -> Result<String, BlobError>;

    async fn presign_put(
        &self,
        bucket: &str,
        key: &str,
        opts: PresignPutOpts,
    ) -> Result<String, BlobError>;

    async fn multipart_begin(
        &self,
        bucket: &str,
        key: &str,
        opts: MultipartPutOpts,
    ) -> Result<MultipartInit, BlobError>;

    async fn multipart_put_part(
        &self,
        _bucket: &str,
        _key: &str,
        _upload_id: &str,
        _part_number: u32,
        _bytes: Bytes,
    ) -> Result<PartETag, BlobError> {
        Err(BlobError::unknown(
            "multipart not implemented in this adapter",
        ))
    }

    async fn multipart_complete(
        &self,
        _bucket: &str,
        _key: &str,
        _upload_id: &str,
        _parts: Vec<PartETag>,
    ) -> Result<BlobRef, BlobError> {
        Err(BlobError::unknown(
            "multipart not implemented in this adapter",
        ))
    }

    async fn multipart_abort(
        &self,
        _bucket: &str,
        _key: &str,
        _upload_id: &str,
    ) -> Result<(), BlobError> {
        Ok(())
    }
}

#[async_trait]
pub trait RetentionExec: Send + Sync {
    async fn apply_rule(&self, rule: &RetentionRule) -> Result<u64, BlobError>;
}
