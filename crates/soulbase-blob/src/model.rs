use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlobRef {
    pub bucket: String,
    pub key: String,
    pub etag: String,
    pub size: u64,
    pub content_type: String,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlobMeta {
    pub ref_: BlobRef,
    pub md5_b64: Option<String>,
    pub user_tags: Option<BTreeMap<String, String>>,
    pub storage_class: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Digest {
    pub algo: String,
    pub b64: String,
    pub size: u64,
}

impl From<soulbase_crypto::Digest> for Digest {
    fn from(value: soulbase_crypto::Digest) -> Self {
        Digest {
            algo: value.algo,
            b64: value.b64,
            size: value.size as u64,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct PutOpts {
    pub content_type: Option<String>,
    pub ttl_days: Option<u32>,
    pub encrypt: bool,
    pub user_tags: Option<BTreeMap<String, String>>,
    pub envelope_id: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct GetOpts {
    pub range: Option<(u64, u64)>,
    pub if_none_match: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct PresignGetOpts {
    pub expire_secs: u32,
}

#[derive(Clone, Debug, Default)]
pub struct PresignPutOpts {
    pub expire_secs: u32,
    pub content_type: Option<String>,
    pub size_hint: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MultipartInit {
    pub upload_id: String,
    pub ref_hint: BlobRef,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartETag {
    pub part_number: u32,
    pub etag: String,
    pub size: u64,
}

#[derive(Clone, Debug, Default)]
pub struct MultipartPutOpts {
    pub content_type: Option<String>,
    pub encrypt: bool,
    pub envelope_id: Option<String>,
}
