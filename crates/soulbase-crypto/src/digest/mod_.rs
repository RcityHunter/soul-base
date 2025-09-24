use serde_json::Value;
use sha2::Digest as ShaDigest;

use crate::base64url;
use crate::canonical::mod_::Canonicalizer;
use crate::errors::CryptoError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Digest {
    pub algo: String,
    pub size: usize,
    pub bytes: Vec<u8>,
    pub b64: String,
}

impl Digest {
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn as_base64url(&self) -> &str {
        &self.b64
    }
}

pub trait Digester: Send + Sync {
    fn digest(&self, algo: &str, data: &[u8]) -> Result<Digest, CryptoError>;

    fn sha256(&self, data: &[u8]) -> Result<Digest, CryptoError> {
        self.digest("sha256", data)
    }

    fn blake3(&self, data: &[u8]) -> Result<Digest, CryptoError> {
        self.digest("blake3", data)
    }

    fn commit_json(
        &self,
        canonicalizer: &impl Canonicalizer,
        value: &Value,
        algo: &str,
    ) -> Result<Digest, CryptoError> {
        let canonical = canonicalizer.canonical_json(value)?;
        self.digest(algo, &canonical)
    }
}

#[derive(Default, Debug, Clone, Copy)]
pub struct DefaultDigester;

impl DefaultDigester {
    fn digest_sha256(&self, data: &[u8]) -> Digest {
        let digest = sha2::Sha256::digest(data);
        build_digest("sha256", digest.as_slice())
    }

    fn digest_blake3(&self, data: &[u8]) -> Digest {
        let digest = blake3::hash(data);
        build_digest("blake3", digest.as_bytes())
    }
}

impl Digester for DefaultDigester {
    fn digest(&self, algo: &str, data: &[u8]) -> Result<Digest, CryptoError> {
        match algo {
            "sha256" => Ok(self.digest_sha256(data)),
            "blake3" => Ok(self.digest_blake3(data)),
            other => Err(CryptoError::unsupported(&format!(
                "unsupported digest algorithm: {other}"
            ))),
        }
    }
}

fn build_digest(algo: &str, bytes: &[u8]) -> Digest {
    let encoded = base64url::encode(bytes);
    Digest {
        algo: algo.to_string(),
        size: bytes.len(),
        bytes: bytes.to_vec(),
        b64: encoded,
    }
}
