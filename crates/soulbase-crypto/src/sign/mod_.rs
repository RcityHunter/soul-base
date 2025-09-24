use core::fmt;

use ed25519_dalek::{Signature, Signer as _, Verifier as _};
use serde::{Deserialize, Serialize};

use crate::base64url;
use crate::errors::CryptoError;

use super::keystore::{KeyStore, VerifyKeyMaterial};

pub trait Signer: Send + Sync {
    fn sign_detached(&self, payload: &[u8]) -> Result<String, CryptoError>;
}

pub trait Verifier: Send + Sync {
    fn verify_detached(
        &self,
        expected_audience: &str,
        payload: &[u8],
        jws: &str,
    ) -> Result<(), CryptoError>;
}

#[derive(Clone)]
pub struct JwsEd25519Signer<K: KeyStore> {
    pub keystore: K,
}

#[derive(Clone)]
pub struct JwsEd25519Verifier<K: KeyStore> {
    pub keystore: K,
}

impl<K: KeyStore> Signer for JwsEd25519Signer<K> {
    fn sign_detached(&self, payload: &[u8]) -> Result<String, CryptoError> {
        let material = self.keystore.current_signing_key()?;
        material.policy.is_active(now_ms())?;
        let header = ProtectedHeader::new(&material.kid);
        let header_segment = encode_header(&header)?;
        let signing_input = build_signing_input(&header_segment, payload);
        let signature = material.signing_key.sign(&signing_input);
        let signature_bytes = signature.to_bytes();
        let signature_segment = base64url::encode(&signature_bytes);
        Ok(format!("{header_segment}..{signature_segment}"))
    }
}

impl<K: KeyStore> Verifier for JwsEd25519Verifier<K> {
    fn verify_detached(
        &self,
        _expected_audience: &str,
        payload: &[u8],
        jws: &str,
    ) -> Result<(), CryptoError> {
        let parsed = ParsedJws::parse(jws)?;
        let header: ProtectedHeader = decode_header(parsed.header_b64)?;
        if header.alg != "EdDSA" {
            return Err(CryptoError::signature_invalid(&format!(
                "unsupported jws alg {}",
                header.alg
            )));
        }
        if header.b64 {
            return Err(CryptoError::signature_invalid(
                "expected detached payload with b64=false",
            ));
        }
        if !header.crit.iter().any(|c| c == "b64") {
            return Err(CryptoError::signature_invalid("missing crit entry for b64"));
        }

        let key: VerifyKeyMaterial = self.keystore.key_for_verification(&header.kid)?;
        key.policy.is_active(now_ms())?;

        let signing_input = build_signing_input(parsed.header_b64, payload);
        let signature = parse_signature(parsed.signature_b64)?;
        key.verifying_key
            .verify(&signing_input, &signature)
            .map_err(|err| {
                CryptoError::signature_invalid(&format!("signature verification failed: {err}"))
            })
    }
}

#[derive(Serialize, Deserialize)]
struct ProtectedHeader {
    alg: String,
    kid: String,
    #[serde(default)]
    b64: bool,
    #[serde(default)]
    crit: Vec<String>,
}

impl ProtectedHeader {
    fn new(kid: &str) -> Self {
        Self {
            alg: "EdDSA".to_string(),
            kid: kid.to_string(),
            b64: false,
            crit: vec!["b64".to_string()],
        }
    }
}

struct ParsedJws<'a> {
    header_b64: &'a str,
    signature_b64: &'a str,
}

impl<'a> ParsedJws<'a> {
    fn parse(token: &'a str) -> Result<Self, CryptoError> {
        let segments: Vec<&str> = token.split('.').collect();
        if segments.len() != 3 || !segments[1].is_empty() {
            return Err(CryptoError::signature_invalid(
                "expected detached JWS with three segments and empty payload",
            ));
        }
        Ok(Self {
            header_b64: segments[0],
            signature_b64: segments[2],
        })
    }
}

fn encode_header(header: &ProtectedHeader) -> Result<String, CryptoError> {
    let encoded = serde_json::to_vec(header).map_err(|err| {
        CryptoError::canonical(&format!("failed to encode protected header: {err}"))
    })?;
    Ok(base64url::encode(&encoded))
}

fn decode_header(encoded: &str) -> Result<ProtectedHeader, CryptoError> {
    let bytes = base64url::decode(encoded)?;
    serde_json::from_slice(&bytes)
        .map_err(|err| CryptoError::canonical(&format!("failed to decode protected header: {err}")))
}

fn build_signing_input(header_segment: &str, payload: &[u8]) -> Vec<u8> {
    let mut signing_input = Vec::with_capacity(header_segment.len() + 1 + payload.len());
    signing_input.extend_from_slice(header_segment.as_bytes());
    signing_input.push(b'.');
    signing_input.extend_from_slice(payload);
    signing_input
}

fn parse_signature(encoded: &str) -> Result<Signature, CryptoError> {
    let bytes = base64url::decode(encoded)?;
    let arr: [u8; 64] = bytes
        .try_into()
        .map_err(|_| CryptoError::signature_invalid("invalid signature length"))?;
    Ok(Signature::from_bytes(&arr))
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_millis() as i64
}

impl fmt::Debug for ProtectedHeader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProtectedHeader")
            .field("alg", &self.alg)
            .field("kid", &self.kid)
            .field("b64", &self.b64)
            .field("crit", &self.crit)
            .finish()
    }
}
