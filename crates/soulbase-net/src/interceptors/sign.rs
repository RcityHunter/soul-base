use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD_NO_PAD;
use hmac::{Hmac, Mac};
use http::header::{HeaderName, HeaderValue};
use sha2::{Digest, Sha256};

use crate::errors::NetError;
use crate::interceptors::Interceptor;
use crate::types::NetRequest;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, Debug)]
pub struct SignatureConfig {
    pub key_id: String,
    pub secret: String,
    pub signature_header: HeaderName,
    pub key_header: HeaderName,
    pub timestamp_header: HeaderName,
}

impl Default for SignatureConfig {
    fn default() -> Self {
        Self {
            key_id: "default".into(),
            secret: String::new(),
            signature_header: HeaderName::from_static("x-soul-signature"),
            key_header: HeaderName::from_static("x-soul-key"),
            timestamp_header: HeaderName::from_static("x-soul-ts"),
        }
    }
}

#[derive(Clone)]
pub struct RequestSigner {
    config: SignatureConfig,
}

impl RequestSigner {
    pub fn new(config: SignatureConfig) -> Self {
        Self { config }
    }

    fn canonical(&self, request: &NetRequest, timestamp: &str) -> Result<String, NetError> {
        let mut canonical = String::new();
        canonical.push_str(request.method.as_str());
        canonical.push('\n');
        canonical.push_str(request.url.path());
        canonical.push('\n');
        if let Some(query) = request.url.query() {
            canonical.push_str(query);
        }
        canonical.push('\n');
        canonical.push_str(timestamp);
        canonical.push('\n');

        let body_bytes = request
            .body
            .as_bytes()
            .ok_or_else(|| NetError::schema("unable to canonicalise body"))?;
        let mut hasher = Sha256::new();
        hasher.update(&body_bytes);
        let body_hash = hasher.finalize();
        canonical.push_str(&STANDARD_NO_PAD.encode(body_hash));

        Ok(canonical)
    }
}

#[async_trait]
impl Interceptor for RequestSigner {
    async fn before_send(&self, request: &mut NetRequest) -> Result<(), NetError> {
        if self.config.secret.is_empty() {
            return Ok(());
        }

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .to_string();

        let canonical = self.canonical(request, &timestamp)?;
        let mut mac = HmacSha256::new_from_slice(self.config.secret.as_bytes())
            .map_err(|err| NetError::schema(&format!("invalid signing key: {err}")))?;
        mac.update(canonical.as_bytes());
        let signature = mac.finalize().into_bytes();
        let signature_encoded = STANDARD_NO_PAD.encode(signature);

        request.headers.insert(
            self.config.key_header.clone(),
            HeaderValue::from_str(&self.config.key_id)
                .map_err(|err| NetError::schema(&format!("invalid key header: {err}")))?,
        );
        request.headers.insert(
            self.config.timestamp_header.clone(),
            HeaderValue::from_str(&timestamp)
                .map_err(|err| NetError::schema(&format!("invalid timestamp header: {err}")))?,
        );
        request.headers.insert(
            self.config.signature_header.clone(),
            HeaderValue::from_str(&signature_encoded)
                .map_err(|err| NetError::schema(&format!("invalid signature header: {err}")))?,
        );

        Ok(())
    }
}
