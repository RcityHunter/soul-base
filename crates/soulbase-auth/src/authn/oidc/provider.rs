#![allow(clippy::result_large_err)]

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use base64::Engine;
use parking_lot::RwLock;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use soulbase_types::prelude::{Id, Subject, SubjectKind, TenantId};

use crate::attr::AttributeProvider;
use crate::authn::Authenticator;
use crate::errors;
use crate::errors::AuthError;
use crate::model::{AuthnInput, AuthzRequest};

const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(300);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JwkConfig {
    pub kid: String,
    #[serde(default)]
    pub alg: Option<String>,
    #[serde(default)]
    pub kty: String,
    #[serde(default)]
    pub n: Option<String>,
    #[serde(default)]
    pub e: Option<String>,
    #[serde(default)]
    pub k: Option<String>,
}

#[derive(Clone, Debug)]
pub enum JwkSource {
    Static(Vec<JwkConfig>),
    Http { uri: String, cache_ttl: Duration },
}

#[derive(Clone, Debug)]
pub struct OidcConfig {
    pub issuer: String,
    pub audience: Vec<String>,
    pub tenant_claim: String,
    pub subject_claim: Option<String>,
    pub role_claim: Option<String>,
    pub jwk_source: JwkSource,
    pub algorithms: Vec<jsonwebtoken::Algorithm>,
}

impl OidcConfig {
    pub fn builder(issuer: impl Into<String>) -> OidcConfigBuilder {
        OidcConfigBuilder {
            issuer: issuer.into(),
            audience: Vec::new(),
            tenant_claim: "tenant".into(),
            subject_claim: None,
            role_claim: None,
            jwk_source: JwkSource::Http {
                uri: String::new(),
                cache_ttl: DEFAULT_CACHE_TTL,
            },
            algorithms: vec![jsonwebtoken::Algorithm::RS256],
        }
    }
}

pub struct OidcConfigBuilder {
    issuer: String,
    audience: Vec<String>,
    tenant_claim: String,
    subject_claim: Option<String>,
    role_claim: Option<String>,
    jwk_source: JwkSource,
    algorithms: Vec<jsonwebtoken::Algorithm>,
}

impl OidcConfigBuilder {
    pub fn audience(mut self, audience: impl Into<String>) -> Self {
        self.audience.push(audience.into());
        self
    }

    pub fn audiences(mut self, audience: Vec<String>) -> Self {
        self.audience = audience;
        self
    }

    pub fn tenant_claim(mut self, claim: impl Into<String>) -> Self {
        self.tenant_claim = claim.into();
        self
    }

    pub fn subject_claim(mut self, claim: impl Into<String>) -> Self {
        self.subject_claim = Some(claim.into());
        self
    }

    pub fn role_claim(mut self, claim: impl Into<String>) -> Self {
        self.role_claim = Some(claim.into());
        self
    }

    pub fn static_keys(mut self, keys: Vec<JwkConfig>) -> Self {
        self.jwk_source = JwkSource::Static(keys);
        self
    }

    pub fn jwks_uri(mut self, uri: impl Into<String>) -> Self {
        self.jwk_source = JwkSource::Http {
            uri: uri.into(),
            cache_ttl: DEFAULT_CACHE_TTL,
        };
        self
    }

    pub fn jwks_uri_with_ttl(mut self, uri: impl Into<String>, ttl: Duration) -> Self {
        self.jwk_source = JwkSource::Http {
            uri: uri.into(),
            cache_ttl: ttl,
        };
        self
    }

    pub fn algorithms(mut self, algs: Vec<jsonwebtoken::Algorithm>) -> Self {
        self.algorithms = algs;
        self
    }

    pub fn build(self) -> OidcConfig {
        OidcConfig {
            issuer: self.issuer,
            audience: self.audience,
            tenant_claim: self.tenant_claim,
            subject_claim: self.subject_claim,
            role_claim: self.role_claim,
            jwk_source: self.jwk_source,
            algorithms: self.algorithms,
        }
    }
}

#[derive(Clone)]
struct CachedKeys {
    keys: HashMap<String, JwkConfig>,
    expires_at: Option<Instant>,
}

pub struct OidcAuthenticator {
    config: Arc<OidcConfig>,
    client: Option<reqwest::Client>,
    cache: Arc<RwLock<Option<CachedKeys>>>,
}

impl OidcAuthenticator {
    pub fn new(config: OidcConfig) -> Result<Self, AuthError> {
        let client = match &config.jwk_source {
            JwkSource::Http { .. } => {
                let builder = reqwest::Client::builder().use_rustls_tls();
                Some(builder.build().map_err(|err| {
                    errors::provider_unavailable(&format!("failed to build http client: {err}"))
                })?)
            }
            JwkSource::Static(_) => None,
        };
        Ok(Self {
            config: Arc::new(config),
            client,
            cache: Arc::new(RwLock::new(None)),
        })
    }

    async fn ensure_keys(&self) -> Result<(), AuthError> {
        let needs_refresh = {
            let guard = self.cache.read();
            match guard.as_ref() {
                Some(cache) => match cache.expires_at {
                    Some(expiry) => expiry <= Instant::now(),
                    None => false,
                },
                None => true,
            }
        };
        if !needs_refresh {
            return Ok(());
        }

        let cache = match &self.config.jwk_source {
            JwkSource::Static(keys) => CachedKeys {
                keys: keys.iter().map(|k| (k.kid.clone(), k.clone())).collect(),
                expires_at: None,
            },
            JwkSource::Http { uri, cache_ttl } => {
                let client = self
                    .client
                    .as_ref()
                    .ok_or_else(|| errors::provider_unavailable("http client not initialised"))?;
                let response = client.get(uri).send().await.map_err(|err| {
                    errors::provider_unavailable(&format!("jwks fetch error: {err}"))
                })?;
                if response.status() != StatusCode::OK {
                    return Err(errors::provider_unavailable(&format!(
                        "jwks fetch status: {}",
                        response.status()
                    )));
                }
                let body: JwkSet = response.json().await.map_err(|err| {
                    errors::provider_unavailable(&format!("jwks decode error: {err}"))
                })?;
                CachedKeys {
                    keys: body.keys.into_iter().map(|k| (k.kid.clone(), k)).collect(),
                    expires_at: Some(Instant::now() + *cache_ttl),
                }
            }
        };

        let mut guard = self.cache.write();
        *guard = Some(cache);
        Ok(())
    }

    fn select_algorithm(&self, alg: Option<&str>) -> Result<jsonwebtoken::Algorithm, AuthError> {
        if let Some(alg) = alg {
            let parsed = jsonwebtoken::Algorithm::from_str(alg).map_err(|_| {
                errors::provider_unavailable(&format!("unsupported jwk algorithm: {alg}"))
            })?;
            if self.config.algorithms.contains(&parsed) {
                return Ok(parsed);
            }
            return Err(errors::provider_unavailable(&format!(
                "algorithm {alg} not allowed"
            )));
        }
        self.config
            .algorithms
            .first()
            .cloned()
            .ok_or_else(|| errors::provider_unavailable("no algorithms configured"))
    }

    fn decoding_key(&self, jwk: &JwkConfig) -> Result<jsonwebtoken::DecodingKey, AuthError> {
        match jwk.kty.as_str() {
            "RSA" => {
                let n = jwk
                    .n
                    .as_ref()
                    .ok_or_else(|| errors::provider_unavailable("jwks rsa modulus missing"))?;
                let e = jwk
                    .e
                    .as_ref()
                    .ok_or_else(|| errors::provider_unavailable("jwks rsa exponent missing"))?;
                jsonwebtoken::DecodingKey::from_rsa_components(n, e).map_err(|err| {
                    errors::provider_unavailable(&format!("failed to build rsa key: {err}"))
                })
            }
            "oct" => {
                let secret = jwk
                    .k
                    .as_ref()
                    .ok_or_else(|| errors::provider_unavailable("jwks secret missing"))?;
                let bytes = base64::engine::general_purpose::URL_SAFE
                    .decode(secret)
                    .map_err(|err| {
                        errors::provider_unavailable(&format!("secret decode error: {err}"))
                    })?;
                Ok(jsonwebtoken::DecodingKey::from_secret(&bytes))
            }
            other => Err(errors::provider_unavailable(&format!(
                "unsupported jwk key type: {other}"
            ))),
        }
    }

    async fn get_key(
        &self,
        kid: Option<&str>,
    ) -> Result<
        (
            JwkConfig,
            jsonwebtoken::DecodingKey,
            jsonwebtoken::Algorithm,
        ),
        AuthError,
    > {
        self.ensure_keys().await?;
        let guard = self.cache.read();
        let cache = guard
            .as_ref()
            .ok_or_else(|| errors::provider_unavailable("jwks cache missing"))?;
        let kid = kid.ok_or_else(|| errors::unauthenticated("token missing kid header"))?;
        let jwk = cache
            .keys
            .get(kid)
            .ok_or_else(|| errors::unauthenticated("matching jwk not found"))?
            .clone();
        drop(guard);
        let alg = self.select_algorithm(jwk.alg.as_deref())?;
        let key = self.decoding_key(&jwk)?;
        Ok((jwk, key, alg))
    }

    fn extract_claim(
        &self,
        map: &mut Map<String, Value>,
        field: &str,
    ) -> Result<String, AuthError> {
        match map.remove(field) {
            Some(Value::String(s)) => Ok(s),
            Some(Value::Number(num)) => Ok(num.to_string()),
            Some(Value::Null) | None => {
                Err(errors::unauthenticated(&format!("claim {field} missing")))
            }
            _ => Err(errors::unauthenticated(&format!(
                "claim {field} must be string"
            ))),
        }
    }

    fn build_validation(&self, alg: jsonwebtoken::Algorithm) -> jsonwebtoken::Validation {
        let mut validation = jsonwebtoken::Validation::new(alg);
        validation.set_required_spec_claims(&["exp", "iat"]);
        validation.validate_exp = true;
        validation.validate_nbf = true;
        validation.set_issuer(std::slice::from_ref(&self.config.issuer));
        if !self.config.audience.is_empty() {
            validation.set_audience(&self.config.audience);
        } else {
            validation.validate_aud = false;
        }
        validation
    }
}

#[async_trait]
impl Authenticator for OidcAuthenticator {
    async fn authenticate(&self, input: AuthnInput) -> Result<Subject, AuthError> {
        let token = match input {
            AuthnInput::BearerJwt(token) => token,
            _ => return Err(errors::unauthenticated("unsupported authn input")),
        };

        let header = jsonwebtoken::decode_header(&token)
            .map_err(|err| errors::unauthenticated(&format!("invalid token header: {err}")))?;
        let (jwk, key, alg) = self.get_key(header.kid.as_deref()).await?;
        let validation = self.build_validation(alg);

        let data = jsonwebtoken::decode::<serde_json::Value>(&token, &key, &validation)
            .map_err(|err| errors::unauthenticated(&format!("jwt verification failed: {err}")))?;

        let mut claims = match data.claims {
            Value::Object(map) => map,
            _ => return Err(errors::unauthenticated("jwt claims must be object")),
        };

        let subject_field = self.config.subject_claim.as_deref().unwrap_or("sub");
        let subject_id = self.extract_claim(&mut claims, subject_field)?;
        let tenant_id = self.extract_claim(&mut claims, &self.config.tenant_claim)?;

        claims.insert("iss".into(), Value::String(self.config.issuer.clone()));
        claims.insert("kid".into(), Value::String(jwk.kid));

        Ok(Subject {
            kind: SubjectKind::User,
            subject_id: Id(subject_id),
            tenant: TenantId(tenant_id),
            claims,
        })
    }
}

#[derive(Clone, Debug)]
pub struct ClaimsAttributeConfig {
    pub roles_claim: Option<String>,
}

impl Default for ClaimsAttributeConfig {
    fn default() -> Self {
        Self {
            roles_claim: Some("roles".into()),
        }
    }
}

pub struct ClaimsAttributeProvider {
    config: ClaimsAttributeConfig,
}

impl ClaimsAttributeProvider {
    pub fn new(config: ClaimsAttributeConfig) -> Self {
        Self { config }
    }

    fn extract_roles(&self, claims: &Map<String, Value>) -> Option<Vec<String>> {
        let claim = self.config.roles_claim.as_ref()?;
        let value = claims.get(claim)?;
        match value {
            Value::String(s) => Some(vec![s.clone()]),
            Value::Array(items) => Some(
                items
                    .iter()
                    .filter_map(|item| match item {
                        Value::String(s) => Some(s.clone()),
                        Value::Object(map) => map
                            .get("id")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string()),
                        _ => None,
                    })
                    .collect(),
            ),
            _ => None,
        }
    }
}

#[async_trait]
impl AttributeProvider for ClaimsAttributeProvider {
    async fn augment(&self, req: &AuthzRequest) -> Result<Value, AuthError> {
        if let Some(roles) = self.extract_roles(&req.subject.claims) {
            if !roles.is_empty() {
                return Ok(json!({ "roles": roles }));
            }
        }
        Ok(Value::Object(Map::new()))
    }
}

#[derive(Deserialize)]
struct JwkSet {
    keys: Vec<JwkConfig>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Action, AuthzRequest, ResourceUrn};
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
    use serde_json::json;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unix_now() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    fn hs256_key() -> (&'static str, JwkConfig) {
        let secret = "super-secret";
        let encoded = base64::engine::general_purpose::URL_SAFE.encode(secret);
        (
            secret,
            JwkConfig {
                kid: "hs-test".into(),
                alg: Some("HS256".into()),
                kty: "oct".into(),
                n: None,
                e: None,
                k: Some(encoded),
            },
        )
    }

    #[tokio::test]
    async fn authenticate_static_hs256_token() {
        let (secret, jwk) = hs256_key();
        let config = OidcConfig::builder("https://issuer.example")
            .audience("api://example")
            .tenant_claim("tenant")
            .role_claim("roles")
            .static_keys(vec![jwk])
            .algorithms(vec![Algorithm::HS256])
            .build();

        let authenticator = OidcAuthenticator::new(config).expect("build authenticator");

        let header = Header {
            alg: Algorithm::HS256,
            kid: Some("hs-test".into()),
            ..Header::default()
        };
        let now = unix_now();
        let claims = json!({
            "sub": "user-123",
            "tenant": "tenant-xyz",
            "iss": "https://issuer.example",
            "aud": "api://example",
            "exp": now + 600,
            "iat": now,
            "roles": ["admin", "dev"],
        });
        let token = encode(
            &header,
            &claims,
            &EncodingKey::from_secret(secret.as_bytes()),
        )
        .expect("encode jwt");

        let subject = authenticator
            .authenticate(AuthnInput::BearerJwt(token))
            .await
            .expect("authenticate");
        assert_eq!(subject.subject_id, Id("user-123".into()));
        assert_eq!(subject.tenant, TenantId("tenant-xyz".into()));
        assert!(subject.claims.contains_key("roles"));
    }

    #[tokio::test]
    async fn attribute_provider_extracts_roles() {
        let subject = Subject {
            kind: SubjectKind::User,
            subject_id: Id("user".into()),
            tenant: TenantId("tenant".into()),
            claims: vec![(
                "roles".into(),
                Value::Array(vec![
                    Value::String("admin".into()),
                    Value::String("ops".into()),
                ]),
            )]
            .into_iter()
            .collect(),
        };
        let provider = ClaimsAttributeProvider::new(ClaimsAttributeConfig {
            roles_claim: Some("roles".into()),
        });
        let augmented = provider
            .augment(&AuthzRequest {
                subject,
                resource: ResourceUrn("urn:test".into()),
                action: Action::Read,
                attrs: Value::Null,
                consent: None,
                correlation_id: None,
            })
            .await
            .expect("augment");
        assert_eq!(augmented["roles"].as_array().unwrap().len(), 2);
    }
}
