use serde::{Deserialize, Serialize};
use soulbase_types::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct ResourceUrn(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum Action {
    Read,
    Write,
    Invoke,
    List,
    Admin,
    Configure,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuthzRequest {
    pub subject: Subject,
    pub resource: ResourceUrn,
    pub action: Action,
    #[serde(default)]
    pub attrs: serde_json::Value,
    #[serde(default)]
    pub consent: Option<Consent>,
    #[serde(default)]
    pub correlation_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Obligation {
    pub kind: String,
    #[serde(default)]
    pub params: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Decision {
    pub allow: bool,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub obligations: Vec<Obligation>,
    #[serde(default)]
    pub evidence: serde_json::Value,
    #[serde(default)]
    pub cache_ttl_ms: u32,
}

#[derive(Clone, Debug)]
pub enum AuthnInput {
    BearerJwt(String),
    ApiKey(String),
    MTls { peer_dn: String, san: Vec<String> },
    ServiceToken(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct QuotaKey {
    pub tenant: TenantId,
    pub subject_id: Id,
    pub resource: ResourceUrn,
    pub action: Action,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum QuotaOutcome {
    Allowed,
    RateLimited,
    BudgetExceeded,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct DecisionKey {
    pub tenant: TenantId,
    pub subject_id: Id,
    pub resource: ResourceUrn,
    pub action: Action,
    pub attrs_fingerprint: u64,
}

pub fn decision_key(req: &AuthzRequest, merged_attrs: &serde_json::Value) -> DecisionKey {
    use ahash::AHasher;
    use std::hash::Hasher;

    let mut hasher = AHasher::default();
    let fingerprint_src = serde_json::to_string(merged_attrs).unwrap_or_default();
    hasher.write(fingerprint_src.as_bytes());

    DecisionKey {
        tenant: req.subject.tenant.clone(),
        subject_id: req.subject.subject_id.clone(),
        resource: req.resource.clone(),
        action: req.action.clone(),
        attrs_fingerprint: hasher.finish(),
    }
}

pub fn cost_from_attrs(attrs: &serde_json::Value) -> u64 {
    attrs.get("cost").and_then(|v| v.as_u64()).unwrap_or(1)
}
