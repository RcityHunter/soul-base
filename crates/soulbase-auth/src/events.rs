use serde::{Deserialize, Serialize};
use soulbase_types::prelude::*;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuthDecisionEvent {
    pub subject_id: Id,
    pub tenant: TenantId,
    pub resource: String,
    pub action: String,
    pub allow: bool,
    #[serde(default)]
    pub policy_hash: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QuotaEvent {
    pub tenant: TenantId,
    pub subject_id: Id,
    pub resource: String,
    pub action: String,
    pub cost: u64,
    pub outcome: String,
}
