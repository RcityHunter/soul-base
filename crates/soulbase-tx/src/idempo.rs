use crate::errors::TxError;
use crate::model::{IdempoKey, IdempoState};
use async_trait::async_trait;
use soulbase_types::prelude::TenantId;

#[async_trait]
pub trait IdempoStore: Send + Sync {
    async fn check_and_put(
        &self,
        tenant: &TenantId,
        key: &str,
        hash: &str,
        ttl_ms: i64,
    ) -> Result<Option<String>, TxError>;

    async fn finish(
        &self,
        tenant: &TenantId,
        key: &str,
        hash: &str,
        digest: &str,
    ) -> Result<(), TxError>;

    async fn fail(
        &self,
        tenant: &TenantId,
        key: &str,
        hash: &str,
        error: &str,
        ttl_ms: i64,
    ) -> Result<(), TxError>;

    async fn purge_expired(&self, now_ms: i64) -> Result<(), TxError>;
}

pub fn composite_key(tenant: &TenantId, key: &str) -> IdempoKey {
    IdempoKey {
        tenant: tenant.clone(),
        key: key.to_string(),
    }
}

pub fn is_expired(state: &IdempoState, now_ms: i64) -> bool {
    match state {
        IdempoState::InFlight { expires_at, .. } => *expires_at <= now_ms,
        IdempoState::Succeeded { .. } => false,
        IdempoState::Failed { expires_at, .. } => *expires_at <= now_ms,
    }
}
