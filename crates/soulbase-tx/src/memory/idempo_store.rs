use crate::errors::TxError;
use crate::idempo::{composite_key, is_expired, IdempoStore};
use crate::model::{IdempoKey, IdempoState};
use async_trait::async_trait;
use parking_lot::RwLock;
use soulbase_types::prelude::TenantId;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Default, Clone)]
pub struct InMemoryIdempoStore {
    inner: Arc<RwLock<HashMap<IdempoKey, IdempoState>>>,
}

impl InMemoryIdempoStore {
    fn hash_mismatch(existing: &str, incoming: &str) -> bool {
        !existing.is_empty() && existing != incoming
    }
}

#[async_trait]
impl IdempoStore for InMemoryIdempoStore {
    async fn check_and_put(
        &self,
        tenant: &TenantId,
        key: &str,
        hash: &str,
        ttl_ms: i64,
    ) -> Result<Option<String>, TxError> {
        let mut guard = self.inner.write();
        let now = crate::util::now_ms();
        let id = composite_key(tenant, key);
        if let Some(existing) = guard.get_mut(&id) {
            match existing {
                IdempoState::Succeeded { hash: h, digest } => {
                    if Self::hash_mismatch(h, hash) {
                        return Err(TxError::conflict("idempotency hash mismatch"));
                    }
                    return Ok(Some(digest.clone()));
                }
                IdempoState::InFlight {
                    hash: h,
                    expires_at,
                } => {
                    if Self::hash_mismatch(h, hash) {
                        return Err(TxError::conflict("idempotency hash mismatch"));
                    }
                    if *expires_at > now {
                        return Ok(None);
                    }
                }
                IdempoState::Failed {
                    hash: h,
                    expires_at,
                    ..
                } => {
                    if Self::hash_mismatch(h, hash) {
                        return Err(TxError::conflict("idempotency hash mismatch"));
                    }
                    if *expires_at > now {
                        return Err(TxError::bad_request(
                            "idempotency record is temporarily locked after failure",
                        ));
                    }
                }
            }
        }

        let expires_at = now + ttl_ms;
        guard.insert(
            id,
            IdempoState::InFlight {
                hash: hash.to_string(),
                expires_at,
            },
        );
        Ok(None)
    }

    async fn finish(
        &self,
        tenant: &TenantId,
        key: &str,
        hash: &str,
        digest: &str,
    ) -> Result<(), TxError> {
        let mut guard = self.inner.write();
        let id = composite_key(tenant, key);
        if let Some(existing) = guard.get_mut(&id) {
            match existing {
                IdempoState::InFlight { hash: h, .. }
                | IdempoState::Failed { hash: h, .. }
                | IdempoState::Succeeded { hash: h, .. } => {
                    if Self::hash_mismatch(h, hash) {
                        return Err(TxError::conflict("idempotency hash mismatch"));
                    }
                    *existing = IdempoState::Succeeded {
                        hash: hash.to_string(),
                        digest: digest.to_string(),
                    };
                    return Ok(());
                }
            }
        }
        Err(TxError::not_found("idempotency entry not found"))
    }

    async fn fail(
        &self,
        tenant: &TenantId,
        key: &str,
        hash: &str,
        error: &str,
        ttl_ms: i64,
    ) -> Result<(), TxError> {
        let mut guard = self.inner.write();
        let id = composite_key(tenant, key);
        if let Some(existing) = guard.get_mut(&id) {
            match existing {
                IdempoState::InFlight { hash: h, .. }
                | IdempoState::Failed { hash: h, .. }
                | IdempoState::Succeeded { hash: h, .. } => {
                    if Self::hash_mismatch(h, hash) {
                        return Err(TxError::conflict("idempotency hash mismatch"));
                    }
                    *existing = IdempoState::Failed {
                        hash: hash.to_string(),
                        error: error.to_string(),
                        expires_at: crate::util::now_ms() + ttl_ms,
                    };
                    return Ok(());
                }
            }
        }
        Err(TxError::not_found("idempotency entry not found"))
    }

    async fn purge_expired(&self, now_ms: i64) -> Result<(), TxError> {
        let mut guard = self.inner.write();
        guard.retain(|_, state| !is_expired(state, now_ms));
        Ok(())
    }
}
