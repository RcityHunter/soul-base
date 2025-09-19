use crate::errors::TxError;
use crate::idempo::{composite_key, IdempoStore};
use crate::model::{IdempoKey, IdempoState};
use async_trait::async_trait;
use parking_lot::RwLock;
use soulbase_types::prelude::TenantId;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone, Debug)]
struct StoredRecord {
    state: IdempoState,
}

#[derive(Default, Clone)]
pub struct InMemoryIdempoStore {
    inner: Arc<RwLock<HashMap<IdempoKey, StoredRecord>>>,
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
        let _ = hash;
        let mut guard = self.inner.write();
        let now = crate::util::now_ms();
        let id = composite_key(tenant, key);

        if let Some(existing) = guard.get(&id) {
            match &existing.state {
                IdempoState::Completed { digest } => return Ok(Some(digest.clone())),
                IdempoState::InFlight { expires_at } => {
                    if *expires_at > now {
                        return Ok(None);
                    }
                }
            }
        }

        let expires_at = now + ttl_ms;
        guard.insert(
            id,
            StoredRecord {
                state: IdempoState::InFlight { expires_at },
            },
        );
        Ok(None)
    }

    async fn finish(&self, tenant: &TenantId, key: &str, digest: &str) -> Result<(), TxError> {
        let mut guard = self.inner.write();
        let id = composite_key(tenant, key);
        if let Some(existing) = guard.get_mut(&id) {
            existing.state = IdempoState::Completed {
                digest: digest.to_string(),
            };
            Ok(())
        } else {
            Err(TxError::not_found("idempotency entry not found"))
        }
    }

    async fn purge_expired(&self, now_ms: i64) -> Result<(), TxError> {
        let mut guard = self.inner.write();
        guard.retain(|_, record| match record.state {
            IdempoState::InFlight { expires_at } => expires_at > now_ms,
            IdempoState::Completed { .. } => true,
        });
        Ok(())
    }
}
