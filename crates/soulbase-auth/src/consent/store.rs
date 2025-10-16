use crate::model::AuthzRequest;
use crate::prelude::AuthError;
use async_trait::async_trait;
use soulbase_types::prelude::{Consent, TenantId};
use std::collections::HashMap;
use std::sync::Arc;

#[async_trait]
pub trait ConsentStore: Send + Sync {
    async fn record(
        &self,
        tenant: &TenantId,
        subject: &str,
        consent: &Consent,
    ) -> Result<(), AuthError>;
    async fn load(&self, tenant: &TenantId, subject: &str) -> Result<Option<Consent>, AuthError>;
}

#[derive(Clone, Default)]
pub struct MemoryConsentStore {
    inner: Arc<parking_lot::RwLock<HashMap<(String, String), Consent>>>,
}

#[async_trait]
impl ConsentStore for MemoryConsentStore {
    async fn record(
        &self,
        tenant: &TenantId,
        subject: &str,
        consent: &Consent,
    ) -> Result<(), AuthError> {
        let mut guard = self.inner.write();
        guard.insert((tenant.0.clone(), subject.to_string()), consent.clone());
        Ok(())
    }

    async fn load(&self, tenant: &TenantId, subject: &str) -> Result<Option<Consent>, AuthError> {
        let guard = self.inner.read();
        Ok(guard.get(&(tenant.0.clone(), subject.to_string())).cloned())
    }
}

pub struct ConsentVerifierWithStore<S: ConsentStore> {
    store: S,
}

impl<S: ConsentStore> ConsentVerifierWithStore<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }
}

#[async_trait]
impl<S: ConsentStore> crate::consent::ConsentVerifier for ConsentVerifierWithStore<S> {
    async fn verify(&self, consent: &Consent, request: &AuthzRequest) -> Result<bool, AuthError> {
        self.store
            .record(
                &request.subject.tenant,
                &request.subject.subject_id.0,
                consent,
            )
            .await?;
        Ok(true)
    }
}
