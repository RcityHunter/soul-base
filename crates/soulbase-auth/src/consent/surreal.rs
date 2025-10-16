use crate::consent::store::ConsentStore;
use crate::errors::AuthError;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::json;
use soulbase_storage::model::Entity;
use soulbase_storage::surreal::{SurrealDatastore, SurrealRepository};
use soulbase_storage::Repository;
use soulbase_types::prelude::{Consent, TenantId};

#[derive(Clone)]
pub struct SurrealConsentStore {
    repo: SurrealRepository<ConsentRecord>,
}

impl SurrealConsentStore {
    pub fn new(datastore: &SurrealDatastore) -> Self {
        Self {
            repo: SurrealRepository::new(datastore),
        }
    }

    fn record_id(tenant: &TenantId, subject: &str) -> String {
        format!("{}:{}:{}", ConsentRecord::TABLE, tenant.0, subject)
    }
}

#[async_trait]
impl ConsentStore for SurrealConsentStore {
    async fn record(
        &self,
        tenant: &TenantId,
        subject: &str,
        consent: &Consent,
    ) -> Result<(), AuthError> {
        self.repo
            .upsert(
                tenant,
                &Self::record_id(tenant, subject),
                json!({
                    "subject": subject,
                    "consent": consent,
                }),
                None,
            )
            .await
            .map(|_| ())
            .map_err(|err| AuthError(err.into_inner()))
    }

    async fn load(&self, tenant: &TenantId, subject: &str) -> Result<Option<Consent>, AuthError> {
        let record = self
            .repo
            .get(tenant, &Self::record_id(tenant, subject))
            .await
            .map_err(|err| AuthError(err.into_inner()))?;
        Ok(record.map(|row| row.consent))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ConsentRecord {
    id: String,
    tenant: TenantId,
    subject: String,
    consent: Consent,
}

impl Entity for ConsentRecord {
    const TABLE: &'static str = "auth_consent";

    fn id(&self) -> &str {
        &self.id
    }

    fn tenant(&self) -> &TenantId {
        &self.tenant
    }
}
