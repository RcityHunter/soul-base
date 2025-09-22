use async_trait::async_trait;
use serde::Serialize;

use crate::model::EvidenceEnvelope;
use crate::ObserveError;

#[async_trait]
pub trait EvidenceSink: Send + Sync {
    async fn emit<T: Serialize + Send + Sync>(
        &self,
        envelope: EvidenceEnvelope<T>,
    ) -> Result<(), ObserveError>;
}

#[derive(Default)]
pub struct NoopEvidenceSink;

#[async_trait]
impl EvidenceSink for NoopEvidenceSink {
    async fn emit<T: Serialize + Send + Sync>(
        &self,
        _envelope: EvidenceEnvelope<T>,
    ) -> Result<(), ObserveError> {
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct EvidenceEvent<T: Serialize> {
    pub category: String,
    pub payload: T,
}
