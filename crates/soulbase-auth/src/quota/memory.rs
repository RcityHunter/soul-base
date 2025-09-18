use super::*;

pub struct MemoryQuota;

#[async_trait::async_trait]
impl super::QuotaStore for MemoryQuota {
    async fn check_and_consume(
        &self,
        _key: &QuotaKey,
        _cost: u64,
    ) -> Result<QuotaOutcome, AuthError> {
        Ok(QuotaOutcome::Allowed)
    }
}
