use crate::errors::AuthError;
use crate::model::{QuotaKey, QuotaOutcome};
use async_trait::async_trait;

pub mod memory;

#[async_trait]
pub trait QuotaStore: Send + Sync {
    async fn check_and_consume(&self, key: &QuotaKey, cost: u64)
        -> Result<QuotaOutcome, AuthError>;
}
