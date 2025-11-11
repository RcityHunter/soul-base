use crate::model::{QuotaKey, QuotaOutcome};
use crate::prelude::AuthError;
use async_trait::async_trait;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

type UsageKey = (String, String, String, String);
type UsageMap = HashMap<UsageKey, u64>;

#[async_trait]
pub trait QuotaStore: Send + Sync {
    async fn check_and_consume(&self, key: &QuotaKey, cost: u64)
    -> Result<QuotaOutcome, AuthError>;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QuotaConfig {
    #[serde(default = "QuotaConfig::default_limit")]
    pub default_limit: u64,
}

impl Default for QuotaConfig {
    fn default() -> Self {
        Self { default_limit: 100 }
    }
}

impl QuotaConfig {
    fn default_limit() -> u64 {
        100
    }
}

#[derive(Clone, Default)]
pub struct MemoryQuotaStore {
    config: QuotaConfig,
    usage: Arc<RwLock<UsageMap>>,
}

impl MemoryQuotaStore {
    pub fn new(config: QuotaConfig) -> Self {
        Self {
            config,
            usage: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn with_default_limit(limit: u64) -> Self {
        Self::new(QuotaConfig {
            default_limit: limit,
        })
    }
}

#[async_trait]
impl QuotaStore for MemoryQuotaStore {
    async fn check_and_consume(
        &self,
        key: &QuotaKey,
        cost: u64,
    ) -> Result<QuotaOutcome, AuthError> {
        let mut guard = self.usage.write();
        let bucket = (
            key.tenant.0.clone(),
            key.subject_id.0.clone(),
            key.resource.0.clone(),
            format!("{:?}", key.action),
        );
        let entry = guard.entry(bucket).or_insert(0);
        if *entry + cost > self.config.default_limit {
            return Ok(QuotaOutcome::BudgetExceeded);
        }
        *entry += cost;
        Ok(QuotaOutcome::Allowed)
    }
}
