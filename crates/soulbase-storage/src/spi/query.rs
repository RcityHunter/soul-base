use crate::errors::StorageError;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct QueryStats {
    pub indices_used: Option<Vec<String>>,
    pub query_hash: Option<String>,
    pub degradation_reason: Option<String>,
    pub full_scan: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryOutcome {
    pub rows: Vec<serde_json::Value>,
    pub stats: QueryStats,
}

#[async_trait]
pub trait QueryExecutor: Send + Sync {
    async fn query(
        &self,
        statement: &str,
        params: serde_json::Value,
    ) -> Result<QueryOutcome, StorageError>;
}
