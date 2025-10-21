use crate::errors::StorageError;
use async_trait::async_trait;

#[async_trait]
pub trait QueryExecutor: Send + Sync {
    async fn query(
        &self,
        statement: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use serde_json::json;

    struct EchoExecutor;

    #[async_trait]
    impl QueryExecutor for EchoExecutor {
        async fn query(
            &self,
            statement: &str,
            params: serde_json::Value,
        ) -> Result<serde_json::Value, StorageError> {
            Ok(json!({"statement": statement, "params": params}))
        }
    }

    #[tokio::test]
    async fn query_executor_echoes_payload() {
        let exec = EchoExecutor;
        let result = exec
            .query("SELECT *", json!({"limit": 1}))
            .await
            .expect("query succeeds");
        assert_eq!(result["statement"], "SELECT *");
        assert_eq!(result["params"], json!({"limit": 1}));
    }
}
