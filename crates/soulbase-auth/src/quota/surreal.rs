#![allow(clippy::result_large_err)]

use crate::model::{Action, QuotaKey, QuotaOutcome};
use crate::prelude::AuthError;
use crate::quota::store::{QuotaConfig, QuotaStore};
use async_trait::async_trait;
use serde_json::{Value, json};
use soulbase_storage::Session;
use soulbase_storage::spi::query::QueryOutcome;
use soulbase_storage::surreal::SurrealDatastore;
use soulbase_storage::{Datastore, QueryExecutor, Transaction};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone)]
pub struct SurrealQuotaStore {
    datastore: SurrealDatastore,
    config: QuotaConfig,
}

impl SurrealQuotaStore {
    const TABLE: &'static str = "auth_quota_usage";

    pub fn new(datastore: &SurrealDatastore, config: QuotaConfig) -> Self {
        Self {
            datastore: datastore.clone(),
            config,
        }
    }

    fn record_key(key: &QuotaKey) -> String {
        format!(
            "{}:{}:{}:{}",
            key.tenant.0,
            key.subject_id.0,
            key.resource.0,
            Self::action_label(&key.action)
        )
    }

    fn action_label(action: &Action) -> String {
        format!("{:?}", action)
    }

    fn now_millis() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
    }

    fn parse_usage_limit(
        outcome: QueryOutcome,
        default_limit: u64,
    ) -> Result<(u64, u64), AuthError> {
        let mut usage = 0u64;
        let mut limit = default_limit;
        if let Some(Value::Object(row)) = outcome.rows.into_iter().next() {
            if let Some(value) = row.get("usage").and_then(Value::as_u64) {
                usage = value;
            }
            if let Some(value) = row.get("limit").and_then(Value::as_u64) {
                limit = value;
            }
        }
        Ok((usage, limit))
    }
}

#[async_trait]
impl QuotaStore for SurrealQuotaStore {
    async fn check_and_consume(
        &self,
        key: &QuotaKey,
        cost: u64,
    ) -> Result<QuotaOutcome, AuthError> {
        let session = self
            .datastore
            .session()
            .await
            .map_err(|err| AuthError(err.into_inner()))?;
        let mut tx = session
            .begin()
            .await
            .map_err(|err| AuthError(err.into_inner()))?;

        let select_value = match tx
            .query(
                r#"
                SELECT usage, limit
                FROM type::thing($table, $key)
                LIMIT 1;
                "#,
                json!({
                    "table": Self::TABLE,
                    "key": Self::record_key(key),
                }),
            )
            .await
        {
            Ok(value) => value,
            Err(err) => {
                if let Err(rollback) = tx.rollback().await {
                    return Err(AuthError(rollback.into_inner()));
                }
                return Err(AuthError(err.into_inner()));
            }
        };

        let (current_usage, current_limit) =
            match Self::parse_usage_limit(select_value, self.config.default_limit) {
                Ok(tuple) => tuple,
                Err(err) => {
                    if let Err(rollback) = tx.rollback().await {
                        return Err(AuthError(rollback.into_inner()));
                    }
                    return Err(err);
                }
            };

        if current_usage.saturating_add(cost) > current_limit {
            return match tx.rollback().await {
                Ok(()) => Ok(QuotaOutcome::BudgetExceeded),
                Err(err) => Err(AuthError(err.into_inner())),
            };
        }

        let new_usage = current_usage.saturating_add(cost);

        if let Err(err) = tx
            .query(
                r#"
                UPSERT type::thing($table, $key) CONTENT {
                    tenant: $tenant,
                    subject: $subject,
                    resource: $resource,
                    action: $action,
                    usage: $usage,
                    limit: $limit,
                    updated_at: $updated_at
                } RETURN NONE;
                "#,
                json!({
                    "table": Self::TABLE,
                    "key": Self::record_key(key),
                    "tenant": key.tenant.0,
                    "subject": key.subject_id.0,
                    "resource": key.resource.0,
                    "action": Self::action_label(&key.action),
                    "usage": new_usage,
                    "limit": current_limit,
                    "updated_at": Self::now_millis(),
                }),
            )
            .await
        {
            if let Err(rollback) = tx.rollback().await {
                return Err(AuthError(rollback.into_inner()));
            }
            return Err(AuthError(err.into_inner()));
        }

        tx.commit()
            .await
            .map_err(|err| AuthError(err.into_inner()))?;
        Ok(QuotaOutcome::Allowed)
    }
}
