use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use soulbase_tools::errors::ToolError;
use soulbase_tools::prelude::{
    InvokeRequest, InvokeResult, InvokeStatus, Invoker, InvokerImpl, PreflightOutput, ToolCall,
};
use soulbase_tx::prelude::{
    Dispatcher as TxDispatcher, RetryPolicy, SurrealDeadStore, SurrealOutboxStore, Transport,
    TxError,
};
use soulbase_types::tenant::TenantId;

use crate::service::infra::InfraManager;

pub struct OutboxDispatcher {
    infra: Arc<InfraManager>,
    outbox: SurrealOutboxStore,
    dead: SurrealDeadStore,
    worker_id: String,
    lease_ms: u64,
    batch: u32,
    group_by_key: bool,
    retry: RetryPolicy,
}

impl OutboxDispatcher {
    pub fn new(
        infra: Arc<InfraManager>,
        outbox: SurrealOutboxStore,
        dead: SurrealDeadStore,
    ) -> Self {
        Self {
            infra,
            outbox,
            dead,
            worker_id: "llm-outbox-worker".into(),
            lease_ms: 30_000,
            batch: 32,
            group_by_key: true,
            retry: RetryPolicy::default(),
        }
    }

    pub async fn dispatch(&self, tenant: &TenantId) -> Result<(), TxError> {
        let queue = self
            .infra
            .queue_for(tenant)
            .await
            .map_err(|err| TxError::from(err.into_inner()))?;

        let dispatcher = TxDispatcher {
            transport: TransportAdapter(queue.transport.clone()),
            store: self.outbox.clone(),
            dead: self.dead.clone(),
            worker_id: self.worker_id.clone(),
            lease_ms: self.lease_ms,
            batch: self.batch,
            group_by_key: self.group_by_key,
            backoff: Box::new(self.retry.clone()),
        };

        dispatcher.tick(tenant, Utc::now().timestamp_millis()).await
    }
}

pub struct OutboxInvoker {
    inner: Arc<InvokerImpl>,
    dispatcher: Arc<OutboxDispatcher>,
}

impl OutboxInvoker {
    pub fn new(inner: Arc<InvokerImpl>, dispatcher: Arc<OutboxDispatcher>) -> Self {
        Self { inner, dispatcher }
    }
}

#[async_trait]
impl Invoker for OutboxInvoker {
    async fn preflight(&self, call: &ToolCall) -> Result<PreflightOutput, ToolError> {
        self.inner.preflight(call).await
    }

    async fn invoke(&self, request: InvokeRequest) -> Result<InvokeResult, ToolError> {
        let tenant = request.call.tenant.clone();
        let result = self.inner.invoke(request).await?;
        if matches!(result.status, InvokeStatus::Ok) {
            if let Err(err) = self.dispatcher.dispatch(&tenant).await {
                tracing::warn!(
                    target = "soulbase::llm",
                    tenant = %tenant.0,
                    error = %format!("{err:?}"),
                    "failed to dispatch outbox events"
                );
            }
        }
        Ok(result)
    }
}

#[derive(Clone)]
struct TransportAdapter(Arc<dyn Transport>);

#[async_trait]
impl Transport for TransportAdapter {
    async fn send(&self, topic: &str, payload: &serde_json::Value) -> Result<(), TxError> {
        self.0.send(topic, payload).await
    }
}
