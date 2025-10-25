use std::sync::Arc;

#[cfg(feature = "kafka")]
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use soulbase_config::prelude::ConfigSnapshot;
use soulbase_tx::errors::TxError;
use soulbase_tx::outbox::Transport;
use soulbase_types::tenant::TenantId;

use crate::config::{self, QueueBackendKind, QueueInfraConfig};
use crate::errors::InfraError;

#[cfg(feature = "kafka")]
use crate::config::KafkaQueueConfig;
#[cfg(feature = "kafka")]
use rdkafka::config::ClientConfig;
#[cfg(feature = "kafka")]
use rdkafka::producer::{FutureProducer, FutureRecord};

#[derive(Clone)]
pub struct QueueHandle {
    pub backend: QueueBackendKind,
    pub transport: Arc<dyn Transport>,
}

pub async fn build_queue_transport(
    snapshot: &ConfigSnapshot,
    tenant: &TenantId,
) -> Result<QueueHandle, InfraError> {
    let infra = config::load(snapshot)?;
    build_queue_for(infra.queue.unwrap_or_default(), tenant).await
}

pub async fn build_queue_for(
    queue_cfg: QueueInfraConfig,
    tenant: &TenantId,
) -> Result<QueueHandle, InfraError> {
    match queue_cfg.kind {
        QueueBackendKind::Noop => Ok(QueueHandle {
            backend: QueueBackendKind::Noop,
            transport: Arc::new(NoopTransport),
        }),
        QueueBackendKind::Kafka => build_kafka(queue_cfg, tenant).await,
    }
}

struct NoopTransport;

#[async_trait]
impl Transport for NoopTransport {
    async fn send(&self, _topic: &str, _payload: &Value) -> Result<(), TxError> {
        Ok(())
    }
}

#[cfg(feature = "kafka")]
async fn build_kafka(
    queue_cfg: QueueInfraConfig,
    tenant: &TenantId,
) -> Result<QueueHandle, InfraError> {
    let kafka_cfg = queue_cfg
        .kafka
        .clone()
        .ok_or_else(|| InfraError::config("infra.queue.kafka missing for kafka backend"))?;
    let transport = KafkaTransport::new(kafka_cfg, tenant)?;
    Ok(QueueHandle {
        backend: QueueBackendKind::Kafka,
        transport: Arc::new(transport),
    })
}

#[cfg(not(feature = "kafka"))]
async fn build_kafka(
    _queue_cfg: QueueInfraConfig,
    _tenant: &TenantId,
) -> Result<QueueHandle, InfraError> {
    Err(InfraError::feature_disabled(
        "kafka",
        "enable the 'kafka' feature on soulbase-infra to use Kafka transport",
    ))
}

#[cfg(feature = "kafka")]
struct KafkaTransport {
    producer: FutureProducer,
    topic_prefix: String,
    timeout: Duration,
}

#[cfg(feature = "kafka")]
impl KafkaTransport {
    fn new(cfg: KafkaQueueConfig, tenant: &TenantId) -> Result<Self, InfraError> {
        if cfg.brokers.is_empty() {
            return Err(InfraError::config(
                "infra.queue.kafka.brokers must not be empty",
            ));
        }

        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &cfg.brokers.join(","))
            .set("linger.ms", &cfg.linger_ms.to_string())
            .set("acks", &cfg.acks)
            .set("message.timeout.ms", &cfg.delivery_timeout_ms.to_string());

        if let Some(security) = cfg.security.as_ref() {
            if let Some(protocol) = &security.security_protocol {
                client_config.set("security.protocol", protocol);
            }
            if let Some(mechanism) = &security.sasl_mechanism {
                client_config.set("sasl.mechanism", mechanism);
            }
            if let Some(username) = &security.sasl_username {
                client_config.set("sasl.username", username);
            }
            if let Some(password) = &security.sasl_password {
                client_config.set("sasl.password", password);
            }
            if let Some(ca) = &security.ca_location {
                client_config.set("ssl.ca.location", ca);
            }
        }

        let producer = client_config
            .create()
            .map_err(|err| InfraError::provider_unavailable(&format!("kafka producer: {err}")))?;

        let topic_prefix = config::tenant_replace(&cfg.topic_prefix, tenant);

        Ok(Self {
            producer,
            topic_prefix,
            timeout: Duration::from_millis(cfg.delivery_timeout_ms.max(1)),
        })
    }

    fn topic_name(&self, channel: &str) -> String {
        if self.topic_prefix.is_empty() {
            channel.to_string()
        } else {
            format!("{}.{channel}", self.topic_prefix)
        }
    }
}

#[cfg(feature = "kafka")]
#[async_trait]
impl Transport for KafkaTransport {
    async fn send(&self, topic: &str, payload: &Value) -> Result<(), TxError> {
        let data = serde_json::to_vec(payload)
            .map_err(|err| TxError::bad_request(&format!("invalid payload: {err}")))?;
        let topic = self.topic_name(topic);
        let record: FutureRecord<'_, [u8], _> = FutureRecord::to(&topic).payload(&data);
        self.producer
            .send(record, self.timeout)
            .await
            .map_err(|(err, _)| {
                TxError::provider_unavailable(&format!("kafka send failed: {err}"))
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use soulbase_config::model::ProvenanceEntry;
    use soulbase_config::prelude::{ConfigSnapshot, SnapshotVersion};
    use soulbase_types::tenant::TenantId;

    fn noop_snapshot() -> ConfigSnapshot {
        let tree = serde_json::json!({
            "infra": {
                "queue": {
                    "kind": "noop"
                }
            }
        });
        ConfigSnapshot::from_tree(
            tree,
            SnapshotVersion("queue-test".into()),
            Vec::<ProvenanceEntry>::new(),
            None,
        )
    }

    #[tokio::test]
    async fn builds_noop_transport_when_not_configured() {
        let snapshot = noop_snapshot();
        let tenant = TenantId("tenant".into());
        let handle = build_queue_transport(&snapshot, &tenant)
            .await
            .expect("queue");
        assert_eq!(handle.backend, QueueBackendKind::Noop);
        let result = handle
            .transport
            .send("channel", &serde_json::json!({"ok": true}))
            .await;
        assert!(result.is_ok());
    }
}
