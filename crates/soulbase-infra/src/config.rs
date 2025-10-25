use soulbase_config::prelude::{ConfigSnapshot, NamespaceId};
use soulbase_types::tenant::TenantId;

use crate::errors::InfraError;

pub use soulbase_config::catalog::{
    BlobBackendKind, BlobInfra as BlobInfraConfig, CacheInfra as CacheInfraConfig, FsBlobConfig,
    InfraNamespace as InfraNamespaceConfig, KafkaQueueConfig, KafkaSecurityConfig,
    QueueBackendKind, QueueInfra as QueueInfraConfig, RedisCacheConfig, S3BlobConfig,
};

pub fn load(snapshot: &ConfigSnapshot) -> Result<InfraNamespaceConfig, InfraError> {
    let value = snapshot.ns(&NamespaceId::new("infra"));
    serde_json::from_value(value)
        .map_err(|err| InfraError::config(&format!("infra namespace decode failed: {err}")))
}

pub fn tenant_replace(template: &str, tenant: &TenantId) -> String {
    template.replace("{tenant}", tenant.0.as_str())
}
