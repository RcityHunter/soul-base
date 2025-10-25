pub use crate::{
    audit::{diff as diff_snapshot, ChangeRecord},
    catalog::{
        bootstrap_catalog, export_namespace_docs, validator_as_trait, BlobBackendKind, BlobInfra,
        CacheInfra, CatalogHandles, FsBlobConfig, InfraNamespace, KafkaQueueConfig,
        KafkaSecurityConfig, LlmBudget, LlmNamespace, LlmProvider, QueueBackendKind, QueueInfra,
        RedisCacheConfig, S3BlobConfig,
    },
    errors::ConfigError,
    loader::{Loader, LoaderBuilder},
    model::{Checksum, KeyPath, NamespaceId, ReloadClass, SnapshotVersion},
    schema::{FieldMeta, InMemorySchemaRegistry, SchemaRegistry},
    secrets::{
        CachingResolver, EnvSecretResolver, FileSecretResolver, KvSecretResolver,
        NoopSecretResolver, SecretResolver,
    },
    snapshot::ConfigSnapshot,
    source::{memory::MemorySource, remote::RemoteSource, Source, SourceSnapshot},
    switch::SnapshotSwitch,
    validate::{BasicValidator, SchemaValidator, Validator},
    watch::{ChangeNotice, WatchTx, Watcher},
};

#[cfg(feature = "schema_json")]
pub use crate::schema::register_namespace_struct;
