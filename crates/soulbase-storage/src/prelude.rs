pub use crate::errors::StorageError;
pub use crate::model::{
    awareness::AwarenessEvent, causal::CausalEdge, recall::RecallChunk, timeline::TimelineEvent,
    vector_manifest::VectorManifest, Entity, MigrationVersion, Page, QueryParams,
};
pub use crate::spi::*;

#[cfg(feature = "mock")]
pub use crate::mock::{
    InMemoryGraph, InMemoryMigrator, InMemoryRepository, InMemoryVector, MockDatastore,
};

#[cfg(feature = "surreal")]
pub use crate::surreal::{
    AwarenessEventRepo, CausalEdgeRepo, RecallChunkRepo, SurrealRepository, TimelineEventRepo,
    VectorManifestRepo,
};
