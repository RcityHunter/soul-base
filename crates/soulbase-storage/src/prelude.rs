pub use crate::errors::StorageError;
pub use crate::model::{Entity, MigrationVersion, Page, QueryParams};
pub use crate::spi::*;

#[cfg(feature = "mock")]
pub use crate::mock::{
    InMemoryGraph, InMemoryMigrator, InMemoryRepository, InMemoryVector, MockDatastore,
};

#[cfg(feature = "surreal")]
pub use crate::surreal::SurrealRepository;
