pub mod errors;
pub mod model;
pub mod observe;
pub mod prelude;

pub mod spi {
    pub mod graph;
    pub mod health;
    pub mod migrate;
    pub mod query;
    pub mod repo;
    pub mod search;
    pub mod vector;

    pub use graph::*;
    pub use health::*;
    pub use migrate::*;
    pub use query::*;
    pub use repo::*;
    pub use search::*;
    pub use vector::*;
}

#[cfg(feature = "mock")]
pub mod mock;
pub mod surreal;

pub use errors::StorageError;
pub use model::*;
pub use spi::*;
