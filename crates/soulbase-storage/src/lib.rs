pub mod errors;
pub mod model;
pub mod observe;
pub mod prelude;

pub mod spi {
    pub mod datastore;
    pub mod graph;
    pub mod health;
    pub mod migrate;
    pub mod query;
    pub mod repo;
    pub mod search;
    pub mod session;
    pub mod tx;
    pub mod vector;

    pub use datastore::*;
    pub use graph::*;
    pub use health::*;
    pub use migrate::*;
    pub use query::*;
    pub use repo::*;
    pub use search::*;
    pub use session::*;
    pub use tx::*;
    pub use vector::*;
}

#[cfg(feature = "mock")]
pub mod mock;
pub mod surreal;

pub use errors::StorageError;
pub use model::*;
pub use spi::*;
