#![allow(dead_code)]

pub mod config;

#[cfg(feature = "surreal")]
pub mod binder;
#[cfg(feature = "surreal")]
pub mod datastore;
#[cfg(feature = "surreal")]
pub mod errors;
#[cfg(feature = "surreal")]
pub mod mapper;
#[cfg(feature = "surreal")]
pub mod migrate;
#[cfg(feature = "surreal")]
pub mod observe;
#[cfg(feature = "surreal")]
pub mod session;
#[cfg(feature = "surreal")]
pub mod tx;

pub use config::SurrealConfig;

#[cfg(feature = "surreal")]
pub use datastore::SurrealDatastore;
#[cfg(feature = "surreal")]
pub use mapper::SurrealMapper;
#[cfg(feature = "surreal")]
pub use migrate::SurrealMigrator;
#[cfg(feature = "surreal")]
pub use session::SurrealSession;
#[cfg(feature = "surreal")]
pub use tx::SurrealTransaction;

#[cfg(not(feature = "surreal"))]
mod stub;
#[cfg(not(feature = "surreal"))]
pub use stub::*;
