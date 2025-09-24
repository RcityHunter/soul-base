pub mod codec;
pub mod config;
pub mod errors;
pub mod invalidate;
pub mod key;
pub mod layer;
pub mod metrics;
pub mod policy;
pub mod prelude;
pub mod r#trait;

pub use codec::{Codec, JsonCodec};
#[cfg(feature = "redis")]
pub use config::RedisConfig;
pub use errors::CacheError;
pub use invalidate::{InvalidateEvent, InvalidateSignal};
pub use key::{build_key, CacheKey, KeyParts};
pub use layer::local_lru::{CacheEntry, LocalLru};
pub use layer::memory::MemoryBackend;
pub use layer::mod_::TwoTierCache;
#[cfg(feature = "redis")]
pub use layer::redis::RedisBackend;
pub use layer::remote::{RemoteCache, RemoteHandle};
pub use layer::singleflight::Flight;
pub use metrics::{SimpleStats, StatsSnapshot};
pub use policy::{Admission, CachePolicy, SwrPolicy};
pub use r#trait::{Cache, Invalidation, SingleFlight, Stats};
