use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

use crate::key::CacheKey;
use crate::r#trait::SingleFlight;

#[derive(Default, Clone)]
pub struct Flight {
    inner: Arc<Mutex<HashMap<String, Arc<AsyncMutex<()>>>>>,
}

pub struct FlightGuard {
    _guard: OwnedMutexGuard<()>,
}

#[async_trait::async_trait]
impl SingleFlight for Flight {
    type Guard = FlightGuard;

    async fn acquire(&self, key: &CacheKey) -> Self::Guard {
        let mutex = {
            let mut map = self.inner.lock();
            map.entry(key.as_str().to_string())
                .or_insert_with(|| Arc::new(AsyncMutex::new(())))
                .clone()
        };
        let guard = mutex.lock_owned().await;
        FlightGuard { _guard: guard }
    }
}
