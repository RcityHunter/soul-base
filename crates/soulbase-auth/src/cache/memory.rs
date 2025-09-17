use super::*;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::time::{Duration, Instant};

struct Entry {
    decision: Decision,
    expires_at: Instant,
}

pub struct MemoryDecisionCache {
    ttl_default: Duration,
    map: RwLock<HashMap<DecisionKey, Entry>>,
}

impl MemoryDecisionCache {
    pub fn new(ttl_ms: u64) -> Self {
        Self {
            ttl_default: Duration::from_millis(ttl_ms.max(1)),
            map: RwLock::new(HashMap::new()),
        }
    }
}

#[async_trait::async_trait]
impl DecisionCache for MemoryDecisionCache {
    async fn get(&self, key: &DecisionKey) -> Option<Decision> {
        let now = Instant::now();
        let map = self.map.read();
        map.get(key)
            .filter(|entry| entry.expires_at > now)
            .map(|entry| entry.decision.clone())
    }

    async fn put(&self, key: DecisionKey, decision: &Decision) {
        let ttl = if decision.cache_ttl_ms == 0 {
            self.ttl_default
        } else {
            Duration::from_millis(decision.cache_ttl_ms as u64)
        };
        let expires_at = Instant::now() + ttl;
        let mut map = self.map.write();
        map.insert(
            key,
            Entry {
                decision: decision.clone(),
                expires_at,
            },
        );
    }
}
