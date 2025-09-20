use super::IdempotencyStore;
use crate::errors::InterceptError;
use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::HashSet;

pub struct MemoryIdempotencyStore {
    keys: Mutex<HashSet<String>>,
}

impl MemoryIdempotencyStore {
    pub fn new() -> Self {
        Self {
            keys: Mutex::new(HashSet::new()),
        }
    }
}

impl Default for MemoryIdempotencyStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl IdempotencyStore for MemoryIdempotencyStore {
    async fn check_or_insert(&self, key: &str) -> Result<bool, InterceptError> {
        let mut guard = self.keys.lock();
        if guard.contains(key) {
            Ok(false)
        } else {
            guard.insert(key.to_string());
            Ok(true)
        }
    }
}
