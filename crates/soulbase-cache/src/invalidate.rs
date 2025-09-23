use std::sync::Arc;

use tokio::sync::broadcast;

use crate::errors::CacheError;
use crate::key::CacheKey;

#[derive(Clone, Debug)]
pub struct InvalidateEvent {
    pub key: String,
}

#[derive(Clone)]
pub struct InvalidateSignal {
    sender: Arc<broadcast::Sender<InvalidateEvent>>,
}

impl InvalidateSignal {
    pub fn new(capacity: usize) -> Self {
        let (tx, _rx) = broadcast::channel(capacity.max(16));
        Self {
            sender: Arc::new(tx),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<InvalidateEvent> {
        self.sender.subscribe()
    }

    pub async fn invalidate(&self, key: &CacheKey) -> Result<(), CacheError> {
        let _ = self.sender.send(InvalidateEvent {
            key: key.as_str().to_string(),
        });
        Ok(())
    }
}
