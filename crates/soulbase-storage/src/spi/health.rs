use crate::errors::StorageError;
use async_trait::async_trait;

#[async_trait]
pub trait HealthCheck: Send + Sync {
    async fn ping(&self) -> Result<(), StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Clone, Default)]
    struct CountingHealthCheck {
        calls: Arc<AtomicUsize>,
        fail: bool,
    }

    #[async_trait]
    impl HealthCheck for CountingHealthCheck {
        async fn ping(&self) -> Result<(), StorageError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                Err(StorageError::provider_unavailable("forced failure"))
            } else {
                Ok(())
            }
        }
    }

    #[tokio::test]
    async fn ping_tracks_calls_and_failures() {
        let healthy = CountingHealthCheck::default();
        healthy.ping().await.expect("healthy ping");
        assert_eq!(healthy.calls.load(Ordering::SeqCst), 1);

        let failing = CountingHealthCheck {
            calls: Arc::new(AtomicUsize::new(0)),
            fail: true,
        };
        let err = failing.ping().await.expect_err("failing ping");
        assert!(err.to_string().contains("forced failure"));
        assert_eq!(failing.calls.load(Ordering::SeqCst), 1);
    }
}
