use super::*;
use crate::errors::ConfigError;
use crate::model::{KeyPath, Layer, ProvenanceEntry};
use crate::watch::{ChangeNotice, WatchTx};
use chrono::Utc;
use futures::future::BoxFuture;
#[cfg(feature = "remote_http")]
use futures::FutureExt;
use futures::SinkExt;
use serde_json::Map;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{interval, MissedTickBehavior};

#[cfg(feature = "remote_http")]
use reqwest::Url;
#[cfg(feature = "remote_http")]
use serde_json::Value as JsonValue;

pub type FetchFn =
    dyn Fn() -> BoxFuture<'static, Result<Map<String, Value>, ConfigError>> + Send + Sync;

pub struct RemoteSource {
    id: &'static str,
    fetcher: Arc<FetchFn>,
    poll_interval: Option<Duration>,
    retry_backoff: Option<Duration>,
}

impl RemoteSource {
    pub fn new<F>(id: &'static str, fetcher: F) -> Self
    where
        F: Fn() -> BoxFuture<'static, Result<Map<String, Value>, ConfigError>>
            + Send
            + Sync
            + 'static,
    {
        Self {
            id,
            fetcher: Arc::new(fetcher),
            poll_interval: None,
            retry_backoff: None,
        }
    }

    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = Some(interval);
        self
    }

    pub fn with_backoff(mut self, backoff: Duration) -> Self {
        self.retry_backoff = Some(backoff);
        self
    }

    async fn fetch(&self) -> Result<Map<String, Value>, ConfigError> {
        (self.fetcher)().await
    }

    #[cfg(feature = "remote_http")]
    pub fn from_http(id: &'static str, client: reqwest::Client, url: Url) -> Self {
        let fetcher = move || {
            let client = client.clone();
            let url = url.clone();
            async move {
                let resp = client.get(url.clone()).send().await.map_err(|err| {
                    crate::errors::io_provider_unavailable("http", &err.to_string())
                })?;
                let json: JsonValue = resp
                    .json()
                    .await
                    .map_err(|err| crate::errors::schema_invalid("http json", &err.to_string()))?;
                match json {
                    JsonValue::Object(map) => Ok(map),
                    other => Err(crate::errors::schema_invalid(
                        "http json",
                        &format!("expected object, got {other:?}"),
                    )),
                }
            }
            .boxed()
        };
        Self::new(id, fetcher)
    }
}

#[async_trait::async_trait]
impl Source for RemoteSource {
    fn id(&self) -> &'static str {
        self.id
    }

    async fn load(&self) -> Result<SourceSnapshot, ConfigError> {
        let map = self.fetch().await?;
        Ok(SourceSnapshot {
            map,
            provenance: vec![ProvenanceEntry {
                key: KeyPath("**".into()),
                source_id: self.id.to_string(),
                layer: Layer::RemoteKV,
                version: None,
                ts_ms: Utc::now().timestamp_millis(),
            }],
        })
    }

    fn supports_watch(&self) -> bool {
        self.poll_interval.is_some()
    }

    async fn watch(&self, tx: WatchTx) -> Result<(), ConfigError> {
        let poll_interval = self.poll_interval.ok_or_else(|| {
            crate::errors::schema_invalid("remote", "poll_interval not configured")
        })?;

        let mut ticker = interval(poll_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut tx = tx;
        let id = self.id;
        let fetcher = self.fetcher.clone();
        let backoff = self.retry_backoff;
        tokio::spawn(async move {
            loop {
                ticker.tick().await;
                match fetcher().await {
                    Ok(_) => {
                        let notice = ChangeNotice {
                            source_id: id.to_string(),
                            changed: vec![KeyPath("**".into())],
                            ts: Utc::now().timestamp_millis(),
                        };
                        if tx.send(notice).await.is_err() {
                            break;
                        }
                    }
                    Err(err) => {
                        tracing::warn!(target = "soulbase::config", "remote fetch failed: {err:?}");
                        if let Some(delay) = backoff {
                            tokio::time::sleep(delay).await;
                        }
                    }
                }
            }
        });
        Ok(())
    }
}
