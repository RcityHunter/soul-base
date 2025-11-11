use super::*;
use crate::errors::ConfigError;
use crate::model::{KeyPath, Layer, ProvenanceEntry};
use crate::watch::{ChangeNotice, WatchTx};
use chrono::Utc;
use futures::SinkExt;
use parking_lot::RwLock;
use serde_json::{Map, Value};
use std::sync::Arc;
use tokio::sync::watch;

pub struct MemorySource {
    id: &'static str,
    state: Arc<RwLock<Map<String, Value>>>,
    notifier: watch::Sender<Vec<String>>,
}

impl MemorySource {
    pub fn new(id: &'static str) -> Self {
        let (tx, _) = watch::channel(Vec::new());
        Self {
            id,
            state: Arc::new(RwLock::new(Map::new())),
            notifier: tx,
        }
    }

    pub fn id(&self) -> &'static str {
        self.id
    }

    pub fn set(&self, key: &str, value: Value) {
        let mut guard = self.state.write();
        let map = &mut *guard;
        crate::access::set_path(map, key, value);
        let _ = self.notifier.send(vec![key.to_string()]);
    }

    pub fn merge(&self, map: Map<String, Value>) {
        let mut guard = self.state.write();
        let dst = &mut *guard;
        merge_object(dst, map.clone());
        let changed: Vec<String> = map.keys().cloned().collect();
        let _ = self.notifier.send(changed);
    }
}

fn merge_object(dst: &mut Map<String, Value>, src: Map<String, Value>) {
    for (key, value) in src {
        match (dst.get_mut(&key), value) {
            (Some(Value::Object(dst_obj)), Value::Object(src_obj)) => {
                merge_object(dst_obj, src_obj)
            }
            (_, v) => {
                dst.insert(key, v);
            }
        }
    }
}

#[async_trait::async_trait]
impl Source for MemorySource {
    fn id(&self) -> &'static str {
        self.id
    }

    async fn load(&self) -> Result<SourceSnapshot, ConfigError> {
        let map = self.state.read().clone();
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
        true
    }

    async fn watch(&self, tx: WatchTx) -> Result<(), ConfigError> {
        let mut rx = self.notifier.subscribe();
        let id = self.id;
        let mut tx = tx;
        tokio::spawn(async move {
            loop {
                if rx.changed().await.is_err() {
                    break;
                }
                let changed = rx.borrow().clone();
                if changed.is_empty() {
                    continue;
                }
                let notice = ChangeNotice {
                    source_id: id.to_string(),
                    changed: changed.iter().map(|key| KeyPath(key.clone())).collect(),
                    ts: Utc::now().timestamp_millis(),
                };
                if tx.send(notice).await.is_err() {
                    break;
                }
            }
        });
        Ok(())
    }
}
