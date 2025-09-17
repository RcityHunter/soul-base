use crate::errors::ConfigError;
use crate::model::KeyPath;
use async_trait::async_trait;
use futures::channel::mpsc::Sender;

pub type WatchTx = Sender<ChangeNotice>;

#[derive(Clone, Debug)]
pub struct ChangeNotice {
    pub source_id: String,
    pub changed: Vec<KeyPath>,
    pub ts: i64,
}

#[async_trait]
pub trait Watcher: Send + Sync {
    async fn run(&self, tx: WatchTx) -> Result<(), ConfigError>;
}
