use crate::model::{Decision, DecisionKey};
use async_trait::async_trait;

pub mod memory;

#[async_trait]
pub trait DecisionCache: Send + Sync {
    async fn get(&self, key: &DecisionKey) -> Option<Decision>;
    async fn put(&self, key: DecisionKey, decision: &Decision);
    async fn revoke(&self, _subject_id: &soulbase_types::prelude::Id) {}
}
