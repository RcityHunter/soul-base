use crate::snapshot::ConfigSnapshot;
use arc_swap::ArcSwap;
use std::sync::Arc;

pub struct SnapshotSwitch {
    current: ArcSwap<ConfigSnapshot>,
    lkg: ArcSwap<ConfigSnapshot>,
}

impl SnapshotSwitch {
    pub fn new(initial: Arc<ConfigSnapshot>) -> Self {
        Self {
            current: ArcSwap::from(initial.clone()),
            lkg: ArcSwap::from(initial),
        }
    }

    pub fn get(&self) -> Arc<ConfigSnapshot> {
        self.current.load_full()
    }

    pub fn swap(&self, next: Arc<ConfigSnapshot>) {
        let previous = self.current.swap(next);
        self.lkg.store(previous);
    }

    pub fn rollback(&self) -> Arc<ConfigSnapshot> {
        let snapshot = self.lkg.load_full();
        self.current.store(snapshot.clone());
        snapshot
    }
}
