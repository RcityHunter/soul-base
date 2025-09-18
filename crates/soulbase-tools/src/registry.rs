use crate::errors::ToolError;
use crate::manifest::{ToolId, ToolManifest};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use soulbase_types::prelude::TenantId;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct ToolState {
    pub manifest: ToolManifest,
    pub enabled: bool,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct AvailableSpec {
    pub manifest: ToolManifest,
}

#[derive(Clone, Debug, Default)]
pub struct ListFilter {
    pub tag: Option<String>,
    pub include_disabled: bool,
}

#[async_trait]
pub trait ToolRegistry: Send + Sync {
    async fn upsert(&self, tenant: &TenantId, manifest: ToolManifest) -> Result<(), ToolError>;
    async fn disable(&self, tenant: &TenantId, tool: &ToolId) -> Result<(), ToolError>;
    async fn get(
        &self,
        tenant: &TenantId,
        tool: &ToolId,
    ) -> Result<Option<AvailableSpec>, ToolError>;
    async fn list(
        &self,
        tenant: &TenantId,
        filter: &ListFilter,
    ) -> Result<Vec<AvailableSpec>, ToolError>;
}

#[derive(Clone, Default)]
pub struct InMemoryRegistry {
    inner: Arc<RwLock<HashMap<String, HashMap<ToolId, ToolState>>>>,
}

impl InMemoryRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    fn tenant_bucket<'a>(
        lock: &'a mut HashMap<String, HashMap<ToolId, ToolState>>,
        tenant: &TenantId,
    ) -> &'a mut HashMap<ToolId, ToolState> {
        lock.entry(tenant.0.clone()).or_default()
    }
}

#[async_trait]
impl ToolRegistry for InMemoryRegistry {
    async fn upsert(&self, tenant: &TenantId, manifest: ToolManifest) -> Result<(), ToolError> {
        let mut guard = self.inner.write();
        let bucket = Self::tenant_bucket(&mut guard, tenant);
        let state = ToolState {
            manifest,
            enabled: true,
            updated_at: Utc::now(),
        };
        bucket.insert(state.manifest.id.clone(), state);
        Ok(())
    }

    async fn disable(&self, tenant: &TenantId, tool: &ToolId) -> Result<(), ToolError> {
        let mut guard = self.inner.write();
        let bucket = guard
            .get_mut(&tenant.0)
            .ok_or_else(|| ToolError::not_found(&tool.0))?;
        if let Some(state) = bucket.get_mut(tool) {
            state.enabled = false;
            state.updated_at = Utc::now();
            Ok(())
        } else {
            Err(ToolError::not_found(&tool.0))
        }
    }

    async fn get(
        &self,
        tenant: &TenantId,
        tool: &ToolId,
    ) -> Result<Option<AvailableSpec>, ToolError> {
        let guard = self.inner.read();
        let spec = guard
            .get(&tenant.0)
            .and_then(|bucket| bucket.get(tool))
            .filter(|state| state.enabled)
            .map(|state| AvailableSpec {
                manifest: state.manifest.clone(),
            });
        Ok(spec)
    }

    async fn list(
        &self,
        tenant: &TenantId,
        filter: &ListFilter,
    ) -> Result<Vec<AvailableSpec>, ToolError> {
        let guard = self.inner.read();
        let mut out = Vec::new();
        if let Some(bucket) = guard.get(&tenant.0) {
            for state in bucket.values() {
                if !filter.include_disabled && !state.enabled {
                    continue;
                }
                if let Some(tag) = &filter.tag {
                    if !state.manifest.tags.iter().any(|t| t == tag) {
                        continue;
                    }
                }
                out.push(AvailableSpec {
                    manifest: state.manifest.clone(),
                });
            }
        }
        Ok(out)
    }
}
