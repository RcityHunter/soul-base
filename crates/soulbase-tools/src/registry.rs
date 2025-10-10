use crate::errors::ToolError;
use crate::manifest::{ToolId, ToolManifest};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use parking_lot::RwLock;

use soulbase_types::prelude::TenantId;
use std::collections::HashMap;
use std::sync::Arc;

#[cfg(feature = "registry_surreal")]
use soulbase_storage::prelude::{Datastore, QueryExecutor};
#[cfg(feature = "registry_surreal")]
use soulbase_storage::surreal::SurrealDatastore;

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

#[cfg(feature = "registry_surreal")]
#[derive(Clone)]
pub struct SurrealToolRegistry {
    datastore: Arc<SurrealDatastore>,
    cache: Arc<RwLock<HashMap<String, HashMap<ToolId, ToolState>>>>,
}

#[cfg(feature = "registry_surreal")]
impl SurrealToolRegistry {
    pub fn new(datastore: Arc<SurrealDatastore>) -> Self {
        Self {
            datastore,
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn cache_get(&self, tenant: &TenantId, tool: &ToolId) -> Option<ToolState> {
        let guard = self.cache.read();
        guard
            .get(&tenant.0)
            .and_then(|bucket| bucket.get(tool))
            .cloned()
    }

    fn cache_upsert(&self, tenant: &TenantId, state: ToolState) {
        let mut guard = self.cache.write();
        let bucket = guard.entry(tenant.0.clone()).or_default();
        bucket.insert(state.manifest.id.clone(), state);
    }

    fn cache_disable(&self, tenant: &TenantId, tool: &ToolId, updated_at: DateTime<Utc>) {
        let mut guard = self.cache.write();
        if let Some(bucket) = guard.get_mut(&tenant.0) {
            if let Some(state) = bucket.get_mut(tool) {
                state.enabled = false;
                state.updated_at = updated_at;
            }
        }
    }

    fn cache_replace(&self, tenant: &TenantId, states: Vec<ToolState>) {
        let mut guard = self.cache.write();
        let mut bucket = HashMap::new();
        for state in states {
            bucket.insert(state.manifest.id.clone(), state);
        }
        guard.insert(tenant.0.clone(), bucket);
    }

    fn cache_list(&self, tenant: &TenantId) -> Option<Vec<ToolState>> {
        let guard = self.cache.read();
        guard
            .get(&tenant.0)
            .map(|bucket| bucket.values().cloned().collect())
    }

    async fn session(
        &self,
    ) -> Result<soulbase_storage::surreal::session::SurrealSession, ToolError> {
        self.datastore.session().await.map_err(ToolError::from)
    }

    async fn upsert_record(
        &self,
        tenant: &TenantId,
        manifest: &ToolManifest,
    ) -> Result<ToolState, ToolError> {
        let session = self.session().await?;
        let now_ms = Utc::now().timestamp_millis();
        session
            .query(
                "UPSERT tool_registry CONTENT {
                    id: type::thing(\"tool_registry\", string::concat($tenant, \":\", $tool)),
                    tenant: $tenant,
                    tool_id: $tool,
                    manifest: $manifest,
                    enabled: true,
                    updated_at_ms: $updated_at
                } RETURN NONE",
                serde_json::json!({
                    "tenant": tenant.0,
                    "tool": manifest.id.0,
                    "manifest": manifest,
                    "updated_at": now_ms,
                }),
            )
            .await
            .map_err(ToolError::from)?;

        self.load_tool(tenant, &manifest.id)
            .await?
            .ok_or_else(|| ToolError::unknown("tool not found after upsert"))
    }

    async fn disable_record(
        &self,
        tenant: &TenantId,
        tool: &ToolId,
    ) -> Result<ToolState, ToolError> {
        let session = self.session().await?;
        let now_ms = Utc::now().timestamp_millis();
        session
            .query(
                "UPDATE type::thing(\"tool_registry\", string::concat($tenant, \":\", $tool))
                 SET enabled = false, updated_at_ms = $updated_at RETURN NONE",
                serde_json::json!({
                    "tenant": tenant.0,
                    "tool": tool.0,
                    "updated_at": now_ms,
                }),
            )
            .await
            .map_err(ToolError::from)?;
        self.load_tool(tenant, tool)
            .await?
            .ok_or_else(|| ToolError::not_found(&tool.0))
    }

    async fn load_tool(
        &self,
        tenant: &TenantId,
        tool: &ToolId,
    ) -> Result<Option<ToolState>, ToolError> {
        let session = self.session().await?;
        let response = session
            .query(
                "SELECT VALUE {
                    tenant: tenant,
                    tool_id: tool_id,
                    manifest: manifest,
                    enabled: enabled,
                    updated_at_ms: updated_at_ms
                 }
                 FROM tool_registry
                 WHERE tenant = $tenant AND tool_id = $tool
                 LIMIT 1",
                serde_json::json!({
                    "tenant": tenant.0,
                    "tool": tool.0,
                }),
            )
            .await
            .map_err(ToolError::from)?;
        let rows: Vec<serde_json::Value> = decode_records(response)?;
        if let Some(record_json) = rows.into_iter().next() {
            let state = value_to_state(record_json)?;
            self.cache_upsert(tenant, state.clone());
            Ok(Some(state))
        } else {
            let mut guard = self.cache.write();
            if let Some(bucket) = guard.get_mut(&tenant.0) {
                bucket.remove(tool);
            }
            Ok(None)
        }
    }

    async fn load_tenant(&self, tenant: &TenantId) -> Result<Vec<ToolState>, ToolError> {
        let session = self.session().await?;
        let response = session
            .query(
                "SELECT VALUE {
                    tenant: tenant,
                    tool_id: tool_id,
                    manifest: manifest,
                    enabled: enabled,
                    updated_at_ms: updated_at_ms
                 }
                 FROM tool_registry
                 WHERE tenant = $tenant",
                serde_json::json!({
                    "tenant": tenant.0,
                }),
            )
            .await
            .map_err(ToolError::from)?;
        let rows: Vec<serde_json::Value> = decode_records(response)?;
        let mut states = Vec::with_capacity(rows.len());
        for record_json in rows {
            states.push(value_to_state(record_json)?);
        }
        self.cache_replace(tenant, states.clone());
        Ok(states)
    }

    fn filter_states(states: Vec<ToolState>, filter: &ListFilter) -> Vec<AvailableSpec> {
        states
            .into_iter()
            .filter(|state| filter.include_disabled || state.enabled)
            .filter(|state| {
                if let Some(tag) = &filter.tag {
                    state.manifest.tags.iter().any(|t| t == tag)
                } else {
                    true
                }
            })
            .map(|state| AvailableSpec {
                manifest: state.manifest,
            })
            .collect()
    }
}

#[cfg(feature = "registry_surreal")]
#[async_trait]
impl ToolRegistry for SurrealToolRegistry {
    async fn upsert(&self, tenant: &TenantId, manifest: ToolManifest) -> Result<(), ToolError> {
        let state = self.upsert_record(tenant, &manifest).await?;
        self.cache_upsert(tenant, state);
        Ok(())
    }

    async fn disable(&self, tenant: &TenantId, tool: &ToolId) -> Result<(), ToolError> {
        let state = self.disable_record(tenant, tool).await?;
        self.cache_disable(tenant, tool, state.updated_at);
        Ok(())
    }

    async fn get(
        &self,
        tenant: &TenantId,
        tool: &ToolId,
    ) -> Result<Option<AvailableSpec>, ToolError> {
        if let Some(state) = self.cache_get(tenant, tool) {
            if state.enabled {
                return Ok(Some(AvailableSpec {
                    manifest: state.manifest,
                }));
            } else {
                return Ok(None);
            }
        }

        let state = self.load_tool(tenant, tool).await?;
        Ok(state.and_then(|state| {
            if state.enabled {
                Some(AvailableSpec {
                    manifest: state.manifest,
                })
            } else {
                None
            }
        }))
    }

    async fn list(
        &self,
        tenant: &TenantId,
        filter: &ListFilter,
    ) -> Result<Vec<AvailableSpec>, ToolError> {
        let states = if let Some(states) = self.cache_list(tenant) {
            states
        } else {
            self.load_tenant(tenant).await?
        };

        Ok(Self::filter_states(states, filter))
    }
}

#[cfg(feature = "registry_surreal")]
fn decode_records(value: serde_json::Value) -> Result<Vec<serde_json::Value>, ToolError> {
    match value {
        serde_json::Value::Array(items) => Ok(items),
        serde_json::Value::Null => Ok(Vec::new()),
        other => Ok(vec![other]),
    }
}

#[cfg(feature = "registry_surreal")]
fn value_to_state(value: serde_json::Value) -> Result<ToolState, ToolError> {
    let _tenant = value
        .get("tenant")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ToolError::unknown("missing tenant in tool registry record"))?;
    let tool_id = value
        .get("tool_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ToolError::unknown("missing tool_id in tool registry record"))?;
    let manifest_value = value
        .get("manifest")
        .cloned()
        .ok_or_else(|| ToolError::unknown("missing manifest in tool registry record"))?;
    let mut manifest: ToolManifest = serde_json::from_value(manifest_value)
        .map_err(|err| ToolError::unknown(&format!("manifest decode failed: {err}")))?;
    manifest.id = ToolId(tool_id.to_string());
    let enabled = value
        .get("enabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let updated_at_ms = value
        .get("updated_at_ms")
        .and_then(|v| v.as_i64())
        .unwrap_or_else(|| Utc::now().timestamp_millis());
    let updated_at =
        chrono::DateTime::<Utc>::from_timestamp_millis(updated_at_ms).unwrap_or_else(|| Utc::now());

    Ok(ToolState {
        manifest,
        enabled,
        updated_at,
    })
}

#[cfg(all(test, feature = "registry_surreal"))]
mod tests {
    use super::*;
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};
    use soulbase_storage::surreal::config::SurrealConfig;

    #[derive(Serialize, JsonSchema, Deserialize, Clone, Debug)]
    struct SampleInput {
        query: String,
    }

    #[derive(Serialize, JsonSchema, Deserialize, Clone, Debug)]
    struct SampleOutput {
        answer: String,
    }

    fn sample_manifest(id: &str, tags: &[&str]) -> ToolManifest {
        let input_schema = schemars::schema_for!(SampleInput);
        let output_schema = schemars::schema_for!(SampleOutput);
        ToolManifest {
            id: ToolId(id.to_string()),
            version: "1.0.0".into(),
            display_name: format!("Tool {id}"),
            description: "demo tool".into(),
            tags: tags.iter().map(|t| t.to_string()).collect(),
            input_schema,
            output_schema,
            scopes: vec![],
            capabilities: vec![],
            side_effect: crate::manifest::SideEffect::Read,
            safety_class: crate::manifest::SafetyClass::Low,
            consent: crate::manifest::ConsentPolicy {
                required: false,
                max_ttl_ms: None,
            },
            limits: crate::manifest::Limits {
                timeout_ms: 1_000,
                max_bytes_in: 16 * 1024,
                max_bytes_out: 16 * 1024,
                max_files: 0,
                max_depth: 1,
                max_concurrency: 1,
            },
            idempotency: crate::manifest::IdempoKind::None,
            concurrency: crate::manifest::ConcurrencyKind::Parallel,
        }
    }

    #[tokio::test]
    async fn surreal_registry_roundtrip() {
        let datastore = SurrealDatastore::connect(SurrealConfig::default())
            .await
            .expect("connect surreal");
        let registry = SurrealToolRegistry::new(Arc::new(datastore));
        let tenant = TenantId("tenant_a".into());
        let manifest = sample_manifest("demo.tool", &["alpha", "beta"]);

        registry
            .upsert(&tenant, manifest.clone())
            .await
            .expect("upsert");

        let spec = registry
            .get(&tenant, &manifest.id)
            .await
            .expect("get")
            .expect("tool available");
        assert_eq!(spec.manifest.display_name, manifest.display_name);

        let listed = registry
            .list(&tenant, &ListFilter::default())
            .await
            .expect("list");
        assert_eq!(listed.len(), 1);

        // Tag filtering
        let tagged = registry
            .list(
                &tenant,
                &ListFilter {
                    tag: Some("alpha".into()),
                    include_disabled: false,
                },
            )
            .await
            .expect("list with tag");
        assert_eq!(tagged.len(), 1);

        let none_tagged = registry
            .list(
                &tenant,
                &ListFilter {
                    tag: Some("gamma".into()),
                    include_disabled: false,
                },
            )
            .await
            .expect("list missing tag");
        assert!(none_tagged.is_empty());

        registry
            .disable(&tenant, &manifest.id)
            .await
            .expect("disable");

        let missing = registry
            .get(&tenant, &manifest.id)
            .await
            .expect("get after disable");
        assert!(missing.is_none());

        let disabled_visible = registry
            .list(
                &tenant,
                &ListFilter {
                    tag: None,
                    include_disabled: true,
                },
            )
            .await
            .expect("list disabled");
        assert_eq!(disabled_visible.len(), 1);
    }
}
