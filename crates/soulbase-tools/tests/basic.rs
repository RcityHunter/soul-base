use schemars::{schema_for, JsonSchema};
use serde::{Deserialize, Serialize};
use soulbase_auth::prelude::{QuotaKey, QuotaOutcome, QuotaStore};
use soulbase_auth::AuthFacade;
use soulbase_sandbox::prelude::{Mappings, PolicyConfig, Sandbox};
use soulbase_tools::prelude::*;
use soulbase_tx::memory::InMemoryOutboxStore;
use soulbase_tx::outbox::OutboxStore;
use soulbase_types::prelude::*;
use std::sync::{Arc, Mutex};
use tempfile::tempdir;

#[derive(Serialize, Deserialize, JsonSchema)]
struct NetInput {
    url: String,
}

#[derive(Serialize, Deserialize, JsonSchema)]
struct NetOutput {
    url: String,
    simulated: bool,
    host: Option<String>,
}

fn manifest_net_get() -> ToolManifest {
    ToolManifest {
        id: ToolId("net.http.get".into()),
        version: "1.0.0".into(),
        display_name: "HTTP GET".into(),
        description: "Fetch a URL via sandboxed GET".into(),
        tags: vec!["net".into(), "http".into()],
        input_schema: schema_for!(NetInput),
        output_schema: schema_for!(NetOutput),
        scopes: vec![],
        capabilities: vec![CapabilityDecl {
            domain: "net.http".into(),
            action: "get".into(),
            resource: "example.com".into(),
            attrs: serde_json::json!({}),
        }],
        side_effect: SideEffect::Network,
        safety_class: SafetyClass::Medium,
        consent: ConsentPolicy {
            required: false,
            max_ttl_ms: Some(60_000),
        },
        limits: Limits {
            timeout_ms: 10_000,
            max_bytes_in: 2_000_000,
            max_bytes_out: 1_000_000,
            max_files: 0,
            max_depth: 2,
            max_concurrency: 2,
        },
        idempotency: IdempoKind::Keyed,
        concurrency: ConcurrencyKind::Parallel,
    }
}

#[derive(Serialize, Deserialize, JsonSchema)]
struct FsInput {
    path: String,
}

#[derive(Serialize, Deserialize, JsonSchema)]
struct FsOutput {
    size: u64,
    preview: String,
    path: String,
}

fn manifest_fs_read(root: &str) -> ToolManifest {
    ToolManifest {
        id: ToolId("fs.read".into()),
        version: "1.0.0".into(),
        display_name: "FS Read".into(),
        description: "Read file via sandbox".into(),
        tags: vec!["fs".into()],
        input_schema: schema_for!(FsInput),
        output_schema: schema_for!(FsOutput),
        scopes: vec![],
        capabilities: vec![CapabilityDecl {
            domain: "fs".into(),
            action: "read".into(),
            resource: root.into(),
            attrs: serde_json::json!({}),
        }],
        side_effect: SideEffect::Read,
        safety_class: SafetyClass::Low,
        consent: ConsentPolicy {
            required: false,
            max_ttl_ms: Some(60_000),
        },
        limits: Limits {
            timeout_ms: 10_000,
            max_bytes_in: 1_000_000,
            max_bytes_out: 0,
            max_files: 0,
            max_depth: 2,
            max_concurrency: 1,
        },
        idempotency: IdempoKind::Keyed,
        concurrency: ConcurrencyKind::Parallel,
    }
}

fn subject(tenant: &TenantId) -> Subject {
    Subject {
        kind: SubjectKind::User,
        subject_id: Id("user-1".into()),
        tenant: tenant.clone(),
        claims: serde_json::Map::new(),
    }
}

#[tokio::test]
async fn register_preflight_and_invoke_tools() {
    let registry = Arc::new(InMemoryRegistry::new());
    let registry_dyn: Arc<dyn ToolRegistry> = registry.clone();

    let tenant = TenantId("tenant-a".into());
    registry
        .upsert(&tenant, manifest_net_get())
        .await
        .expect("register net");

    let root = tempdir().expect("tmp");
    let root_path = root.path().to_path_buf();
    let file_path = root_path.join("hello.txt");
    std::fs::write(&file_path, b"hello world").expect("write file");
    registry
        .upsert(
            &tenant,
            manifest_fs_read(root_path.to_string_lossy().as_ref()),
        )
        .await
        .expect("register fs");

    let auth = Arc::new(AuthFacade::minimal());
    let mut policy = PolicyConfig {
        mappings: Mappings {
            root_fs: root_path.display().to_string(),
            ..Mappings::default()
        },
        ..PolicyConfig::default()
    };
    policy.whitelists.domains.push("example.com".into());

    let invoker = InvokerImpl::new(registry_dyn, auth, Sandbox::minimal(), policy.clone());

    let call_net = ToolCall {
        tool_id: ToolId("net.http.get".into()),
        call_id: Id("call-1".into()),
        tenant: tenant.clone(),
        actor: subject(&tenant),
        origin: ToolOrigin::Llm,
        args: serde_json::json!({ "url": "https://example.com/demo" }),
        consent: None,
        idempotency_key: Some("idem-1".into()),
    };

    let pf = invoker.preflight(&call_net).await.expect("preflight net");
    assert!(pf.allow);
    let spec_net = pf.spec.clone().expect("spec");
    let profile_hash_net = pf.profile_hash.clone().expect("hash");
    let obligations_net = pf.obligations.clone();
    let planned_ops_net = pf.planned_ops.clone();
    let request = InvokeRequest {
        spec: spec_net.clone(),
        call: call_net.clone(),
        profile_hash: profile_hash_net.clone(),
        obligations: obligations_net.clone(),
        planned_ops: planned_ops_net.clone(),
    };
    let result = invoker.invoke(request).await.expect("invoke net");
    assert_eq!(result.status, InvokeStatus::Ok);
    let output = result.output.expect("output");
    assert_eq!(output.get("url").unwrap(), "https://example.com/demo");
    assert_eq!(output.get("simulated").unwrap(), true);

    let request_repeat = InvokeRequest {
        spec: spec_net,
        call: call_net.clone(),
        profile_hash: profile_hash_net,
        obligations: obligations_net,
        planned_ops: Vec::new(),
    };
    let repeat = invoker.invoke(request_repeat).await.expect("invoke repeat");
    assert_eq!(repeat.status, InvokeStatus::Ok);
    assert!(repeat.evidence_ref.is_none());

    let call_fs = ToolCall {
        tool_id: ToolId("fs.read".into()),
        call_id: Id("call-2".into()),
        tenant: tenant.clone(),
        actor: subject(&tenant),
        origin: ToolOrigin::System,
        args: serde_json::json!({ "path": "hello.txt" }),
        consent: None,
        idempotency_key: None,
    };

    let pf_fs = invoker.preflight(&call_fs).await.expect("preflight fs");
    assert!(pf_fs.allow);
    let request_fs = InvokeRequest {
        spec: pf_fs.spec.clone().unwrap(),
        call: call_fs,
        profile_hash: pf_fs.profile_hash.clone().unwrap(),
        obligations: pf_fs.obligations.clone(),
        planned_ops: pf_fs.planned_ops.clone(),
    };
    let fs_result = invoker.invoke(request_fs).await.expect("invoke fs");
    assert_eq!(fs_result.status, InvokeStatus::Ok);
    let fs_out = fs_result.output.expect("fs output");
    assert_eq!(fs_out.get("size").and_then(|v| v.as_u64()).unwrap(), 11);
}

#[derive(Clone)]
struct SpyQuota {
    records: Arc<Mutex<Vec<(QuotaKey, u64)>>>,
    outcome: QuotaOutcome,
}

impl SpyQuota {
    fn with_outcome(outcome: QuotaOutcome) -> (Self, Arc<Mutex<Vec<(QuotaKey, u64)>>>) {
        let records = Arc::new(Mutex::new(Vec::new()));
        (
            SpyQuota {
                records: records.clone(),
                outcome,
            },
            records,
        )
    }
}

#[async_trait::async_trait]
impl QuotaStore for SpyQuota {
    async fn check_and_consume(
        &self,
        key: &QuotaKey,
        cost: u64,
    ) -> Result<QuotaOutcome, soulbase_auth::errors::AuthError> {
        let mut guard = self.records.lock().expect("quota records lock");
        guard.push((key.clone(), cost));
        Ok(self.outcome.clone())
    }
}

#[tokio::test]
async fn preflight_invoke_pipeline_hits_quota_and_outbox() {
    let registry = Arc::new(InMemoryRegistry::new());
    let registry_dyn: Arc<dyn ToolRegistry> = registry.clone();

    let tenant = TenantId("tenant-z".into());
    registry
        .upsert(&tenant, manifest_net_get())
        .await
        .expect("register net");

    let root = tempdir().expect("tmp");
    let root_path = root.path().to_path_buf();
    std::fs::write(root_path.join("hello.txt"), b"hello quota").expect("write file");
    registry
        .upsert(
            &tenant,
            manifest_fs_read(root_path.to_string_lossy().as_ref()),
        )
        .await
        .expect("register fs");

    let (spy_quota, records_handle) = SpyQuota::with_outcome(QuotaOutcome::Allowed);

    let mut auth = AuthFacade::minimal();
    auth.quota = Box::new(spy_quota);
    let auth = Arc::new(auth);

    let mut policy = PolicyConfig {
        mappings: Mappings {
            root_fs: root_path.display().to_string(),
            ..Mappings::default()
        },
        ..PolicyConfig::default()
    };
    policy.whitelists.domains.push("example.com".into());

    let outbox = Arc::new(InMemoryOutboxStore::default());

    let invoker = InvokerImpl::new(registry_dyn, auth, Sandbox::minimal(), policy.clone())
        .with_outbox(outbox.clone());

    let call = ToolCall {
        tool_id: ToolId("net.http.get".into()),
        call_id: Id("call-quota".into()),
        tenant: tenant.clone(),
        actor: subject(&tenant),
        origin: ToolOrigin::Api,
        args: serde_json::json!({ "url": "https://example.com/path" }),
        consent: None,
        idempotency_key: None,
    };

    let pf = invoker.preflight(&call).await.expect("preflight");
    assert!(pf.allow);

    {
        let records = records_handle.lock().expect("quota records snapshot");
        assert_eq!(records.len(), 1);
        let (key, cost) = &records[0];
        assert_eq!(cost, &1);
        assert_eq!(key.tenant, tenant);
        assert_eq!(key.action, soulbase_auth::prelude::Action::Invoke);
    }

    let request = InvokeRequest {
        spec: pf.spec.clone().expect("spec"),
        call: call.clone(),
        profile_hash: pf.profile_hash.clone().expect("profile hash"),
        obligations: pf.obligations.clone(),
        planned_ops: pf.planned_ops.clone(),
    };

    let result = invoker.invoke(request).await.expect("invoke");
    assert_eq!(result.status, InvokeStatus::Ok);
    assert!(result.output.is_some());

    let leased = outbox
        .lease_batch(
            &tenant,
            "worker-1",
            chrono::Utc::now().timestamp_millis(),
            10_000,
            5,
            false,
        )
        .await
        .expect("lease outbox");
    assert_eq!(leased.len(), 1);
    let message = &leased[0];
    assert_eq!(message.channel, "tool.invoke");
    assert_eq!(
        message.payload.get("call_id").and_then(|v| v.as_str()),
        Some("call-quota")
    );
    assert_eq!(
        message
            .payload
            .get("tool_id")
            .and_then(|v| v.as_str())
            .unwrap(),
        "net.http.get"
    );
    assert_eq!(
        message
            .payload
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap(),
        "ok"
    );
}
