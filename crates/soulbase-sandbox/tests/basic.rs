use soulbase_sandbox::config::{Mappings, PolicyConfig, Whitelists};
use soulbase_sandbox::model::capability_fingerprint;
use soulbase_sandbox::model::{Budget, Capability, SafetyClass, SideEffect};
use soulbase_sandbox::prelude::*;
use soulbase_sandbox::{
    budget::MemoryBudget, evidence::MemoryEvidence, exec::Sandbox, guard::PolicyGuardDefault,
    model::ExecOp, model::Grant, model::ToolManifestLite, profile::ProfileBuilderDefault,
};
use soulbase_types::prelude::*;
use std::collections::HashMap;
use tempfile::tempdir;

fn grant_for_fs_read(root: &str) -> Grant {
    let capabilities = vec![Capability::FsRead {
        path: root.to_string(),
    }];
    Grant {
        tenant: TenantId("tenantA".into()),
        subject_id: Id("user_1".into()),
        tool_name: "fs_reader".into(),
        call_id: Id("call_fs".into()),
        capabilities: capabilities.clone(),
        expires_at: chrono::Utc::now().timestamp_millis() + 60_000,
        budget: Budget {
            calls: 10,
            bytes_in: 4 * 1024,
            bytes_out: i64::MAX,
            duration_ms: 5_000,
        },
        decision_key_fingerprint: format!("{:016x}", capability_fingerprint(&capabilities)),
    }
}

fn manifest_for_fs_read(root: &str) -> ToolManifestLite {
    ToolManifestLite {
        name: "fs_reader".into(),
        permissions: vec![Capability::FsRead {
            path: root.to_string(),
        }],
        safety_class: SafetyClass::Low,
        side_effect: SideEffect::Read,
    }
}

fn policy_for_root(root: &str) -> PolicyConfig {
    PolicyConfig {
        mappings: Mappings {
            root_fs: root.to_string(),
            tmp_dir: std::env::temp_dir().display().to_string(),
        },
        whitelists: Whitelists::default(),
    }
}

#[tokio::test]
async fn fs_read_allows_and_evidence_recorded() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("hello.txt");
    tokio::fs::write(&file_path, b"hello sandbox")
        .await
        .unwrap();
    let root = dir.path().display().to_string();

    let grant = grant_for_fs_read(&root);
    let manifest = manifest_for_fs_read(&root);
    let policy = policy_for_root(&root);

    let builder = ProfileBuilderDefault;
    let profile = builder
        .build(&grant, &manifest, &policy)
        .await
        .expect("profile");

    let guard = PolicyGuardDefault;
    guard
        .validate(
            &profile,
            &ExecOp::FsRead {
                path: "hello.txt".into(),
                offset: None,
                len: None,
            },
        )
        .await
        .expect("guard");

    let sandbox = Sandbox::minimal();
    let evidence = MemoryEvidence::new();
    let budget = MemoryBudget::new(grant.budget.clone());
    let env_id = Id("env_1".into());

    let result = sandbox
        .run(
            &profile,
            &env_id,
            &evidence,
            &budget,
            ExecOp::FsRead {
                path: "hello.txt".into(),
                offset: None,
                len: None,
            },
        )
        .await
        .expect("exec ok");

    assert!(result.ok);
    assert!(result.out["size"].as_u64().unwrap() > 0);
    assert_eq!(evidence.begins.lock().len(), 1);
    assert_eq!(evidence.ends.lock().len(), 1);
}

#[tokio::test]
async fn fs_read_path_escape_denied() {
    let dir = tempdir().unwrap();
    let root = dir.path().display().to_string();
    let grant = grant_for_fs_read(&root);
    let manifest = manifest_for_fs_read(&root);
    let policy = policy_for_root(&root);

    let profile = ProfileBuilderDefault
        .build(&grant, &manifest, &policy)
        .await
        .unwrap();
    let guard = PolicyGuardDefault;
    let op = ExecOp::FsRead {
        path: "../../etc/passwd".into(),
        offset: None,
        len: Some(4),
    };
    let err = guard.validate(&profile, &op).await.expect_err("denied");
    let eo = err.into_inner();
    assert_eq!(eo.code.0, "SANDBOX.PERMISSION_DENY");
}

#[tokio::test]
async fn budget_calls_exceeded() {
    let dir = tempdir().unwrap();
    let root = dir.path().display().to_string();
    let mut grant = grant_for_fs_read(&root);
    grant.budget.calls = 0;
    let manifest = manifest_for_fs_read(&root);
    let policy = policy_for_root(&root);
    let profile = ProfileBuilderDefault
        .build(&grant, &manifest, &policy)
        .await
        .unwrap();
    let sandbox = Sandbox::minimal();
    let evidence = MemoryEvidence::new();
    let budget = MemoryBudget::new(grant.budget.clone());
    let env_id = Id("env_2".into());
    let op = ExecOp::FsRead {
        path: "nonexistent.txt".into(),
        offset: None,
        len: Some(1),
    };
    let err = sandbox
        .run(&profile, &env_id, &evidence, &budget, op)
        .await
        .expect_err("budget exceed");
    let eo = err.into_inner();
    assert_eq!(eo.code.0, "QUOTA.BUDGET_EXCEEDED");
}

#[tokio::test]
async fn net_whitelist_denied() {
    let grant = Grant {
        tenant: TenantId("tenantA".into()),
        subject_id: Id("user_1".into()),
        tool_name: "net_fetch".into(),
        call_id: Id("call_net".into()),
        capabilities: vec![Capability::NetHttp {
            host: "example.com".into(),
            port: None,
            scheme: Some("https".into()),
            methods: vec!["GET".into()],
        }],
        expires_at: chrono::Utc::now().timestamp_millis() + 60_000,
        budget: Budget::default(),
        decision_key_fingerprint: "dk2".into(),
    };
    let manifest = ToolManifestLite {
        name: "net_fetch".into(),
        permissions: grant.capabilities.clone(),
        safety_class: SafetyClass::Medium,
        side_effect: SideEffect::Network,
    };
    let mut policy = PolicyConfig::default();
    policy.whitelists.domains = vec!["example.com".into()];
    let profile = ProfileBuilderDefault
        .build(&grant, &manifest, &policy)
        .await
        .unwrap();
    let guard = PolicyGuardDefault;
    let op = ExecOp::NetHttp {
        method: "GET".into(),
        url: "https://blocked.test/".into(),
        headers: HashMap::new(),
        body_b64: None,
    };
    let err = guard.validate(&profile, &op).await.expect_err("blocked");
    let eo = err.into_inner();
    assert_eq!(eo.code.0, "SANDBOX.PERMISSION_DENY");
}
