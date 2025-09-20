use crate::errors::ToolError;
use crate::manifest::{CapabilityDecl, ToolManifest};
use crate::mapping::plan_ops;
use crate::registry::{AvailableSpec, ToolRegistry};
use ahash::AHasher;
use serde_json::json;
use soulbase_auth::{prelude::*, AuthFacade};
use soulbase_sandbox::prelude::{
    Budget, Capability, ExecOp, Grant, PolicyConfig, PolicyGuard, ProfileBuilder,
    ProfileBuilderDefault, SafetyClass as SandboxSafetyClass, SideEffect as SandboxSideEffect,
    ToolManifestLite,
};
use soulbase_types::prelude::*;
use std::hash::Hasher;

#[derive(Clone, Debug)]
pub enum ToolOrigin {
    Llm,
    Api,
    System,
}

#[derive(Clone, Debug)]
pub struct ToolCall {
    pub tool_id: super::manifest::ToolId,
    pub call_id: Id,
    pub tenant: TenantId,
    pub actor: Subject,
    pub origin: ToolOrigin,
    pub args: serde_json::Value,
    pub consent: Option<Consent>,
    pub idempotency_key: Option<String>,
}

#[derive(Clone, Debug)]
pub struct PreflightOutput {
    pub allow: bool,
    pub reason: Option<String>,
    pub profile_hash: Option<String>,
    pub obligations: Vec<Obligation>,
    pub budget_snapshot: serde_json::Value,
    pub spec: Option<AvailableSpec>,
    pub planned_ops: Vec<ExecOp>,
}

impl Default for PreflightOutput {
    fn default() -> Self {
        Self {
            allow: false,
            reason: None,
            profile_hash: None,
            obligations: Vec::new(),
            budget_snapshot: json!({}),
            spec: None,
            planned_ops: Vec::new(),
        }
    }
}

pub struct Preflight<'a> {
    pub registry: &'a dyn ToolRegistry,
    pub auth: &'a AuthFacade,
    pub policy: &'a PolicyConfig,
    pub guard: &'a dyn PolicyGuard,
}

impl Preflight<'_> {
    pub async fn run(&self, call: &ToolCall) -> Result<PreflightOutput, ToolError> {
        let spec_opt = self.registry.get(&call.tenant, &call.tool_id).await?;
        let spec = match spec_opt {
            Some(spec) => spec,
            None => {
                return Ok(PreflightOutput {
                    reason: Some("tool_not_registered".into()),
                    ..PreflightOutput::default()
                })
            }
        };

        spec.manifest.validate_input(&call.args)?;

        let auth_input =
            AuthnInput::BearerJwt(format!("{}@{}", call.actor.subject_id.0, call.tenant.0));
        let resource = ResourceUrn(format!("soul:tool:{}", spec.manifest.id.0));
        let decision = self
            .auth
            .authorize(
                auth_input,
                resource,
                Action::Invoke,
                json!({ "allow": true, "cost": 1 }),
                call.consent.clone(),
                None,
            )
            .await?;

        if !decision.allow {
            return Ok(PreflightOutput {
                reason: decision.reason.clone(),
                obligations: decision.obligations,
                spec: Some(spec),
                ..PreflightOutput::default()
            });
        }

        let ops = plan_ops(&spec.manifest, &call.args)?;
        let policy = compose_policy(self.policy.clone(), &spec.manifest);
        let grant = build_grant(&spec.manifest, call);
        let manifest_lite = manifest_to_lite(&spec.manifest);
        let profile = ProfileBuilderDefault
            .build(&grant, &manifest_lite, &policy)
            .await?;

        for op in &ops {
            self.guard.validate(&profile, op).await?;
        }

        let profile_hash = format!(
            "{:016x}",
            compute_profile_hash(&spec.manifest, call, &grant)
        );

        Ok(PreflightOutput {
            allow: true,
            reason: None,
            profile_hash: Some(profile_hash),
            obligations: decision.obligations,
            budget_snapshot: json!({
                "timeout_ms": spec.manifest.limits.timeout_ms,
                "max_bytes_in": spec.manifest.limits.max_bytes_in,
                "max_bytes_out": spec.manifest.limits.max_bytes_out,
            }),
            spec: Some(spec),
            planned_ops: ops,
        })
    }
}

pub(crate) fn compose_policy(mut base: PolicyConfig, manifest: &ToolManifest) -> PolicyConfig {
    for cap in &manifest.capabilities {
        if cap.domain == "net.http" && !base.whitelists.domains.contains(&cap.resource) {
            base.whitelists.domains.push(cap.resource.clone());
        }
    }
    base
}

pub(crate) fn build_grant(manifest: &ToolManifest, call: &ToolCall) -> Grant {
    Grant {
        tenant: call.tenant.clone(),
        subject_id: call.actor.subject_id.clone(),
        tool_name: manifest.id.0.clone(),
        call_id: call.call_id.clone(),
        capabilities: manifest
            .capabilities
            .iter()
            .map(capability_to_sandbox)
            .collect(),
        expires_at: chrono::Utc::now().timestamp_millis()
            + to_i64(manifest.consent.max_ttl_ms.unwrap_or(60_000)),
        budget: Budget {
            calls: to_i64(manifest.limits.max_concurrency as u64),
            bytes_in: to_i64(manifest.limits.max_bytes_in),
            bytes_out: to_i64(manifest.limits.max_bytes_out),
            duration_ms: to_i64(manifest.limits.timeout_ms),
        },
        decision_key_fingerprint: manifest.fingerprint().to_string(),
    }
}

pub(crate) fn manifest_to_lite(manifest: &ToolManifest) -> ToolManifestLite {
    ToolManifestLite {
        name: manifest.id.0.clone(),
        permissions: manifest
            .capabilities
            .iter()
            .map(capability_to_sandbox)
            .collect(),
        safety_class: match manifest.safety_class {
            crate::manifest::SafetyClass::Low => SandboxSafetyClass::Low,
            crate::manifest::SafetyClass::Medium => SandboxSafetyClass::Medium,
            crate::manifest::SafetyClass::High => SandboxSafetyClass::High,
        },
        side_effect: match manifest.side_effect {
            crate::manifest::SideEffect::None => SandboxSideEffect::Read,
            crate::manifest::SideEffect::Read => SandboxSideEffect::Read,
            crate::manifest::SideEffect::Write => SandboxSideEffect::Write,
            crate::manifest::SideEffect::Network => SandboxSideEffect::Network,
            crate::manifest::SideEffect::Filesystem => SandboxSideEffect::Read,
            crate::manifest::SideEffect::Browser => SandboxSideEffect::Read,
            crate::manifest::SideEffect::Process => SandboxSideEffect::Execute,
        },
    }
}

pub(crate) fn capability_to_sandbox(cap: &CapabilityDecl) -> Capability {
    match (cap.domain.as_str(), cap.action.as_str()) {
        ("fs", "read") => Capability::FsRead {
            path: cap.resource.clone(),
        },
        ("fs", "write") => Capability::FsWrite {
            path: cap.resource.clone(),
        },
        ("fs", "list") => Capability::FsList {
            path: cap.resource.clone(),
        },
        ("net.http", "get") => Capability::NetHttp {
            host: cap.resource.clone(),
            port: None,
            scheme: Some("https".into()),
            methods: vec!["GET".into(), "HEAD".into()],
        },
        _ => Capability::TmpUse,
    }
}

pub(crate) fn compute_profile_hash(manifest: &ToolManifest, call: &ToolCall, grant: &Grant) -> u64 {
    let mut hasher = AHasher::default();
    hasher.write_u64(manifest.fingerprint());
    hasher.write(call.call_id.0.as_bytes());
    hasher.write(call.actor.subject_id.0.as_bytes());
    hasher.write(call.tenant.0.as_bytes());
    hasher.write(grant.decision_key_fingerprint.as_bytes());
    hasher.finish()
}

pub(crate) fn to_i64(v: u64) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}
