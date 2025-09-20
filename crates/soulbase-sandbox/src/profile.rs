use crate::config::PolicyConfig;
use crate::errors::SandboxError;
use crate::model::{Capability, Grant, Profile, ToolManifestLite};
use async_trait::async_trait;
use chrono::Utc;

#[async_trait]
pub trait ProfileBuilder: Send + Sync {
    async fn build(
        &self,
        grant: &Grant,
        manifest: &ToolManifestLite,
        policy: &PolicyConfig,
    ) -> Result<Profile, SandboxError>;
}

pub struct ProfileBuilderDefault;

#[async_trait]
impl ProfileBuilder for ProfileBuilderDefault {
    async fn build(
        &self,
        grant: &Grant,
        manifest: &ToolManifestLite,
        policy: &PolicyConfig,
    ) -> Result<Profile, SandboxError> {
        if grant.tool_name != manifest.name {
            return Err(SandboxError::permission("tool name mismatch"));
        }

        if grant.expires_at < Utc::now().timestamp_millis() {
            return Err(SandboxError::expired());
        }

        let allowed: Vec<Capability> = manifest
            .permissions
            .iter()
            .filter(|cap| grant.capabilities.iter().any(|g| g == *cap))
            .cloned()
            .collect();

        if allowed.is_empty() {
            return Err(SandboxError::permission("no intersecting capabilities"));
        }

        Ok(Profile {
            tenant: grant.tenant.clone(),
            subject_id: grant.subject_id.clone(),
            tool_name: grant.tool_name.clone(),
            call_id: grant.call_id.clone(),
            capabilities: allowed,
            policy: policy.clone(),
            expires_at: grant.expires_at,
            budget: grant.budget.clone(),
            safety_class: manifest.safety_class,
            side_effect: manifest.side_effect,
            manifest_name: manifest.name.clone(),
            decision_key_fingerprint: grant.decision_key_fingerprint.clone(),
        })
    }
}
