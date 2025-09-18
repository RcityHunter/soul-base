use crate::config::PolicyConfig;
use crate::errors::SandboxError;
use crate::model::{Capability, ExecOp, Profile};
use async_trait::async_trait;
use path_clean::PathClean;
use std::path::{Path, PathBuf};
use url::Url;

#[async_trait]
pub trait PolicyGuard: Send + Sync {
    async fn validate(&self, profile: &Profile, op: &ExecOp) -> Result<(), SandboxError>;
}

pub struct PolicyGuardDefault;

#[async_trait]
impl PolicyGuard for PolicyGuardDefault {
    async fn validate(&self, profile: &Profile, op: &ExecOp) -> Result<(), SandboxError> {
        match op {
            ExecOp::FsRead { path, .. } => validate_fs(profile, path, CapabilityKind::Read),
            ExecOp::FsWrite { path, .. } => validate_fs(profile, path, CapabilityKind::Write),
            ExecOp::FsList { path } => validate_fs(profile, path, CapabilityKind::List),
            ExecOp::NetHttp { method, url, .. } => validate_net(profile, method, url),
            ExecOp::TmpAlloc { .. } => validate_tmp(profile),
        }
    }
}

enum CapabilityKind {
    Read,
    Write,
    List,
}

fn validate_fs(
    profile: &Profile,
    rel_path: &str,
    kind: CapabilityKind,
) -> Result<(), SandboxError> {
    let resolved = resolve_fs_path(&profile.policy, rel_path)?;
    let allowed = profile.capabilities.iter().any(|cap| match (cap, &kind) {
        (Capability::FsRead { path }, CapabilityKind::Read) => {
            path_allows(&profile.policy, path, &resolved)
        }
        (Capability::FsWrite { path }, CapabilityKind::Write) => {
            path_allows(&profile.policy, path, &resolved)
        }
        (Capability::FsList { path }, CapabilityKind::List) => {
            path_allows(&profile.policy, path, &resolved)
        }
        _ => false,
    });

    if !allowed {
        return Err(SandboxError::permission(
            "filesystem capability not granted",
        ));
    }
    Ok(())
}

fn validate_tmp(profile: &Profile) -> Result<(), SandboxError> {
    if profile
        .capabilities
        .iter()
        .any(|cap| matches!(cap, Capability::TmpUse))
    {
        Ok(())
    } else {
        Err(SandboxError::permission("tmp capability not granted"))
    }
}

fn validate_net(profile: &Profile, method: &str, url_str: &str) -> Result<(), SandboxError> {
    let url = Url::parse(url_str).map_err(|_| SandboxError::permission("invalid url"))?;
    let host = url
        .host_str()
        .ok_or_else(|| SandboxError::permission("missing host"))?;
    let scheme = url.scheme().to_string();
    let port = url.port();

    if !profile.policy.whitelists.domains.is_empty()
        && !profile
            .policy
            .whitelists
            .domains
            .iter()
            .any(|d| host.ends_with(d))
    {
        return Err(SandboxError::permission("domain not whitelisted"));
    }

    let allowed = profile.capabilities.iter().any(|cap| match cap {
        Capability::NetHttp {
            host: cap_host,
            port: cap_port,
            scheme: cap_scheme,
            methods,
        } => {
            host.ends_with(cap_host)
                && cap_port.map_or(true, |p| Some(p) == port)
                && cap_scheme
                    .as_ref()
                    .map_or(true, |s| s.eq_ignore_ascii_case(&scheme))
                && methods.iter().any(|m| m.eq_ignore_ascii_case(method))
        }
        _ => false,
    });

    if !allowed {
        return Err(SandboxError::permission("network capability not granted"));
    }

    Ok(())
}

fn path_allows(policy: &PolicyConfig, cap_path: &str, resolved: &Path) -> bool {
    match resolve_capability_path(policy, cap_path) {
        Ok(base) => resolved.starts_with(base),
        Err(_) => false,
    }
}

fn resolve_capability_path(policy: &PolicyConfig, cap_path: &str) -> Result<PathBuf, SandboxError> {
    let path = Path::new(cap_path);
    let joined = if path.is_absolute() {
        path.to_path_buf()
    } else {
        Path::new(&policy.mappings.root_fs).join(path)
    };
    canonicalize_and_check(&joined, &policy.mappings.root_fs)
}

pub fn resolve_fs_path(policy: &PolicyConfig, rel: &str) -> Result<PathBuf, SandboxError> {
    let joined = Path::new(&policy.mappings.root_fs).join(rel);
    canonicalize_and_check(&joined, &policy.mappings.root_fs)
}

fn canonicalize_and_check(path: &Path, root: &str) -> Result<PathBuf, SandboxError> {
    let cleaned = path.clean();
    let root_clean = Path::new(root).clean();
    if !cleaned.starts_with(&root_clean) {
        return Err(SandboxError::permission("path escape detected"));
    }
    Ok(cleaned)
}
