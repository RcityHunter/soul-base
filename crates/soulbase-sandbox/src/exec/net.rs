use crate::errors::SandboxError;
use crate::model::{ExecOp, ExecResult, Profile};
use serde_json::json;
use url::Url;

#[derive(Clone, Default)]
pub struct NetExecutor {
    pub simulate_latency_ms: u64,
}

impl NetExecutor {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn execute(
        &self,
        profile: &Profile,
        op: &ExecOp,
    ) -> Result<ExecResult, SandboxError> {
        match op {
            ExecOp::NetHttp { method, url, .. } => self.http(profile, method, url).await,
            _ => Err(SandboxError::permission("network operation not supported")),
        }
    }

    async fn http(
        &self,
        profile: &Profile,
        method: &str,
        url: &str,
    ) -> Result<ExecResult, SandboxError> {
        let parsed = Url::parse(url).map_err(|_| SandboxError::permission("invalid url"))?;
        let host = parsed.host_str().unwrap_or_default().to_string();
        let out = json!({
            "simulated": true,
            "host": host,
            "method": method.to_uppercase(),
            "tool": profile.tool_name,
        });
        Ok(ExecResult::success(out))
    }
}
