use crate::errors::SandboxError;
use crate::model::{ExecOp, ExecResult, Profile};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use serde_json::json;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::time::timeout;

#[derive(Clone)]
pub struct ProcExecutor {
    pub max_output_bytes: usize,
}

impl Default for ProcExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl ProcExecutor {
    pub fn new() -> Self {
        Self {
            max_output_bytes: 64 * 1024,
        }
    }

    pub async fn execute(
        &self,
        profile: &Profile,
        op: &ExecOp,
    ) -> Result<ExecResult, SandboxError> {
        match op {
            ExecOp::ProcSpawn {
                program,
                args,
                env,
                stdin_b64,
                timeout_ms,
            } => {
                self.spawn(
                    program,
                    args,
                    env,
                    stdin_b64.as_deref(),
                    *timeout_ms,
                    profile,
                )
                .await
            }
            _ => Err(SandboxError::permission("process operation not supported")),
        }
    }

    async fn spawn(
        &self,
        program: &str,
        args: &[String],
        env: &std::collections::HashMap<String, String>,
        stdin_b64: Option<&str>,
        timeout_ms: Option<u64>,
        profile: &Profile,
    ) -> Result<ExecResult, SandboxError> {
        let mut command = Command::new(program);
        command.args(args);
        command.stdout(std::process::Stdio::piped());
        command.stderr(std::process::Stdio::piped());

        if stdin_b64.is_some() {
            command.stdin(std::process::Stdio::piped());
        }

        for (key, value) in env {
            command.env(key, value);
        }

        let mut child = command
            .spawn()
            .map_err(|err| SandboxError::internal(&format!("process spawn: {err}")))?;

        if let Some(b64) = stdin_b64 {
            if let Some(mut stdin) = child.stdin.take() {
                let data = B64
                    .decode(b64)
                    .map_err(|err| SandboxError::schema(&format!("process stdin decode: {err}")))?;
                stdin
                    .write_all(&data)
                    .await
                    .map_err(|err| SandboxError::internal(&format!("process stdin: {err}")))?;
            }
        }

        let stdout_handle = child.stdout.take();
        let stderr_handle = child.stderr.take();

        let stdout_task = tokio::spawn(async move {
            let mut buf = Vec::new();
            if let Some(mut reader) = stdout_handle.map(tokio::io::BufReader::new) {
                reader.read_to_end(&mut buf).await?;
            }
            Ok::<Vec<u8>, std::io::Error>(buf)
        });

        let stderr_task = tokio::spawn(async move {
            let mut buf = Vec::new();
            if let Some(mut reader) = stderr_handle.map(tokio::io::BufReader::new) {
                reader.read_to_end(&mut buf).await?;
            }
            Ok::<Vec<u8>, std::io::Error>(buf)
        });

        let status = if let Some(ms) = timeout_ms {
            match timeout(Duration::from_millis(ms), child.wait()).await {
                Ok(result) => {
                    result.map_err(|err| SandboxError::internal(&format!("process wait: {err}")))?
                }
                Err(_) => {
                    let _ = child.kill().await;
                    stdout_task.abort();
                    stderr_task.abort();
                    return Ok(ExecResult::failure(json!({
                        "tool": profile.tool_name,
                        "program": program,
                        "timed_out": true,
                        "timeout_ms": ms,
                    })));
                }
            }
        } else {
            child
                .wait()
                .await
                .map_err(|err| SandboxError::internal(&format!("process wait: {err}")))?
        };

        let stdout_owned = stdout_task
            .await
            .map_err(|err| SandboxError::internal(&format!("process stdout join: {err}")))?
            .map_err(|err| SandboxError::internal(&format!("process stdout read: {err}")))?;
        let stderr_owned = stderr_task
            .await
            .map_err(|err| SandboxError::internal(&format!("process stderr join: {err}")))?
            .map_err(|err| SandboxError::internal(&format!("process stderr read: {err}")))?;

        let stdout = truncate(&stdout_owned, self.max_output_bytes);
        let stderr = truncate(&stderr_owned, self.max_output_bytes);

        let out = json!({
            "tool": profile.tool_name,
            "program": program,
            "args": args,
            "exit_code": status.code(),
            "success": status.success(),
            "stdout_b64": B64.encode(stdout.data),
            "stdout_len": stdout.len,
            "stdout_truncated": stdout.truncated,
            "stderr_b64": B64.encode(stderr.data),
            "stderr_len": stderr.len,
            "stderr_truncated": stderr.truncated,
        });

        if status.success() {
            Ok(ExecResult::success(out))
        } else {
            Ok(ExecResult::failure(out))
        }
    }
}

struct Truncated<'a> {
    data: &'a [u8],
    truncated: bool,
    len: usize,
}

fn truncate<'a>(input: &'a [u8], limit: usize) -> Truncated<'a> {
    if input.len() > limit {
        Truncated {
            data: &input[..limit],
            truncated: true,
            len: input.len(),
        }
    } else {
        Truncated {
            data: input,
            truncated: false,
            len: input.len(),
        }
    }
}

#[cfg(all(test, feature = "exec-proc", unix))]
mod tests {
    use super::ProcExecutor;
    use crate::config::{Mappings, PolicyConfig};
    use crate::model::{Budget, Capability, ExecOp, Profile, SafetyClass, SideEffect};
    use base64::engine::general_purpose::STANDARD as B64;
    use base64::Engine as _;
    use chrono::Utc;
    use soulbase_types::prelude::{Id, TenantId};
    use std::collections::HashMap;

    fn profile(cap: Capability) -> Profile {
        let policy = PolicyConfig {
            mappings: Mappings {
                root_fs: ".".into(),
                tmp_dir: std::env::temp_dir().display().to_string(),
            },
            ..Default::default()
        };

        Profile {
            tenant: TenantId("tenant".into()),
            subject_id: Id("subject".into()),
            tool_name: "tool".into(),
            call_id: Id("call".into()),
            capabilities: vec![cap],
            policy,
            expires_at: Utc::now().timestamp_millis() + 60_000,
            budget: Budget::default(),
            safety_class: SafetyClass::Low,
            side_effect: SideEffect::Execute,
            manifest_name: "tool".into(),
            decision_key_fingerprint: "fp".into(),
        }
    }

    #[tokio::test]
    async fn proc_executor_runs_program() {
        let capability = Capability::ProcRun {
            program: "/bin/echo".into(),
            args: vec!["hello".into()],
            env_keys: vec![],
        };
        let profile = profile(capability);
        let executor = ProcExecutor::new();
        let op = ExecOp::ProcSpawn {
            program: "/bin/echo".into(),
            args: vec!["hello".into()],
            env: HashMap::new(),
            stdin_b64: None,
            timeout_ms: Some(5_000),
        };

        let result = executor.execute(&profile, &op).await.expect("proc execute");
        assert!(result.ok);
        let stdout_b64 = result
            .out
            .get("stdout_b64")
            .and_then(|v| v.as_str())
            .expect("stdout");
        let stdout = B64.decode(stdout_b64).expect("decode");
        assert!(stdout.starts_with(b"hello"));
    }
}
