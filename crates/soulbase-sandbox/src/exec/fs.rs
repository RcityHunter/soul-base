use crate::errors::SandboxError;
use crate::guard::resolve_fs_path;
use crate::model::{ExecOp, ExecResult, Profile};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use serde_json::json;
use tokio::fs;

#[derive(Clone, Default)]
pub struct FsExecutor;

impl FsExecutor {
    pub fn new() -> Self {
        Self
    }

    pub async fn execute(
        &self,
        profile: &Profile,
        op: &ExecOp,
    ) -> Result<ExecResult, SandboxError> {
        match op {
            ExecOp::FsRead { path, offset, len } => self.read(profile, path, *offset, *len).await,
            ExecOp::FsList { path } => self.list(profile, path).await,
            ExecOp::FsWrite { path, contents_b64 } => self.write(profile, path, contents_b64).await,
            _ => Err(SandboxError::permission(
                "filesystem operation not supported",
            )),
        }
    }

    async fn read(
        &self,
        profile: &Profile,
        rel_path: &str,
        offset: Option<u64>,
        len: Option<u64>,
    ) -> Result<ExecResult, SandboxError> {
        let path = resolve_fs_path(&profile.policy, rel_path)?;
        let data = fs::read(&path)
            .await
            .map_err(|e| SandboxError::internal(&format!("fs read: {e}")))?;
        let start = offset.unwrap_or(0) as usize;
        let end = len.map(|l| start + l as usize).unwrap_or(data.len());
        let slice_end = end.min(data.len());
        let slice = if start >= slice_end {
            &data[0..0]
        } else {
            &data[start..slice_end]
        };
        let preview_len = slice.len().min(128);
        let preview = String::from_utf8_lossy(&slice[..preview_len]).to_string();
        let encoded = B64.encode(slice);
        let out = json!({
            "size": slice.len() as u64,
            "preview": preview,
            "data_b64": encoded,
            "path": path.to_string_lossy(),
        });
        Ok(ExecResult::success(out))
    }

    async fn list(&self, profile: &Profile, rel_path: &str) -> Result<ExecResult, SandboxError> {
        let path = resolve_fs_path(&profile.policy, rel_path)?;
        let mut entries = Vec::new();
        let mut read_dir = fs::read_dir(&path)
            .await
            .map_err(|e| SandboxError::internal(&format!("fs list: {e}")))?;
        while let Some(entry) = read_dir
            .next_entry()
            .await
            .map_err(|e| SandboxError::internal(&format!("fs list: {e}")))?
        {
            entries.push(entry.file_name().to_string_lossy().to_string());
        }
        let out = json!({
            "entries": entries,
            "path": path.to_string_lossy(),
        });
        Ok(ExecResult::success(out))
    }

    async fn write(
        &self,
        profile: &Profile,
        rel_path: &str,
        contents_b64: &str,
    ) -> Result<ExecResult, SandboxError> {
        let path = resolve_fs_path(&profile.policy, rel_path)?;
        let bytes = B64
            .decode(contents_b64)
            .map_err(|err| SandboxError::schema(&format!("fs write decode: {err}")))?;

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|err| SandboxError::internal(&format!("fs write mkdir: {err}")))?;
        }

        fs::write(&path, &bytes)
            .await
            .map_err(|err| SandboxError::internal(&format!("fs write: {err}")))?;

        let out = json!({
            "path": path.to_string_lossy(),
            "bytes_written": bytes.len(),
        });
        Ok(ExecResult::success(out))
    }
}

#[cfg(test)]
mod tests {
    use super::{FsExecutor, B64};
    use crate::config::{Mappings, PolicyConfig};
    use crate::model::{Budget, Capability, ExecOp, Profile, SafetyClass, SideEffect};
    use base64::Engine as _;
    use chrono::Utc;
    use soulbase_types::prelude::{Id, TenantId};
    use tempfile::tempdir;

    fn profile(root: &std::path::Path, capabilities: Vec<Capability>) -> Profile {
        let policy = PolicyConfig {
            mappings: Mappings {
                root_fs: root.display().to_string(),
                tmp_dir: root.join("tmp").display().to_string(),
            },
            ..Default::default()
        };

        Profile {
            tenant: TenantId("tenant".into()),
            subject_id: Id("subject".into()),
            tool_name: "tool".into(),
            call_id: Id("call".into()),
            capabilities,
            policy,
            expires_at: Utc::now().timestamp_millis() + 60_000,
            budget: Budget::default(),
            safety_class: SafetyClass::Low,
            side_effect: SideEffect::Write,
            manifest_name: "tool".into(),
            decision_key_fingerprint: "fp".into(),
        }
    }

    #[tokio::test]
    async fn fs_write_and_read_roundtrip() {
        let dir = tempdir().expect("tmp dir");
        let caps = vec![
            Capability::FsWrite { path: ".".into() },
            Capability::FsRead { path: ".".into() },
        ];
        let profile = profile(dir.path(), caps);
        let executor = FsExecutor::new();

        let contents = B64.encode(b"hello world");
        let write_op = ExecOp::FsWrite {
            path: "file.txt".into(),
            contents_b64: contents,
        };
        let write_res = executor
            .execute(&profile, &write_op)
            .await
            .expect("write success");
        assert!(write_res.ok);

        let read_op = ExecOp::FsRead {
            path: "file.txt".into(),
            offset: None,
            len: None,
        };
        let read_res = executor
            .execute(&profile, &read_op)
            .await
            .expect("read success");
        assert!(read_res.ok);
        let data_b64 = read_res
            .out
            .get("data_b64")
            .and_then(|v| v.as_str())
            .expect("data present");
        let data = B64.decode(data_b64).expect("decode");
        assert_eq!(data, b"hello world");
    }

    #[tokio::test]
    async fn fs_list_returns_entries() {
        let dir = tempdir().expect("tmp dir");
        let file_path = dir.path().join("a.txt");
        tokio::fs::write(&file_path, b"data")
            .await
            .expect("prep file");

        let profile = profile(dir.path(), vec![Capability::FsList { path: ".".into() }]);
        let executor = FsExecutor::new();
        let op = ExecOp::FsList { path: ".".into() };
        let res = executor.execute(&profile, &op).await.expect("list success");
        assert!(res.ok);
        let entries = res
            .out
            .get("entries")
            .and_then(|v| v.as_array())
            .expect("entries array");
        assert!(entries.iter().any(|v| v.as_str() == Some("a.txt")));
    }
}
