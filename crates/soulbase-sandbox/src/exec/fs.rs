use crate::errors::SandboxError;
use crate::guard::resolve_fs_path;
use crate::model::{ExecOp, ExecResult, Profile};
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
        let out = json!({
            "size": slice.len() as u64,
            "preview": preview,
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
}
