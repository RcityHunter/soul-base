use crate::errors::ToolError;
use crate::manifest::ToolManifest;
use soulbase_sandbox::prelude::ExecOp;
use std::collections::HashMap;

/// Plan sandbox ExecOps based on manifest capability declarations and call arguments.
pub fn plan_ops(
    manifest: &ToolManifest,
    args: &serde_json::Value,
) -> Result<Vec<ExecOp>, ToolError> {
    let mut ops = Vec::new();
    for cap in &manifest.capabilities {
        match (cap.domain.as_str(), cap.action.as_str()) {
            ("net.http", "get") => {
                let url = args
                    .get("url")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| ToolError::schema("missing args.url"))?;
                ops.push(ExecOp::NetHttp {
                    method: "GET".into(),
                    url: url.to_string(),
                    headers: HashMap::new(),
                    body_b64: None,
                });
            }
            ("fs", "read") => {
                let path = args
                    .get("path")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| ToolError::schema("missing args.path"))?;
                let len = args.get("len").and_then(|v| v.as_u64());
                ops.push(ExecOp::FsRead {
                    path: path.to_string(),
                    offset: None,
                    len,
                });
            }
            ("fs", "list") => {
                let path = args
                    .get("path")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| ToolError::schema("missing args.path"))?;
                ops.push(ExecOp::FsList {
                    path: path.to_string(),
                });
            }
            ("fs", "write") => {
                let path = args
                    .get("path")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| ToolError::schema("missing args.path"))?;
                let contents = args
                    .get("contents_b64")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| ToolError::schema("missing args.contents_b64"))?;
                ops.push(ExecOp::FsWrite {
                    path: path.to_string(),
                    contents_b64: contents.to_string(),
                });
            }
            _ => {}
        }
    }
    Ok(ops)
}
