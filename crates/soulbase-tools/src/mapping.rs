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
            ("tmp", "use") => {
                let size = args.get("size_bytes").and_then(|v| v.as_u64()).unwrap_or(0);
                ops.push(ExecOp::TmpAlloc { size_bytes: size });
            }
            _ => {}
        }
    }
    Ok(ops)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{
        CapabilityDecl, ConcurrencyKind, ConsentPolicy, IdempoKind, Limits, SafetyClass,
        SideEffect, ToolId,
    };
    use serde_json::json;

    fn schema_from_json(value: serde_json::Value) -> crate::manifest::SchemaDoc {
        serde_json::from_value(value).expect("schema")
    }

    fn manifest_with_caps(caps: Vec<CapabilityDecl>) -> ToolManifest {
        ToolManifest {
            id: ToolId("test.tool".into()),
            version: "1.0.0".into(),
            display_name: "Test Tool".into(),
            description: "Unit test tool".into(),
            tags: vec![],
            input_schema: schema_from_json(json!({"type": "object"})),
            output_schema: schema_from_json(json!({"type": "object"})),
            scopes: vec![],
            capabilities: caps,
            side_effect: SideEffect::None,
            safety_class: SafetyClass::Low,
            consent: ConsentPolicy {
                required: false,
                max_ttl_ms: None,
            },
            limits: Limits {
                timeout_ms: 10_000,
                max_bytes_in: 1_024,
                max_bytes_out: 1_024,
                max_files: 0,
                max_depth: 1,
                max_concurrency: 1,
            },
            idempotency: IdempoKind::None,
            concurrency: ConcurrencyKind::Parallel,
        }
    }

    #[test]
    fn maps_net_http_get() {
        let manifest = manifest_with_caps(vec![CapabilityDecl {
            domain: "net.http".into(),
            action: "get".into(),
            resource: "example.com".into(),
            attrs: json!({}),
        }]);
        let ops = plan_ops(&manifest, &json!({ "url": "https://example.com" })).unwrap();
        assert!(
            matches!(ops.as_slice(), [ExecOp::NetHttp { method, url, .. }] if method == "GET" && url == "https://example.com")
        );
    }

    #[test]
    fn fs_write_requires_contents() {
        let manifest = manifest_with_caps(vec![CapabilityDecl {
            domain: "fs".into(),
            action: "write".into(),
            resource: "/tmp".into(),
            attrs: json!({}),
        }]);
        let err = plan_ops(&manifest, &json!({ "path": "file.txt" })).unwrap_err();
        let message = format!("{}", err);
        assert!(message.contains("missing args.contents_b64"));
    }

    #[test]
    fn maps_tmp_use_to_alloc() {
        let manifest = manifest_with_caps(vec![CapabilityDecl {
            domain: "tmp".into(),
            action: "use".into(),
            resource: "".into(),
            attrs: json!({}),
        }]);
        let ops = plan_ops(&manifest, &json!({ "size_bytes": 2048 })).unwrap();
        match ops.as_slice() {
            [ExecOp::TmpAlloc { size_bytes }] => assert_eq!(*size_bytes, 2048),
            other => panic!("unexpected ops: {other:?}"),
        }
    }
}
