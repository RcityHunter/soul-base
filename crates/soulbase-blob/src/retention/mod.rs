use crate::{errors::BlobError, r#trait::RetentionExec};
use async_trait::async_trait;
use chrono::Duration;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RetentionRule {
    pub bucket: String,
    pub class: RetentionClass,
    pub selector: Selector,
    pub ttl_days: u32,
    pub archive_to: Option<String>,
    pub version_hash: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RetentionClass {
    Hot,
    Warm,
    Cold,
    Frozen,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Selector {
    pub tenant: String,
    pub namespace: Option<String>,
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
}

#[derive(Clone, Debug)]
pub struct FsRetentionExec {
    pub root: PathBuf,
}

impl FsRetentionExec {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
}

#[async_trait]
impl RetentionExec for FsRetentionExec {
    async fn apply_rule(&self, rule: &RetentionRule) -> Result<u64, BlobError> {
        let bucket_dir = self.root.join(&rule.bucket);
        if !bucket_dir.exists() {
            return Ok(0);
        }

        let prefix = if let Some(ns) = &rule.selector.namespace {
            format!("{}/{}", rule.selector.tenant, ns)
        } else {
            rule.selector.tenant.clone()
        };

        let cutoff = if rule.ttl_days == 0 {
            Duration::zero()
        } else {
            Duration::days(rule.ttl_days as i64)
        };

        let mut removed = 0u64;
        let mut stack = vec![bucket_dir.clone()];
        while let Some(dir) = stack.pop() {
            for entry in fs::read_dir(&dir)
                .map_err(|err| BlobError::provider_unavailable(&format!("read_dir: {err}")))?
            {
                let entry = entry
                    .map_err(|err| BlobError::provider_unavailable(&format!("dir_entry: {err}")))?;
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                if path.extension().and_then(|ext| ext.to_str()) == Some("json") {
                    continue;
                }
                let rel = match path.strip_prefix(&bucket_dir) {
                    Ok(rel) => rel,
                    Err(_) => continue,
                };
                let rel_str = rel.to_string_lossy().replace('\\', "/");
                if !rel_str.starts_with(&prefix) {
                    continue;
                }

                let metadata = fs::metadata(&path)
                    .map_err(|err| BlobError::provider_unavailable(&format!("stat: {err}")))?;
                let should_remove = if rule.ttl_days == 0 {
                    true
                } else {
                    let age = metadata
                        .modified()
                        .ok()
                        .and_then(|ts| ts.elapsed().ok())
                        .and_then(|elapsed| Duration::from_std(elapsed).ok())
                        .unwrap_or_else(Duration::zero);
                    age >= cutoff
                };

                if should_remove {
                    fs::remove_file(&path).map_err(|err| {
                        BlobError::provider_unavailable(&format!("remove: {err}"))
                    })?;
                    let meta_path = path.with_extension("meta.json");
                    let _ = fs::remove_file(meta_path);
                    removed += 1;
                }
            }
        }

        Ok(removed)
    }
}
