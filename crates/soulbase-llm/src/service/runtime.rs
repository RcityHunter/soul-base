use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use soulbase_config::prelude::{
    bootstrap_catalog, validator_as_trait, ConfigSnapshot, InfraNamespace, LlmNamespace, Loader,
    NamespaceId,
};
use soulbase_infra::config as infra_cfg;

#[derive(Clone)]
pub struct RuntimeConfig {
    pub snapshot: ConfigSnapshot,
    pub llm: LlmNamespace,
    pub infra: InfraNamespace,
}

pub async fn load_runtime_config() -> Result<RuntimeConfig> {
    let handles = bootstrap_catalog()
        .await
        .context("bootstrap configuration catalog")?;

    let mut builder = Loader::builder()
        .with_registry(handles.registry.clone())
        .with_validator(validator_as_trait(&handles.validator));

    if let Some(paths) = config_paths_from_env() {
        builder = builder.add_file_sources(paths);
    }

    builder = builder.add_env_source("LLM", "__");

    let loader = builder.build();
    let snapshot = loader
        .load_once()
        .await
        .context("load LLM configuration snapshot")?;

    let llm = decode_namespace::<LlmNamespace>(&snapshot, "llm").context("decode llm namespace")?;

    let infra = infra_cfg::load(&snapshot)
        .map_err(|err| anyhow!(err))
        .context("decode infra namespace")?;

    Ok(RuntimeConfig {
        snapshot,
        llm,
        infra,
    })
}

fn config_paths_from_env() -> Option<Vec<PathBuf>> {
    let raw = std::env::var("LLM_CONFIG_PATHS")
        .or_else(|_| std::env::var("LLM_CONFIG_FILE"))
        .ok()?;
    let paths: Vec<PathBuf> = raw
        .split([';', ','])
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .collect();
    if paths.is_empty() {
        None
    } else {
        Some(paths)
    }
}

fn decode_namespace<T>(snapshot: &ConfigSnapshot, ns: &str) -> Result<T>
where
    T: serde::de::DeserializeOwned + Default,
{
    let value = snapshot.ns(&NamespaceId::new(ns));
    if value.is_null() {
        return Ok(T::default());
    }
    serde_json::from_value(value).map_err(|err| anyhow!(err))
}
