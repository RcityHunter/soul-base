use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct PolicyConfig {
    pub mappings: Mappings,
    pub whitelists: Whitelists,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Mappings {
    pub root_fs: String,
    pub tmp_dir: String,
}

impl Default for Mappings {
    fn default() -> Self {
        Self {
            root_fs: ".".into(),
            tmp_dir: std::env::temp_dir().display().to_string(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct Whitelists {
    #[serde(default)]
    pub domains: Vec<String>,
}
