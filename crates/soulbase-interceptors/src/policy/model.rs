use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RoutePolicySpec {
    pub when: MatchCond,
    pub bind: RouteBindingSpec,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum MatchCond {
    Http { method: String, path_glob: String },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RouteBindingSpec {
    pub resource: String,
    pub action: String,
    #[serde(default)]
    pub attrs_template: Option<serde_json::Value>,
    #[serde(default)]
    pub attrs_from_body: bool,
}
