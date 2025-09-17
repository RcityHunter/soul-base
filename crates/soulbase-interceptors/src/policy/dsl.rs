use super::model::{MatchCond, RoutePolicySpec};

#[derive(Clone)]
pub struct RoutePolicy {
    rules: Vec<RoutePolicySpec>,
}

impl RoutePolicy {
    pub fn new(rules: Vec<RoutePolicySpec>) -> Self {
        Self { rules }
    }

    pub fn match_http(&self, method: &str, path: &str) -> Option<&RoutePolicySpec> {
        self.rules.iter().find(|r| match &r.when {
            MatchCond::Http { method: m, path_prefix } => {
                m.eq_ignore_ascii_case(method) && path.starts_with(path_prefix)
            }
        })
    }
}
