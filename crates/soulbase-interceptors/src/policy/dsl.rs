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
        self.rules.iter().find(|rule| match &rule.when {
            MatchCond::Http {
                method: expected,
                path_glob,
            } => expected.eq_ignore_ascii_case(method) && path_matches(path_glob, path),
        })
    }
}

fn path_matches(glob: &str, path: &str) -> bool {
    if let Some(stripped) = glob.strip_suffix('*') {
        path.starts_with(stripped)
    } else {
        glob == path
    }
}
