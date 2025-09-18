use crate::model::{Action, Decision, ResourceUrn};
use std::collections::BTreeMap;

pub fn labels(
    tenant: &str,
    resource: &ResourceUrn,
    action: &Action,
    decision: &Decision,
) -> BTreeMap<&'static str, String> {
    let mut map = BTreeMap::new();
    map.insert("tenant", tenant.to_string());
    map.insert("resource", resource.0.clone());
    map.insert("action", format!("{:?}", action));
    map.insert("allow", decision.allow.to_string());
    map
}
