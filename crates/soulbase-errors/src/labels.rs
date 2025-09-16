use crate::model::ErrorObj;
use std::collections::BTreeMap;

pub fn labels(err: &ErrorObj) -> BTreeMap<&'static str, String> {
    let mut map = BTreeMap::new();
    map.insert("code", err.code.0.to_string());
    map.insert("kind", err.kind.as_str().to_string());
    map.insert("retryable", err.retryable.as_str().to_string());
    map.insert("severity", err.severity.as_str().to_string());

    if let Some(value) = err.meta.get("provider") {
        map.insert("provider", value.to_string());
    }
    if let Some(value) = err.meta.get("tool") {
        map.insert("tool", value.to_string());
    }
    if let Some(value) = err.meta.get("tenant") {
        map.insert("tenant", value.to_string());
    }

    map
}
