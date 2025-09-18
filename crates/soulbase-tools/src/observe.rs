use crate::manifest::ToolId;
use std::collections::BTreeMap;

pub fn labels(tenant: &str, tool: &ToolId, code: Option<&str>) -> BTreeMap<&'static str, String> {
    let mut map = BTreeMap::new();
    map.insert("tenant", tenant.to_string());
    map.insert("tool_id", tool.0.clone());
    if let Some(c) = code {
        map.insert("code", c.to_string());
    }
    map
}
