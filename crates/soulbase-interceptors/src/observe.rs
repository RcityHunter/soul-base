use crate::context::RouteBinding;
use std::collections::BTreeMap;

pub fn labels(
    tenant: Option<&str>,
    route: Option<&RouteBinding>,
    code: Option<&str>,
) -> BTreeMap<&'static str, String> {
    let mut map = BTreeMap::new();
    if let Some(t) = tenant {
        map.insert("tenant", t.to_string());
    }
    if let Some(binding) = route {
        map.insert("resource", binding.resource.0.clone());
        map.insert("action", format!("{:?}", binding.action));
    }
    if let Some(c) = code {
        map.insert("code", c.to_string());
    }
    map
}
