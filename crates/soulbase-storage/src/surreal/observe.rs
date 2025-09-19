use std::collections::BTreeMap;

pub fn labels(op: &str, code: Option<&str>) -> BTreeMap<&'static str, String> {
    let mut map = BTreeMap::new();
    map.insert("backend", "surreal".into());
    map.insert("operation", op.into());
    if let Some(c) = code {
        map.insert("code", c.into());
    }
    map
}
