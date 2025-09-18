use std::collections::BTreeMap;

pub fn labels(provider: &str, model: &str, code: Option<&str>) -> BTreeMap<&'static str, String> {
    let mut m = BTreeMap::new();
    m.insert("provider", provider.to_string());
    m.insert("model", model.to_string());
    if let Some(c) = code {
        m.insert("code", c.to_string());
    }
    m
}
