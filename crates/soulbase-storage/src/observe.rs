use soulbase_types::prelude::TenantId;
use std::collections::BTreeMap;

pub fn labels(
    tenant: &TenantId,
    collection: &str,
    code: Option<&str>,
) -> BTreeMap<&'static str, String> {
    let mut map = BTreeMap::new();
    map.insert("tenant", tenant.0.clone());
    map.insert("collection", collection.to_string());
    if let Some(c) = code {
        map.insert("code", c.to_string());
    }
    map
}
