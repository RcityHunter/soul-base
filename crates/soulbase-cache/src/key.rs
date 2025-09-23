#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CacheKey {
    raw: String,
}

impl CacheKey {
    pub fn new(raw: String) -> Self {
        Self { raw }
    }

    pub fn as_str(&self) -> &str {
        &self.raw
    }
}

#[derive(Clone, Debug)]
pub struct KeyParts {
    pub tenant: String,
    pub namespace: String,
    pub payload_hash: String,
}

impl KeyParts {
    pub fn new(
        tenant: impl Into<String>,
        namespace: impl Into<String>,
        payload_hash: impl Into<String>,
    ) -> Self {
        Self {
            tenant: tenant.into(),
            namespace: namespace.into(),
            payload_hash: payload_hash.into(),
        }
    }
}

pub fn build_key(parts: KeyParts) -> CacheKey {
    let raw = format!(
        "{}:{}:{}",
        parts.tenant, parts.namespace, parts.payload_hash
    );
    CacheKey { raw }
}
