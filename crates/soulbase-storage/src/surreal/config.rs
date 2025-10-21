#[derive(Clone, Debug)]
pub struct SurrealConfig {
    pub endpoint: String,
    pub namespace: String,
    pub database: String,
    pub username: Option<String>,
    pub password: Option<String>,
}

impl Default for SurrealConfig {
    fn default() -> Self {
        Self {
            endpoint: "mem://".to_string(),
            namespace: "soul".to_string(),
            database: "default".to_string(),
            username: None,
            password: None,
        }
    }
}

impl SurrealConfig {
    pub fn new(
        endpoint: impl Into<String>,
        namespace: impl Into<String>,
        database: impl Into<String>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            namespace: namespace.into(),
            database: database.into(),
            username: None,
            password: None,
        }
    }

    pub fn with_credentials(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.username = Some(username.into());
        self.password = Some(password.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::SurrealConfig;

    #[test]
    fn default_config_uses_mem_endpoint() {
        let cfg = SurrealConfig::default();
        assert_eq!(cfg.endpoint, "mem://");
        assert_eq!(cfg.namespace, "soul");
        assert_eq!(cfg.database, "default");
        assert!(cfg.username.is_none());
        assert!(cfg.password.is_none());
    }

    #[test]
    fn new_with_credentials_sets_fields() {
        let cfg = SurrealConfig::new("ws://remote", "ns", "db")
            .with_credentials("user", "pass");
        assert_eq!(cfg.endpoint, "ws://remote");
        assert_eq!(cfg.namespace, "ns");
        assert_eq!(cfg.database, "db");
        assert_eq!(cfg.username.as_deref(), Some("user"));
        assert_eq!(cfg.password.as_deref(), Some("pass"));
    }
}
