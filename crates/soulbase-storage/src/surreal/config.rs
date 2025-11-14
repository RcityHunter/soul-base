#[derive(Clone, Debug)]
pub struct SurrealConfig {
    pub endpoint: String,
    pub namespace: String,
    pub database: String,
    pub username: Option<String>,
    pub password: Option<String>,
    /// Connection pool size (currently not used, kept for backward compatibility)
    pub pool_size: Option<usize>,
    /// Strict mode flag (currently not used, kept for backward compatibility)
    pub strict_mode: Option<bool>,
}

impl Default for SurrealConfig {
    fn default() -> Self {
        Self {
            endpoint: "mem://".to_string(),
            namespace: "soul".to_string(),
            database: "default".to_string(),
            username: None,
            password: None,
            pool_size: None,
            strict_mode: None,
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
            pool_size: None,
            strict_mode: None,
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

    /// Configure connection pool size (backward compatibility - currently not enforced)
    pub fn with_pool(mut self, size: usize) -> Self {
        self.pool_size = Some(size);
        self
    }

    /// Configure strict mode (backward compatibility - currently not enforced)
    pub fn strict_mode(mut self, enabled: bool) -> Self {
        self.strict_mode = Some(enabled);
        self
    }
}
