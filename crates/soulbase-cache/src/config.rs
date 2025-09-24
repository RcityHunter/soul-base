#[cfg(feature = "redis")]
#[derive(Clone, Debug)]
pub struct RedisConfig {
    pub url: String,
    pub key_prefix: String,
}

#[cfg(feature = "redis")]
impl RedisConfig {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            key_prefix: "soulbase:cache".into(),
        }
    }

    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.key_prefix = prefix.into();
        self
    }
}
