use crate::errors::{self, ConfigError};
use async_trait::async_trait;
use chrono::Utc;
use futures::{future::BoxFuture, FutureExt};
use parking_lot::Mutex;
use percent_encoding::percent_decode_str;
use serde_json::Value;
use std::collections::HashMap;
use std::env;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

type SecretFetchFuture = BoxFuture<'static, Result<Option<Value>, ConfigError>>;
type SecretFetchFn = dyn Fn(String) -> SecretFetchFuture + Send + Sync;

#[async_trait]
pub trait SecretResolver: Send + Sync {
    fn id(&self) -> &'static str;
    async fn resolve(&self, uri: &str) -> Result<Value, ConfigError>;
}

pub struct NoopSecretResolver;

#[async_trait]
impl SecretResolver for NoopSecretResolver {
    fn id(&self) -> &'static str {
        "noop"
    }

    async fn resolve(&self, uri: &str) -> Result<Value, ConfigError> {
        Ok(Value::String(uri.to_string()))
    }
}

pub fn is_secret_ref(value: &Value) -> Option<&str> {
    value.as_str().and_then(|s| s.strip_prefix("secret://"))
}

#[derive(Clone)]
struct CacheEntry {
    value: Value,
    expires_at_ms: Option<i64>,
}

impl CacheEntry {
    fn is_valid(&self, now_ms: i64) -> bool {
        self.expires_at_ms.map_or(true, |exp| now_ms < exp)
    }
}

/// Wraps a resolver and caches resolved values with an optional TTL.
pub struct CachingResolver<R> {
    inner: R,
    ttl_ms: Option<i64>,
    cache: Mutex<HashMap<String, CacheEntry>>,
}

impl<R> CachingResolver<R> {
    pub fn new(inner: R, ttl: Option<Duration>) -> Self {
        Self {
            inner,
            ttl_ms: ttl.and_then(duration_to_i64_ms),
            cache: Mutex::new(HashMap::new()),
        }
    }

    pub fn invalidate(&self, uri: &str) {
        self.cache.lock().remove(uri);
    }

    pub fn clear(&self) {
        self.cache.lock().clear();
    }

    fn cache_lookup(&self, uri: &str) -> Option<Value> {
        let now = Utc::now().timestamp_millis();
        let mut guard = self.cache.lock();
        match guard.get(uri) {
            Some(entry) if entry.is_valid(now) => Some(entry.value.clone()),
            Some(_) => {
                guard.remove(uri);
                None
            }
            None => None,
        }
    }

    fn cache_store(&self, uri: &str, value: Value) {
        let expires_at_ms = self
            .ttl_ms
            .map(|ttl| Utc::now().timestamp_millis().saturating_add(ttl));
        self.cache.lock().insert(
            uri.to_string(),
            CacheEntry {
                value,
                expires_at_ms,
            },
        );
    }
}

fn duration_to_i64_ms(duration: Duration) -> Option<i64> {
    let millis = duration.as_millis();
    if millis > i64::MAX as u128 {
        Some(i64::MAX)
    } else {
        Some(millis as i64)
    }
}

#[async_trait]
impl<R> SecretResolver for CachingResolver<R>
where
    R: SecretResolver + Send + Sync,
{
    fn id(&self) -> &'static str {
        self.inner.id()
    }

    async fn resolve(&self, uri: &str) -> Result<Value, ConfigError> {
        if let Some(value) = self.cache_lookup(uri) {
            return Ok(value);
        }

        let resolved = self.inner.resolve(uri).await?;
        self.cache_store(uri, resolved.clone());
        Ok(resolved)
    }
}

/// Reads secrets from process environment variables ().
pub struct EnvSecretResolver {
    prefix: Option<String>,
}

impl EnvSecretResolver {
    pub fn new() -> Self {
        Self { prefix: None }
    }

    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }
}

impl Default for EnvSecretResolver {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SecretResolver for EnvSecretResolver {
    fn id(&self) -> &'static str {
        "env"
    }

    async fn resolve(&self, uri: &str) -> Result<Value, ConfigError> {
        let raw_key = strip_resolver_prefix(uri, self.id())?;
        let key = decode_component(raw_key, "secret.env")?;
        let full_key = match &self.prefix {
            Some(prefix) => format!("{prefix}{key}"),
            None => key,
        };
        match env::var(&full_key) {
            Ok(value) => Ok(Value::String(value)),
            Err(err) => Err(errors::io_provider_unavailable(
                "secret.env",
                &format!("env var '{full_key}' not available: {err}"),
            )),
        }
    }
}

/// Reads secrets from files (URIs like secret://file/path). Paths may be percent-encoded.
pub struct FileSecretResolver {
    base_dir: Option<PathBuf>,
    allow_absolute: bool,
    trim_newline: bool,
}

impl FileSecretResolver {
    pub fn new() -> Self {
        Self {
            base_dir: None,
            allow_absolute: false,
            trim_newline: true,
        }
    }

    pub fn with_base_dir(mut self, base_dir: impl Into<PathBuf>) -> Self {
        self.base_dir = Some(base_dir.into());
        self
    }

    pub fn allow_absolute(mut self, allow: bool) -> Self {
        self.allow_absolute = allow;
        self
    }

    pub fn trim_trailing_newline(mut self, trim: bool) -> Self {
        self.trim_newline = trim;
        self
    }

    fn resolve_path(&self, raw: &str) -> Result<PathBuf, ConfigError> {
        if raw.is_empty() {
            return Err(errors::schema_invalid(
                "secret.file",
                "path must not be empty",
            ));
        }
        let decoded = decode_component(raw, "secret.file")?;
        let candidate = Path::new(decoded.as_str());

        if candidate.is_absolute() {
            if self.base_dir.is_some() && !self.allow_absolute {
                return Err(errors::schema_invalid(
                    "secret.file",
                    "absolute paths disallowed when base_dir is set",
                ));
            }
            return Ok(candidate.to_path_buf());
        }

        if let Some(base) = &self.base_dir {
            if candidate
                .components()
                .any(|component| matches!(component, Component::ParentDir))
            {
                return Err(errors::schema_invalid(
                    "secret.file",
                    "relative path must not contain '..'",
                ));
            }
            Ok(base.join(candidate))
        } else {
            Ok(candidate.to_path_buf())
        }
    }
}

impl Default for FileSecretResolver {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SecretResolver for FileSecretResolver {
    fn id(&self) -> &'static str {
        "file"
    }

    async fn resolve(&self, uri: &str) -> Result<Value, ConfigError> {
        let raw_path = strip_resolver_prefix(uri, self.id())?;
        let path = self.resolve_path(raw_path)?;
        let bytes = std::fs::read(&path).map_err(|err| {
            errors::io_provider_unavailable(
                "secret.file",
                &format!("failed to read '{}': {err}", path.display()),
            )
        })?;

        let mut content = String::from_utf8(bytes).map_err(|err| {
            errors::schema_invalid(
                "secret.file",
                &format!("file '{}' is not valid UTF-8: {err}", path.display()),
            )
        })?;

        if self.trim_newline {
            while matches!(content.as_bytes().last(), Some(10) | Some(13)) {
                content.pop();
            }
        }

        Ok(Value::String(content))
    }
}

/// Delegates secret lookups to an async key-value backend
/// (URIs like secret://resolver/key). Keys may be percent-encoded.
pub struct KvSecretResolver {
    id: &'static str,
    fetcher: Arc<SecretFetchFn>,
    error_on_missing: bool,
}

impl KvSecretResolver {
    pub fn new<F, Fut>(id: &'static str, fetcher: F) -> Self
    where
        F: Fn(String) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<Option<Value>, ConfigError>> + Send + 'static,
    {
        let fetcher: Arc<SecretFetchFn> = Arc::new(move |key: String| fetcher(key).boxed());
        Self {
            id,
            fetcher,
            error_on_missing: true,
        }
    }

    pub fn allow_missing(mut self, allow: bool) -> Self {
        self.error_on_missing = !allow;
        self
    }
}

#[async_trait]
impl SecretResolver for KvSecretResolver {
    fn id(&self) -> &'static str {
        self.id
    }

    async fn resolve(&self, uri: &str) -> Result<Value, ConfigError> {
        let raw_key = strip_resolver_prefix(uri, self.id)?;
        let key = decode_component(raw_key, "secret.kv")?;
        if key.is_empty() {
            return Err(errors::schema_invalid("secret.kv", "key must not be empty"));
        }

        match (self.fetcher)(key.clone()).await? {
            Some(value) => Ok(value),
            None if self.error_on_missing => Err(errors::io_provider_unavailable(
                "secret.kv",
                &format!("key '{key}' not found"),
            )),
            None => Ok(Value::Null),
        }
    }
}

fn strip_resolver_prefix<'a>(uri: &'a str, id: &str) -> Result<&'a str, ConfigError> {
    let expected = format!("secret://{id}/");
    uri.strip_prefix(&expected).ok_or_else(|| {
        errors::schema_invalid(
            "secret",
            &format!("resolver '{id}' expects URIs like '{expected}<value>', got '{uri}'"),
        )
    })
}

fn decode_component(raw: &str, ctx: &str) -> Result<String, ConfigError> {
    percent_decode_str(raw)
        .decode_utf8()
        .map(|cow| cow.into_owned())
        .map_err(|err| errors::schema_invalid(ctx, &format!("invalid percent-encoding: {err}")))
}
