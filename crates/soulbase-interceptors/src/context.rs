use async_trait::async_trait;
use http::Extensions;
use serde::{Deserialize, Serialize};
use soulbase_types::prelude::*;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct InterceptContext {
    pub request_id: String,
    pub trace: TraceContext,
    pub tenant_header: Option<String>,
    pub consent_token: Option<String>,
    pub route: Option<RouteBinding>,
    pub subject: Option<Subject>,
    pub obligations: Vec<Obligation>,
    pub envelope_seed: EnvelopeSeed,
    pub authn_input: Option<soulbase_auth::prelude::AuthnInput>,
    pub config_version: Option<String>,
    pub config_checksum: Option<String>,
    pub resilience: ResilienceConfig,
    pub extensions: Extensions,
}

impl Default for InterceptContext {
    fn default() -> Self {
        Self {
            request_id: String::new(),
            trace: TraceContext {
                trace_id: None,
                span_id: None,
                baggage: Default::default(),
            },
            tenant_header: None,
            consent_token: None,
            route: None,
            subject: None,
            obligations: Vec::new(),
            envelope_seed: EnvelopeSeed::default(),
            authn_input: None,
            config_version: None,
            config_checksum: None,
            resilience: ResilienceConfig::default(),
            extensions: Extensions::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct EnvelopeSeed {
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
    pub partition_key: String,
    pub produced_at_ms: i64,
}

#[derive(Clone, Debug)]
pub struct RouteBinding {
    pub resource: soulbase_auth::prelude::ResourceUrn,
    pub action: soulbase_auth::prelude::Action,
    pub attrs: serde_json::Value,
}

pub type Obligation = soulbase_auth::prelude::Obligation;

#[derive(Clone, Copy, Debug)]
pub struct ResilienceConfig {
    pub timeout: Duration,
    pub max_retries: usize,
    pub backoff: Duration,
}

impl Default for ResilienceConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(5),
            max_retries: 0,
            backoff: Duration::from_millis(0),
        }
    }
}

#[async_trait]
pub trait ProtoRequest: Send {
    fn method(&self) -> &str;
    fn path(&self) -> &str;
    fn header(&self, name: &str) -> Option<String>;
    async fn read_json(&mut self) -> Result<serde_json::Value, crate::errors::InterceptError>;
}

#[async_trait]
pub trait ProtoResponse: Send {
    fn set_status(&mut self, code: u16);
    fn insert_header(&mut self, name: &str, value: &str);
    async fn write_json(
        &mut self,
        body: &serde_json::Value,
    ) -> Result<(), crate::errors::InterceptError>;
}
