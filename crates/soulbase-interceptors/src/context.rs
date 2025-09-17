use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use soulbase_types::prelude::*;

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
    async fn write_json(&mut self, body: &serde_json::Value) -> Result<(), crate::errors::InterceptError>;
}
