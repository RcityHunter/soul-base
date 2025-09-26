use std::time::Duration;

use bytes::Bytes;
use http::{HeaderMap, Method, StatusCode};
use serde_json::Value;
use url::Url;

use crate::policy::RetryDecision;

#[derive(Clone, Debug, Default)]
pub struct TimeoutCfg {
    pub connect: Option<Duration>,
    pub overall: Option<Duration>,
    pub read: Option<Duration>,
    pub write: Option<Duration>,
}

#[derive(Clone, Debug)]
pub enum Body {
    Empty,
    Bytes(Bytes),
    Json(Value),
}

impl Default for Body {
    fn default() -> Self {
        Body::Empty
    }
}

impl Body {
    pub fn as_bytes(&self) -> Option<Bytes> {
        match self {
            Body::Empty => Some(Bytes::new()),
            Body::Bytes(b) => Some(b.clone()),
            Body::Json(val) => serde_json::to_vec(val).ok().map(Bytes::from),
        }
    }
}

#[derive(Clone, Debug)]
pub struct NetRequest {
    pub method: Method,
    pub url: Url,
    pub headers: HeaderMap,
    pub body: Body,
    pub timeout: TimeoutCfg,
    pub idempotent: bool,
    pub trace_id: Option<String>,
    pub retry_decision: Option<RetryDecision>,
}

impl Default for NetRequest {
    fn default() -> Self {
        Self {
            method: Method::GET,
            url: Url::parse("http://127.0.0.1/").expect("static url"),
            headers: HeaderMap::new(),
            body: Body::Empty,
            timeout: TimeoutCfg::default(),
            idempotent: true,
            trace_id: None,
            retry_decision: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct NetResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Bytes,
    pub elapsed: Duration,
}

impl NetResponse {
    pub fn new(status: StatusCode, headers: HeaderMap, body: Bytes, elapsed: Duration) -> Self {
        Self {
            status,
            headers,
            body,
            elapsed,
        }
    }
}
