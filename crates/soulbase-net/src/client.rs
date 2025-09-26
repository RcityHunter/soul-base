use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use http::StatusCode;
use parking_lot::Mutex;
use tokio::time::sleep;

use crate::errors::NetError;
use crate::interceptors::{Interceptor, InterceptorObject};
use crate::metrics::NetMetrics;
use crate::policy::{NetPolicy, RetryDecision};
use crate::runtime::cbreaker::CircuitBreaker;
use crate::runtime::retry::RetryState;
use crate::types::{Body, NetRequest, NetResponse};

#[async_trait]
pub trait NetClient: Send + Sync {
    async fn send(&self, request: NetRequest) -> Result<NetResponse, NetError>;
}

#[derive(Clone)]
pub struct ReqwestClient {
    pub policy: NetPolicy,
    pub client: reqwest::Client,
    pub interceptors: Vec<InterceptorObject>,
    pub metrics: NetMetrics,
    circuit: Arc<Mutex<CircuitBreaker>>,
}

impl ReqwestClient {
    pub fn metrics(&self) -> &NetMetrics {
        &self.metrics
    }

    fn should_retry_status(&self, status: StatusCode, decision: &Option<RetryDecision>) -> bool {
        match decision {
            Some(RetryDecision::ForceRetry) => true,
            Some(RetryDecision::ForceNoRetry) => false,
            _ => self.policy.retry.retry_on.should_retry_status(status),
        }
    }
}

#[async_trait]
impl NetClient for ReqwestClient {
    async fn send(&self, request: NetRequest) -> Result<NetResponse, NetError> {
        let mut retry_state = RetryState::new();

        loop {
            let mut attempt_request = request.clone();
            for interceptor in &self.interceptors {
                interceptor.before_send(&mut attempt_request).await?;
            }

            if let Some(limit) = self.policy.limits.max_body_bytes {
                if let Some(bytes) = attempt_request.body.as_bytes() {
                    if bytes.len() > limit {
                        return Err(NetError::schema("request body exceeds max_body_bytes"));
                    }
                }
            }

            {
                let mut circuit = self.circuit.lock();
                if !circuit.can_execute() {
                    return Err(NetError::provider_unavailable("circuit breaker open"));
                }
            }

            self.metrics.record_request();

            let start = Instant::now();
            let mut req_builder = self
                .client
                .request(attempt_request.method.clone(), attempt_request.url.clone())
                .headers(attempt_request.headers.clone());

            match &attempt_request.body {
                Body::Empty => {}
                Body::Bytes(bytes) => {
                    req_builder = req_builder.body(bytes.clone());
                }
                Body::Json(value) => {
                    req_builder = req_builder.json(value);
                }
            }

            if let Some(timeout) = attempt_request.timeout.overall {
                req_builder = req_builder.timeout(timeout);
            }

            let response = req_builder.send().await;

            match response {
                Ok(resp) => {
                    let status = resp.status();
                    let headers = resp.headers().clone();
                    let body_bytes = resp
                        .bytes()
                        .await
                        .map_err(|err| NetError::unknown(&format!("response body error: {err}")))?;

                    if let Some(limit) = self.policy.limits.max_response_bytes {
                        if body_bytes.len() > limit {
                            self.metrics.record_failure();
                            {
                                let mut circuit = self.circuit.lock();
                                circuit.record_failure();
                            }
                            return Err(NetError::provider_unavailable(
                                "response exceeded max_response_bytes",
                            ));
                        }
                    }

                    let elapsed = start.elapsed();

                    if status.is_success() {
                        {
                            let mut circuit = self.circuit.lock();
                            circuit.record_success();
                        }
                        return Ok(NetResponse::new(status, headers, body_bytes, elapsed));
                    }

                    let decision = attempt_request.retry_decision.clone();
                    if self.should_retry_status(status, &decision) {
                        if let Some(delay) =
                            retry_state.next_delay(&self.policy.retry, &self.policy.backoff)
                        {
                            self.metrics.record_retry();
                            {
                                let mut circuit = self.circuit.lock();
                                circuit.record_failure();
                            }
                            sleep(delay).await;
                            continue;
                        }
                    }

                    self.metrics.record_failure();
                    {
                        let mut circuit = self.circuit.lock();
                        circuit.record_failure();
                    }
                    return Err(NetError::provider_unavailable(&format!(
                        "upstream returned status {status}"
                    )));
                }
                Err(err) => {
                    let decision = attempt_request.retry_decision.clone();
                    let should_retry = if err.is_timeout() {
                        decision
                            .as_ref()
                            .map(|d| matches!(d, RetryDecision::ForceRetry))
                            .unwrap_or(self.policy.retry.retry_on.timeout_errors)
                    } else if err.is_connect() {
                        decision
                            .as_ref()
                            .map(|d| matches!(d, RetryDecision::ForceRetry))
                            .unwrap_or(self.policy.retry.retry_on.connect_errors)
                    } else if err.is_request() {
                        decision
                            .as_ref()
                            .map(|d| matches!(d, RetryDecision::ForceRetry))
                            .unwrap_or(self.policy.retry.retry_on.dns_errors)
                    } else {
                        false
                    };

                    if should_retry {
                        if let Some(delay) =
                            retry_state.next_delay(&self.policy.retry, &self.policy.backoff)
                        {
                            self.metrics.record_retry();
                            {
                                let mut circuit = self.circuit.lock();
                                circuit.record_failure();
                            }
                            sleep(delay).await;
                            continue;
                        }
                    }

                    self.metrics.record_failure();
                    {
                        let mut circuit = self.circuit.lock();
                        circuit.record_failure();
                    }
                    return Err(NetError::provider_unavailable(&format!(
                        "request error: {err}"
                    )));
                }
            }
        }
    }
}

pub struct ClientBuilder {
    policy: NetPolicy,
    interceptors: Vec<InterceptorObject>,
    metrics: NetMetrics,
    client: Option<reqwest::Client>,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self {
            policy: NetPolicy::default(),
            interceptors: Vec::new(),
            metrics: NetMetrics::default(),
            client: None,
        }
    }
}

impl ClientBuilder {
    pub fn with_policy(mut self, policy: NetPolicy) -> Self {
        self.policy = policy;
        self
    }

    pub fn with_interceptor<I>(mut self, interceptor: I) -> Self
    where
        I: Interceptor + 'static,
    {
        self.interceptors.push(Arc::new(interceptor));
        self
    }

    pub fn with_metrics(mut self, metrics: NetMetrics) -> Self {
        self.metrics = metrics;
        self
    }

    pub fn with_reqwest_client(mut self, client: reqwest::Client) -> Self {
        self.client = Some(client);
        self
    }

    pub fn build(self) -> Result<ReqwestClient, NetError> {
        let client = match self.client {
            Some(client) => client,
            None => build_reqwest_client(&self.policy)?,
        };
        let circuit = CircuitBreaker::new(self.policy.cbreaker.clone());
        Ok(ReqwestClient {
            policy: self.policy,
            client,
            interceptors: self.interceptors,
            metrics: self.metrics,
            circuit: Arc::new(Mutex::new(circuit)),
        })
    }
}

fn build_reqwest_client(policy: &NetPolicy) -> Result<reqwest::Client, NetError> {
    let mut builder = reqwest::Client::builder()
        .use_rustls_tls()
        .tcp_keepalive(Some(std::time::Duration::from_secs(30)))
        .connect_timeout(std::time::Duration::from_secs(10));

    if !policy.redirect.enabled {
        builder = builder.redirect(reqwest::redirect::Policy::none());
    } else {
        builder = builder.redirect(reqwest::redirect::Policy::limited(
            policy.redirect.max_redirects as usize,
        ));
    }

    if policy.tls.allow_invalid_certs {
        builder = builder.danger_accept_invalid_certs(true);
    }

    if let Some(proxy) = policy.proxy.http.clone() {
        let proxy = reqwest::Proxy::http(proxy)
            .map_err(|err| NetError::schema(&format!("invalid http proxy: {err}")))?;
        builder = builder.proxy(proxy);
    }

    if let Some(proxy) = policy.proxy.https.clone() {
        let proxy = reqwest::Proxy::https(proxy)
            .map_err(|err| NetError::schema(&format!("invalid https proxy: {err}")))?;
        builder = builder.proxy(proxy);
    }

    builder
        .build()
        .map_err(|err| NetError::unknown(&format!("failed to build reqwest client: {err}")))
}
