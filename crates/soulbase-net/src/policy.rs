use std::collections::HashSet;
use std::time::Duration;

use http::StatusCode;
use url::Url;

#[derive(Clone, Debug)]
pub struct NetPolicy {
    pub retry: RetryPolicy,
    pub backoff: BackoffCfg,
    pub cbreaker: CircuitBreakerPolicy,
    pub redirect: RedirectPolicy,
    pub tls: TlsPolicy,
    pub dns: DnsPolicy,
    pub proxy: ProxyPolicy,
    pub security: SecurityPolicy,
    pub limits: LimitsPolicy,
    pub cache_hook: CacheHookPolicy,
}

impl Default for NetPolicy {
    fn default() -> Self {
        Self {
            retry: RetryPolicy::default(),
            backoff: BackoffCfg::default(),
            cbreaker: CircuitBreakerPolicy::default(),
            redirect: RedirectPolicy::default(),
            tls: TlsPolicy::default(),
            dns: DnsPolicy::default(),
            proxy: ProxyPolicy::default(),
            security: SecurityPolicy::default(),
            limits: LimitsPolicy::default(),
            cache_hook: CacheHookPolicy::default(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RetryPolicy {
    pub enabled: bool,
    pub max_attempts: u32,
    pub retry_on: RetryOn,
    pub respect_retry_after: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            max_attempts: 3,
            retry_on: RetryOn::default(),
            respect_retry_after: true,
        }
    }
}

#[derive(Clone, Debug)]
pub struct RetryOn {
    pub statuses: HashSet<u16>,
    pub connect_errors: bool,
    pub timeout_errors: bool,
    pub dns_errors: bool,
}

impl RetryOn {
    pub fn should_retry_status(&self, status: StatusCode) -> bool {
        self.statuses.contains(&status.as_u16())
    }
}

impl Default for RetryOn {
    fn default() -> Self {
        let mut statuses = HashSet::new();
        statuses.insert(StatusCode::REQUEST_TIMEOUT.as_u16());
        statuses.insert(StatusCode::TOO_MANY_REQUESTS.as_u16());
        statuses.insert(StatusCode::INTERNAL_SERVER_ERROR.as_u16());
        statuses.insert(StatusCode::BAD_GATEWAY.as_u16());
        statuses.insert(StatusCode::SERVICE_UNAVAILABLE.as_u16());
        statuses.insert(StatusCode::GATEWAY_TIMEOUT.as_u16());
        Self {
            statuses,
            connect_errors: true,
            timeout_errors: true,
            dns_errors: true,
        }
    }
}

#[derive(Clone, Debug)]
pub struct BackoffCfg {
    pub base_delay: Duration,
    pub max_delay: Duration,
    pub multiplier: f32,
    pub jitter: bool,
}

impl Default for BackoffCfg {
    fn default() -> Self {
        Self {
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
            multiplier: 2.0,
            jitter: true,
        }
    }
}

#[derive(Clone, Debug)]
pub struct CircuitBreakerPolicy {
    pub enabled: bool,
    pub failure_ratio: f32,
    pub min_samples: u32,
    pub open_for: Duration,
    pub half_open_max: u32,
}

impl Default for CircuitBreakerPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            failure_ratio: 0.5,
            min_samples: 10,
            open_for: Duration::from_secs(30),
            half_open_max: 2,
        }
    }
}

#[derive(Clone, Debug)]
pub struct RedirectPolicy {
    pub enabled: bool,
    pub max_redirects: u8,
    pub allow_https_to_http: bool,
}

impl Default for RedirectPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            max_redirects: 4,
            allow_https_to_http: false,
        }
    }
}

#[derive(Clone, Debug)]
pub struct TlsPolicy {
    pub allow_invalid_certs: bool,
    pub alpn_h2: bool,
}

impl Default for TlsPolicy {
    fn default() -> Self {
        Self {
            allow_invalid_certs: false,
            alpn_h2: true,
        }
    }
}

#[derive(Clone, Debug)]
pub struct DnsPolicy {
    pub cache_ttl: Duration,
}

impl Default for DnsPolicy {
    fn default() -> Self {
        Self {
            cache_ttl: Duration::from_secs(60),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ProxyPolicy {
    pub http: Option<Url>,
    pub https: Option<Url>,
}

#[derive(Clone, Debug)]
pub struct SecurityPolicy {
    pub deny_private: bool,
    pub allowed_hosts: Vec<String>,
    pub blocked_hosts: Vec<String>,
}

impl Default for SecurityPolicy {
    fn default() -> Self {
        Self {
            deny_private: true,
            allowed_hosts: Vec::new(),
            blocked_hosts: Vec::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct LimitsPolicy {
    pub max_response_bytes: Option<usize>,
    pub max_body_bytes: Option<usize>,
}

impl Default for LimitsPolicy {
    fn default() -> Self {
        Self {
            max_response_bytes: None,
            max_body_bytes: None,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct CacheHookPolicy {
    pub enabled: bool,
}

#[derive(Clone, Debug)]
pub enum RetryDecision {
    UsePolicy,
    ForceRetry,
    ForceNoRetry,
}
