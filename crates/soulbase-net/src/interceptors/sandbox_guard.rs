use async_trait::async_trait;
use std::net::IpAddr;

use crate::errors::NetError;
use crate::interceptors::Interceptor;
use crate::policy::SecurityPolicy;
use crate::types::NetRequest;

#[derive(Clone)]
pub struct SandboxGuard {
    pub policy: SecurityPolicy,
}

#[async_trait]
impl Interceptor for SandboxGuard {
    async fn before_send(&self, request: &mut NetRequest) -> Result<(), NetError> {
        let host = match request.url.host_str() {
            Some(h) => h,
            None => return Err(NetError::schema("request missing host")),
        };

        if !self.policy.allowed_hosts.is_empty()
            && !self
                .policy
                .allowed_hosts
                .iter()
                .any(|allow| host.ends_with(allow))
        {
            return Err(NetError::forbidden("host not in allowed list"));
        }

        if self
            .policy
            .blocked_hosts
            .iter()
            .any(|blocked| host.ends_with(blocked))
        {
            return Err(NetError::forbidden("host blocked by sandbox policy"));
        }

        if self.policy.deny_private && is_private_host(host) {
            return Err(NetError::forbidden("private network access denied"));
        }

        Ok(())
    }
}

fn is_private_host(host: &str) -> bool {
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    if let Ok(ip) = host.parse::<IpAddr>() {
        return match ip {
            IpAddr::V4(v4) => {
                v4.is_private() || v4.is_loopback() || v4.is_link_local() || v4.is_broadcast()
            }
            IpAddr::V6(v6) => v6.is_loopback() || v6.is_unspecified() || v6.is_unique_local(),
        };
    }
    false
}
