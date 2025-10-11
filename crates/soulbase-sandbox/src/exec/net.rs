use crate::errors::SandboxError;
use crate::model::{ExecOp, ExecResult, Profile};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use serde_json::json;
use std::collections::HashMap;
use std::time::Instant;
use tokio::time::{sleep, Duration};
use url::Url;

#[cfg(feature = "net-reqwest")]
#[cfg(test)]
mod tests {
    use super::{NetExecutor, B64};
    use crate::config::{Mappings, PolicyConfig};
    use crate::model::{Budget, Capability, ExecOp, Profile, SafetyClass, SideEffect};
    use base64::Engine as _;
    use chrono::Utc;
    use soulbase_types::prelude::{Id, TenantId};
    use std::collections::HashMap;

    fn profile(cap: Capability) -> Profile {
        let policy = PolicyConfig {
            mappings: Mappings {
                root_fs: ".".into(),
                tmp_dir: std::env::temp_dir().display().to_string(),
            },
            whitelists: Default::default(),
        };
        Profile {
            tenant: TenantId("tenant".into()),
            subject_id: Id("subject".into()),
            tool_name: "tool".into(),
            call_id: Id("call".into()),
            capabilities: vec![cap],
            policy,
            expires_at: Utc::now().timestamp_millis() + 60_000,
            budget: Budget::default(),
            safety_class: SafetyClass::Low,
            side_effect: SideEffect::Network,
            manifest_name: "tool".into(),
            decision_key_fingerprint: "fp".into(),
        }
    }

    #[cfg(feature = "net-reqwest")]
    #[tokio::test]
    async fn net_executor_makes_real_request() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");

        tokio::spawn(async move {
            if let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = [0u8; 512];
                let _ = socket.read(&mut buf).await;
                let response = b"HTTP/1.1 200 OK\r\nContent-Length:5\r\n\r\nhello";
                let _ = socket.write_all(response).await;
            }
        });

        let capability = Capability::NetHttp {
            host: addr.ip().to_string(),
            port: Some(addr.port()),
            scheme: Some("http".into()),
            methods: vec!["GET".into()],
        };
        let profile = profile(capability);
        let executor = NetExecutor::new();
        let op = ExecOp::NetHttp {
            method: "GET".into(),
            url: format!("http://{}", addr),
            headers: HashMap::new(),
            body_b64: None,
        };

        let result = executor.execute(&profile, &op).await.expect("net execute");
        assert!(result.ok);
        let body_b64 = result
            .out
            .get("body_b64")
            .and_then(|v| v.as_str())
            .expect("body present");
        let data = B64.decode(body_b64).expect("decode");
        assert_eq!(data, b"hello");
    }
}

#[derive(Clone)]
pub struct NetExecutor {
    pub simulate_latency_ms: u64,
    pub max_body_bytes: usize,
    pub preview_bytes: usize,
    #[cfg(feature = "net-reqwest")]
    client: reqwest::Client,
}

impl Default for NetExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl NetExecutor {
    pub fn new() -> Self {
        Self {
            simulate_latency_ms: 0,
            max_body_bytes: 64 * 1024,
            preview_bytes: 2 * 1024,
            #[cfg(feature = "net-reqwest")]
            client: reqwest::Client::new(),
        }
    }

    pub async fn execute(
        &self,
        profile: &Profile,
        op: &ExecOp,
    ) -> Result<ExecResult, SandboxError> {
        if self.simulate_latency_ms > 0 {
            sleep(Duration::from_millis(self.simulate_latency_ms)).await;
        }

        match op {
            ExecOp::NetHttp {
                method,
                url,
                headers,
                body_b64,
            } => {
                self.http(profile, method, url, headers, body_b64.as_deref())
                    .await
            }
            _ => Err(SandboxError::permission("network operation not supported")),
        }
    }

    async fn http(
        &self,
        profile: &Profile,
        method: &str,
        url: &str,
        headers: &HashMap<String, String>,
        body_b64: Option<&str>,
    ) -> Result<ExecResult, SandboxError> {
        let parsed = Url::parse(url).map_err(|_| SandboxError::permission("invalid url"))?;
        let host = parsed.host_str().unwrap_or_default().to_string();

        #[cfg(feature = "net-reqwest")]
        {
            use reqwest::{header::HeaderMap, Method};

            let method = Method::from_bytes(method.as_bytes())
                .map_err(|_| SandboxError::schema("unsupported http method"))?;

            let mut header_map = HeaderMap::new();
            for (name, value) in headers {
                if let Ok(header_name) = reqwest::header::HeaderName::from_bytes(name.as_bytes()) {
                    if let Ok(header_value) = reqwest::header::HeaderValue::from_str(value) {
                        header_map.insert(header_name, header_value);
                    }
                }
            }

            let body =
                if let Some(b64) = body_b64 {
                    Some(B64.decode(b64).map_err(|err| {
                        SandboxError::schema(&format!("net http body decode: {err}"))
                    })?)
                } else {
                    None
                };

            let mut request = self
                .client
                .request(method, parsed.clone())
                .headers(header_map);
            if let Some(body) = body {
                request = request.body(body);
            }

            let started = Instant::now();
            let response = request
                .send()
                .await
                .map_err(|err| SandboxError::provider(&format!("http request: {err}")))?;
            let status = response.status();

            let body_bytes = response
                .bytes()
                .await
                .map_err(|err| SandboxError::provider(&format!("http body: {err}")))?;
            let original_len = body_bytes.len();
            let mut collected = body_bytes.to_vec();
            let truncated = collected.len() > self.max_body_bytes;
            if truncated {
                collected.truncate(self.max_body_bytes);
            }
            let preview_len = collected.len().min(self.preview_bytes);
            let preview = String::from_utf8_lossy(&collected[..preview_len]).to_string();
            let body_b64_out = B64.encode(&collected);
            let elapsed_ms = started.elapsed().as_millis() as u64;

            let out = json!({
                "tool": profile.tool_name,
                "status": status.as_u16(),
                "host": host,
                "elapsed_ms": elapsed_ms,
                "headers": headers,
                "body_b64": body_b64_out,
                "body_len": original_len,
                "body_preview": preview,
                "truncated": truncated,
            });
            Ok(ExecResult::success(out))
        }

        #[cfg(not(feature = "net-reqwest"))]
        {
            let out = json!({
                "simulated": true,
                "degradation": {
                    "reason": "net.http.simulated",
                },
                "host": host,
                "method": method.to_uppercase(),
                "tool": profile.tool_name,
            });
            Ok(ExecResult::success(out))
        }
    }
}
