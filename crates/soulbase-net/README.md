# soulbase-net (RIS)

Resilient HTTP Client for the Soul platform:
- `NetClient` trait with `ReqwestClient` implementation
- Retry (5xx/429/connect) with policy driven backoff
- Circuit breaker supporting Closed/Open/HalfOpen states
- Sandbox guard to deny private/internal networks and enforce security
- Trace/User-Agent interceptors for observability
- Metrics counters with optional `observe` integration

## Quick Start
```rust
use soulbase_net::prelude::*;

let mut policy = NetPolicy::default();
policy.security.deny_private = false;

let client = ClientBuilder::default()
    .with_policy(policy.clone())
    .with_interceptor(TraceUa::default())
    .with_interceptor(SandboxGuard { policy: policy.security.clone() })
    .build()?;

let mut request = NetRequest::default();
request.method = http::Method::GET;
request.url = "https://example.com".parse()?;

let resp = client.send(request).await?;
println!("status = {}", resp.status);
```

### Observe integration
```rust
use soulbase_observe::sdk::metrics::MeterRegistry;
use soulbase_net::prelude::*;

let meter = MeterRegistry::default();
let metrics = NetMetrics::with_meter(&meter);
let client = ClientBuilder::default()
    .with_metrics(metrics.clone())
    .build()?;
metrics.record_request();
```

## Roadmap
- HTTP/3, mTLS, and SWR/cache hooks
- Advanced routing, DNS policy sync, proxy auth
```
