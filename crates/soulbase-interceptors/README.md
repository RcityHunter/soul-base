# soulbase-interceptors (RIS)

Minimal runnable skeleton for the unified interceptor chain:
- ContextInit → RoutePolicy → AuthNMap → AuthZQuota → ResponseStamp
- Protocol-agnostic ProtoRequest / ProtoResponse abstractions
- Error normalization to soulbase-errors
- Optional HTTP adapter for Axum/Tower (--features with-axum)

## Build & Test
~~~bash
cargo check
cargo test
~~~

## Axum Example (feature = with-axum)
See src/adapters/http.rs for a minimal integration example using handle_with_chain.
