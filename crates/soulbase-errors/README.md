# soulbase-errors

Unified error domain & stable error codes.

## Build & Test
~~~bash
cargo check
cargo test
~~~

## Optional features
- http: map to HTTP status codes
- grpc: map to gRPC status
- wrap-reqwest / wrap-sqlx: wrap external errors
- wrap-llm: reserved for future LLM adapters
