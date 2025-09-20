# soulbase-tx (RIS)

Reliable transactions for the Soul platform:

- Outbox leasing with dispatch-key grouping, configurable backoff, dead-letter recording, and replay/quarantine hooks
- Dispatcher abstraction (transport + store + backoff policy)
- Idempotency registry with hash guards, success/failure outcomes, and TTL cleanup
- Saga orchestrator with execute/compensate lifecycle and deterministic cancellation
- In-memory backend mirroring the public SPI for local development and tests
- SurrealDB adapter scaffolding ready for future implementation

## Build & Test

```bash
cargo fmt
cargo test -p soulbase-tx
```

> `cargo clippy --workspace --all-targets --all-features -D warnings` is recommended when the `clippy` component is installed.

## Next

- Implement the SurrealDB stores to persist outbox/idempotency/saga state
- Add TX-specific error codes and richer metrics integration
- Extend Saga orchestration with concurrent branches and QoS budgets
- Wire dispatcher instrumentation into soulbase-observe and soulbase-qos