# soulbase-tx (RIS)

Reliable transactions for the Soul platform:

- Outbox leasing with retry, dead lettering, and replay hooks
- Dispatcher abstraction (transport + store + backoff)
- Idempotency registry for producer/consumer flows
- Saga orchestrator with execute/compensate lifecycle
- In-memory backend to support local development and tests
- SurrealDB adapter scaffolding ready for future implementation

## Build & Test

```bash
cargo check
cargo test
```

## Next

- Implement the SurrealDB stores to persist outbox/idempotency/saga state
- Add TX-specific error codes and richer metrics integration
- Extend Saga orchestration with concurrent branches and QoS budgets
- Wire dispatcher instrumentation into soulbase-observe and soulbase-qos

