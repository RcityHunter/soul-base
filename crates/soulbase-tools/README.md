# soulbase-tools (RIS)

Tool manifest + registry + preflight + invoker SDK for the Soul platform.
- Declarative manifests with JSON-Schema validation
- In-memory registry with tenant scoping placeholder
- Preflight hook to auth, sandbox policy, and caching digests
- Invoker that maps manifests to sandbox ExecOps and records evidence

## Build & Test
```bash
cargo check
cargo test -p soulbase-tools
```

## Next
- Pluggable registry backends (Redis/Postgres)
- Rich capability → ExecOp mapping DSL
- Tool analytics export and observability wiring
- Multi-tenant caching / idempotency stores
