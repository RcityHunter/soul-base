# soulbase-llm (RIS)

Provider-agnostic LLM SPI with a local provider:
- Chat (sync/stream), Tool proposal placeholder
- Structured output guard (Off / StrictReject / StrictRepair)
- Embeddings (toy), Rerank (toy)
- Error normalization (stable codes)
- No external HTTP required

## Build & Test
```bash
cargo check
cargo test
```

## Configuration & Runtime

`soulbase-llm-service` now consumes configuration via `soulbase-config` and the unified
`infra` schema. Provide runtime configuration with `LLM_CONFIG_PATHS` (comma or semicolon
separated file list) and `LLM__` environment overrides. A sample lives in
`examples/config/dev.json` alongside the `soulbase-configctl` CLI:

```bash
cargo run -p soulbase-config --bin soulbase-configctl -- \
  validate --file examples/config/dev.json \
  --snapshot-out var/config/llm.snapshot.json \
  --changes-out var/config/llm.changes.json

LLM_CONFIG_PATHS=examples/config/dev.json cargo run \
  -p soulbase-llm --bin soulbase-llm-service --features service
```

The service materialises cache/blob/queue handles through `soulbase-infra`. Redis/S3/Kafka
settings share `{tenant}` placeholders that resolve per request, backed by Surreal outbox
delivery for tool invocation events.

## Features

- `provider-local` – enable the built-in echo provider (default scaffold)
- `provider-openai` – enable the OpenAI adapter. Configure it and register the factory:

```rust
use soulbase_llm::prelude::*;

let cfg = OpenAiConfig::new("sk-live-...")
    .unwrap()
    .with_alias("gpt-4o", "gpt-4o-2024-08-06");
let factory = OpenAiProviderFactory::new(cfg).unwrap();
let mut reg = Registry::new();
factory.install(&mut reg);
```

## Next

- Extend real providers (Claude/Gemini) via feature flags
- Integrate soulbase-tools ToolSpec with plan/JSON convergence
- Pricing table & QoS integration
- Observability exports (metrics/tracing)
