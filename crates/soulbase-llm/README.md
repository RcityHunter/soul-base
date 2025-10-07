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
