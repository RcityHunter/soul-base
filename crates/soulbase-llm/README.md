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

## Next

- Add real providers (OpenAI/Claude/Gemini) via feature flags
- Integrate soulbase-tools ToolSpec
- Pricing table & QoS integration
- Observability exports (metrics/tracing)
