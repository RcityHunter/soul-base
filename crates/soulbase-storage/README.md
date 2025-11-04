# soulbase-storage (RIS)

Storage SPI + adapters for the Soul platform.

- SPI: Datastore/Session/Tx/Repository/Graph/Search/Vector/Migrator
- Default: Mock/In-Memory adapter (no external deps)
- SurrealDB adapter scaffold ready (fill in with real SDK later)
- Tenant guard, named-parameter enforcement, error normalization, metrics labels

## Build & Test
```bash
cargo check
cargo test
```

## SurrealDB Migrations
The Surreal adapter ships with schema migrations covering timeline, causal, recall, awareness, vector, and LLM explain/tooling tables. Apply them before enabling the Surreal feature set:

```bash
cargo run -p soulbase-storage --bin soulbase-storage-migrate --features surreal,migrate -- \
  --endpoint http://127.0.0.1:8000 \
  --namespace soul \
  --database default up
```

Use `--dry-run` to preview pending versions or `status` to inspect applied history. Credentials can be provided with `--username/--password` when targeting a secured instance.

## Next
- Add storage-specific error codes to `soulbase-errors`
- Wire metrics to soulbase-observe & QoS modules
- Expand filter DSL and cursor pagination support
