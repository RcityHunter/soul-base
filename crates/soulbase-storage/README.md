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

## Next
- Implement the `surreal/` adapter with SurrealDB v2.3.x
- Add storage-specific error codes to `soulbase-errors`
- Wire metrics to soulbase-observe & QoS modules
- Expand filter DSL and cursor pagination support
