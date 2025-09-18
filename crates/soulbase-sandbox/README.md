# soulbase-sandbox (RIS)

Least-privilege, evidence-first controlled execution for Tools & Computer-Use.

## Included
- Capability model, grant/budget/profile builder
- Policy guard (path/domain/method checks)
- Executors: FS (read-only), NET (whitelist + simulated)
- Budget accounting & in-memory evidence sink

## Build & Test
~~~bash
cargo check
cargo test
~~~

## Next
- Replace ToolManifestLite with soulbase-tools::Manifest
- Real HTTP support via --features net-reqwest
- Additional executors (browser/proc) and QoS integration
