# soulbase-crypto (RIS)

The Soul platform cryptography toolbox providing:
- Deterministic canonical JSON serialization for signing and hashing
- Digest helpers for SHA-256 and BLAKE3
- Detached JWS signatures over Ed25519 with a pluggable keystore
- XChaCha20-Poly1305 authenticated encryption and HKDF utilities
- Error types aligned with the soulbase-errors catalogue

## Quick Start
```rust
use soulbase_crypto::prelude::*;

let cano = JsonCanonicalizer::default();
let payload = serde_json::json!({"b":2,"a":1});
let canonical = cano.canonical_json(&payload)?;

let dig = DefaultDigester::default().sha256(&canonical)?;

let ks = MemoryKeyStore::generate("ed25519:tenant:key", 600_000);
let signer = JwsEd25519Signer { keystore: ks.clone() };
let verifier = JwsEd25519Verifier { keystore: ks.clone() };
let jws = signer.sign_detached(&canonical)?;
verifier.verify_detached("unused", &canonical, &jws)?;

let key = hkdf_extract_expand(b"salt", b"ikm", b"info", 32);
let nonce = [0u8; 24];
let ct = XChaChaAead::default().seal(&key, &nonce, b"aad", b"plaintext")?;
let pt = XChaChaAead::default().open(&key, &nonce, b"aad", &ct)?;
assert_eq!(pt, b"plaintext");
```

### Observe integration
Enable the `observe` feature to publish counters into the Soul observe pipeline:
```rust
use soulbase_observe::sdk::metrics::MeterRegistry;
use soulbase_crypto::prelude::*;

let meter = MeterRegistry::default();
let metrics = CryptoMetrics::with_meter(&meter);
metrics.record_digest_ok();
```

## Tests
```bash
cargo test -p soulbase-crypto
```
