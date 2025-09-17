# soulbase-auth (RIS)

Minimal runnable skeleton for AuthN · AuthZ · Quota SPI.

- AuthN: OIDC stub (BearerJwt "sub@tenant" -> Subject)
- PDP: Local (deny-by-default; allow when attrs.allow = true)
- Quota: In-memory store (always allowed)
- Cache: In-memory TTL decision cache

## Run
~~~bash
cargo check
cargo test
~~~

## Next
- Replace the OIDC stub with a real verifier (Soul-Auth service).
- Add remote PDP adapters (OPA, Cedar, ...).
- Provide Redis-backed quota and decision cache implementations.
