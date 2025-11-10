#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export GATEWAY_CONFIG_FILE="${GATEWAY_CONFIG_FILE:-$ROOT_DIR/config/gateway.local.toml}"
export RUST_LOG="${RUST_LOG:-info}"

echo "[soulbase-gateway] using config: $GATEWAY_CONFIG_FILE"
echo "[soulbase-gateway] RUST_LOG=${RUST_LOG}"

cd "$ROOT_DIR"
cargo run --release -p soulbase-gateway -- "$@"
