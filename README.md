# Soul Base

Soul Base is a Rust workspace that unifies “thin-waist” capabilities for Soulbase Gateway—standardized APIs that sit between upstream apps and diverse execution/storage backends. The gateway (Axum + Tokio) orchestrates tool execution, collaboration flows, LLM access, graph/repo/observe services, plus authentication, policy, storage, and observability.

Soul Base 是一个多 crate 的 Rust 工作区，为 Soulbase Gateway 及其周边工具、存储、观测组件提供统一的“薄腰”能力：上游应用只需面对一组稳定 API，就能调用不同的工具执行、协作、LLM 与存储后端。

## Repository Structure / 仓库结构

- `crates/soulbase-gateway`: Axum gateway binary (tools/collab/LLM/graph/repo/observe routes + interceptor chain).  
- `crates/soulbase-tools`, `soulbase-llm`, `soulbase-interceptors`, etc.: thin-waist protocol + execution runtimes (invoker, LLM providers, auth/schema/policy stages).  
- `config/`: reference configs (e.g., `gateway.local.toml`).  
- `docs/`: runbooks, secret guidelines.  
- `scripts/`, `deploy/`: systemd/Docker helpers.  
- `tests/`, `crates/*/tests`: contract/integration/perf suites.

（以上目录分别包含网关主服务、薄腰协议与执行库、配置、文档、运维脚本以及契约/集成/性能测试。）

## Architecture Overview / 架构概览

1. **Interceptor Chain** (`soulbase-interceptors`): Context init, AuthN (token/HMAC/OIDC), schema validation, policy/quota, idempotency, error normalization.  
2. **Service Dispatch** (`soulbase-gateway/src/main.rs`): Routes map to Tool/Collab/LLM/Graph/Repo/Observe handlers backed by `soulbase-tools`, `soulbase-llm`, `soulbase-storage`, etc.  
3. **Storage & Registry**: `GatewayStorage` abstracts Surreal/memory backends for manifests, idempotency, evidence, repo/graph/observe stores.  
4. **Observability**: `soulbase-observe`, `soulbase-net` provide tracing, metrics, logging hooks (see `docs/runbook.md`).  
5. **Tests & Tooling**: `gateway_contracts` / `gateway_perf` start real binaries with temp configs; `scripts/run-gateway.sh`, `deploy/` help local or prod deployments.

整体数据流：

```
HTTP Request
   └─> Interceptor Chain (Context/Auth/Schema/Policy/Error)
        └─> Service Dispatch (Tool/Collab/LLM/Graph/Repo/Observe)
             ├─> soulbase-tools / sandbox（工具执行与证据）
             ├─> soulbase-llm（LLM Providers / Usage / Cost）
             ├─> soulbase-storage（Repo/Graph/Observe 存储）
             └─> soulbase-observe（事件与指标）
```

## Quick Start / 快速开始

```bash
cargo fmt --all
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```

Launch the gateway with the sample config / 使用示例配置启动网关：

```bash
export GATEWAY_CONFIG_FILE=config/gateway.local.toml
export GATEWAY_TOKEN_LOCAL=dev-token
export GATEWAY_HMAC_LOCAL=dev-hmac
cargo run -p soulbase-gateway
```

## Test Matrix / 测试矩阵

- Contract & integration tests / 契约与集成测试：  
  `cargo test -p soulbase-gateway contract_`  
  `cargo test -p soulbase-gateway integration_`
- Performance harness / 性能压测（需允许本地端口）：  
  `cargo test -p soulbase-gateway --test gateway_perf -- --ignored`

更多演练、压测记录详见 `docs/runbook.md`。

## Contributing / 贡献指南

1. Read `AGENTS.md` & `docs/runbook.md` for coding and ops conventions / 先阅读 `AGENTS.md`、`docs/runbook.md`。  
2. Review crate-specific READMEs before editing / 修改前查看对应 crate 的说明。  
3. Run `cargo fmt`, `cargo clippy`, `cargo test --workspace --all-features` before submitting / 提交前确保上述命令通过。  
4. Use Conventional Commit style (e.g., `feat: add collab contract tests`) / 使用规范化的提交信息。

欢迎通过 Issue/PR 贡献新的薄腰能力、测试、性能优化与文档更新。***
