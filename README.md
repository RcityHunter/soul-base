# Soul Base

<div class="language-switch" align="right">
  <input type="radio" id="lang-zh" name="lang" checked>
  <label for="lang-zh">中文</label>
  <input type="radio" id="lang-en" name="lang">
  <label for="lang-en">English</label>
</div>

<style>
.language-switch {
  display: inline-flex;
  gap: 0.75rem;
  font-weight: 600;
  margin-bottom: 1rem;
}
.language-switch input[type="radio"] {
  display: none;
}
.language-switch label {
  cursor: pointer;
  padding: 0.1rem 0.6rem;
  border-radius: 0.4rem;
  border: 1px solid #ccc;
  font-size: 0.88rem;
}
#lang-zh:checked + label,
#lang-en:checked + label {
  background: #222;
  color: #fff;
  border-color: #222;
}
#lang-zh:checked ~ #lang-en + label,
#lang-en:checked ~ #lang-zh + label {
  background: transparent;
  color: inherit;
  border-color: #ccc;
}
.lang-section {
  display: none;
}
#lang-zh:checked ~ .lang-zh {
  display: block;
}
#lang-en:checked ~ .lang-en {
  display: block;
}
</style>

<div class="lang-section lang-zh">

## 概览

Soul Base 是 Soul 平台的“薄腰”能力集合：在统一 API 下协调工具执行、协作流程、LLM 接入、图谱/仓库/观测服务，并提供鉴权、策略、观测等公共基础设施。项目以 Rust workspace 形式组织，核心组件包含：

- `soulbase-gateway`：Axum/Tokio 网关二进制，提供 HTTP/gRPC 入口与拦截链。
- `soulbase-tools`：工具注册、前置校验、计划生成、沙箱执行与证据采集。
- `soulbase-llm`：LLM Provider SPI、服务端与 Outbox 分发。
- 支撑库：`soulbase-auth`、`soulbase-sandbox`、`soulbase-observe`、`soulbase-config`、`soulbase-net`、`soulbase-storage`、`soulbase-tx` 等。

依赖关系与拆分建议详见 [`ARCHITECTURE_OVERVIEW.md`](ARCHITECTURE_OVERVIEW.md)。

## 仓库结构

| 路径 | 说明 |
| --- | --- |
| `crates/soulbase-gateway` | 网关主程序（工具/协作/LLM/图谱/Repo/Observe 路由 + 拦截链）。 |
| `crates/soulbase-tools`, `soulbase-llm`, `soulbase-interceptors` | “薄腰”协议与执行 runtime（Invoker、Provider、Auth/Policy 阶段）。 |
| `config/` | 参考配置（如 `gateway.local.toml`）。 |
| `docs/` | Runbook、密钥与部署指南。 |
| `scripts/`, `deploy/` | 本地调试、systemd、Docker 脚本。 |
| `tests/`, `crates/*/tests` | 契约/集成/性能测试。 |

## 架构要点

1. **Interceptor Chain**：Context 初始化、AuthN（Token/HMAC/OIDC）、Schema 校验、Policy/Quota、幂等、错误归一化。  
2. **Service Dispatch**：路由至工具/协作/LLM/图谱/仓库/观测处理器，背靠 `soulbase-tools`、`soulbase-llm`、`soulbase-storage`。  
3. **Storage & Registry**：`GatewayStorage` 与 `soulbase-storage` 统一 Surreal/内存实现，提供 manifest、idempotency、evidence、Repo/Graph。  
4. **Observability**：`soulbase-observe` 与 `soulbase-net` 输出日志、指标、Trace。  
5. **测试与工具**：`gateway_contracts`、`gateway_perf` 运行真实二进制；`scripts/run-gateway.sh`、`deploy/` 支持本地及生产部署。

```
HTTP Request
 └─ Interceptor Chain (Context/Auth/Schema/Policy/Error)
     └─ Service Dispatch (Tool/Collab/LLM/Graph/Repo/Observe)
         ├─ soulbase-tools + sandbox
         ├─ soulbase-llm (providers, usage, cost)
         ├─ soulbase-storage (repo/graph/observe)
         └─ soulbase-observe (metrics/logs)
```

## 快速开始

```bash
cargo fmt --all
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace

export GATEWAY_CONFIG_FILE=config/gateway.local.toml
export GATEWAY_TOKEN_LOCAL=dev-token
export GATEWAY_HMAC_LOCAL=dev-hmac
cargo run -p soulbase-gateway
```

## 测试矩阵

- 契约/集成：`cargo test -p soulbase-gateway contract_`、`cargo test -p soulbase-gateway integration_`
- 性能压测：`cargo test -p soulbase-gateway --test gateway_perf -- --ignored`

## 贡献指南

1. 阅读 `AGENTS.md` 与 `docs/runbook.md` 了解编码和运维规范。  
2. 修改前先查对应 crate 的 README。  
3. 提交前务必通过 `cargo fmt`, `cargo clippy`, `cargo test --workspace --all-features`。  
4. 使用 Conventional Commit（如 `feat: add collab contract tests`）。  

欢迎通过 Issue/PR 贡献新的薄腰能力、测试、性能优化与文档。

</div>

<div class="lang-section lang-en">

## Overview

Soul Base is the “thin-waist” layer of the Soul platform. It exposes a stable API surface for tool execution, collaboration, LLM access, graph/repo/observe services, while sharing authentication, policy, storage, and observability foundations across crates.

Main components:

- `soulbase-gateway`: Axum/Tokio gateway (HTTP/gRPC) with interceptor chain and service routing.
- `soulbase-tools`: tool registration, planning, sandbox execution, evidence and metrics.
- `soulbase-llm`: LLM provider SPI, service runtime, outbox dispatcher.
- Supporting libraries: `soulbase-auth`, `soulbase-sandbox`, `soulbase-observe`, `soulbase-config`, `soulbase-net`, `soulbase-storage`, `soulbase-tx`, etc.

Refer to [`ARCHITECTURE_OVERVIEW.md`](ARCHITECTURE_OVERVIEW.md) for dependency and split guidance.

## Repository Layout

| Path | Description |
| --- | --- |
| `crates/soulbase-gateway` | Gateway binary (tool/collab/LLM/graph/repo/observe routes + interceptor chain). |
| `crates/soulbase-tools`, `soulbase-llm`, `soulbase-interceptors` | Thin-waist protocol + runtimes (invoker, providers, auth/schema/policy stages). |
| `config/` | Reference configs (e.g., `gateway.local.toml`). |
| `docs/` | Runbooks and security/deployment guides. |
| `scripts/`, `deploy/` | Local/systemd/Docker helpers. |
| `tests/`, `crates/*/tests` | Contract, integration, and performance suites. |

## Architecture Highlights

1. **Interceptor Chain**: Context init, AuthN (token/HMAC/OIDC), schema validation, policy/quota, idempotency, error normalization.  
2. **Service Dispatch**: Routes to tool/collab/LLM/graph/repo/observe handlers backed by `soulbase-tools`, `soulbase-llm`, `soulbase-storage`.  
3. **Storage & Registry**: `GatewayStorage` + `soulbase-storage` provide Surreal/memory-backed manifests, idempotency, evidence, repo, graph.  
4. **Observability**: `soulbase-observe` and `soulbase-net` plug into tracing/logging/metrics pipelines.  
5. **Tooling & Tests**: `gateway_contracts`, `gateway_perf`, plus `scripts`/`deploy` for local/production workflows.

## Quick Start

```bash
cargo fmt --all
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace

export GATEWAY_CONFIG_FILE=config/gateway.local.toml
export GATEWAY_TOKEN_LOCAL=dev-token
export GATEWAY_HMAC_LOCAL=dev-hmac
cargo run -p soulbase-gateway
```

## Test Matrix

- Contract / integration suites: `cargo test -p soulbase-gateway contract_`, `cargo test -p soulbase-gateway integration_`
- Performance harness: `cargo test -p soulbase-gateway --test gateway_perf -- --ignored`

## Contributing

1. Read `AGENTS.md` and `docs/runbook.md` for coding/ops conventions.  
2. Review crate-specific READMEs before editing.  
3. Run `cargo fmt`, `cargo clippy`, `cargo test --workspace --all-features` before submitting.  
4. Follow Conventional Commits (e.g., `feat: add collab contract tests`).  

Contributions that improve the thin-waist API surface, testing, performance, or documentation are highly appreciated.

</div>
