# Soulbase Gateway 运维演练手册

本手册包含常规巡检、故障排查、灾备演练的操作步骤，帮助运维团队快速定位问题并恢复服务。

---

## 1. 巡检清单

| 项目 | 指令/方法 | 期望结果 |
| --- | --- | --- |
| 进程状态 | `systemctl status soulbase-gateway` 或 `docker ps` | `Active: active (running)` |
| 监听端口 | `ss -ltnp | grep 8080` | 显示网关 PID 正在监听 |
| 配置版本 | `curl http://<host>:8080/version` | 返回当前 Git SHA / 构建信息 |
| 健康检查 | `curl http://<host>:8080/health` | `{"status":"ok"}` |
| 关键指标 | `curl http://<host>:8080/metrics` | Prometheus 格式指标可访问 |
| Secret 来源 | `printenv GATEWAY_TOKEN_LOCAL` 或检查 mount | 与预期一致，不为空 |

建议将上述检查纳入日常巡检脚本。

---

## 2. 常见故障排查

### 2.1 无法启动 / Secret 缺失
1. 查看日志：`journalctl -u soulbase-gateway -n 200`.
2. 若报错 `environment variable ... not set`，检查是否通过 systemd `EnvironmentFile` 或 Docker `-e` 提供。
3. 修复后重启：`systemctl restart soulbase-gateway` 或 `docker restart`.

### 2.2 API 401 / 403
1. 确认调用方是否携带 API Key 或 Bearer Token。
2. 在 `config/gateway.local.toml` 的 `[auth.tokens]/[auth.hmac_keys]` 中检查 scopes 是否覆盖访问路由。
3. 若使用 HMAC，确保 `x-sb-date` 与 canonical string 一致。

### 2.3 存储/仓储异常
1. 查看 Graph/Repo/Observe 服务的日志（`warn!(error = ?err, ...)`）。
2. 若使用 Surreal 等外部数据库，确认连接信息与凭证有效。
3. 可运行 `cargo test -p soulbase-gateway repo_append_records_and_edges` 在本地复现逻辑。

---

## 3. 演练脚本

### 3.1 配置更新演练
```bash
# 1. 准备 manifests.json（符合 ToolManifestEntry 格式）
cat config/new-manifests.json

# 2. 生成版本号
VERSION="v$(date +%s)"

# 3. 调用配置 API
curl -X POST http://localhost:8080/api/config/tools \
  -H "Authorization: Bearer $GATEWAY_TOKEN_LOCAL" \
  -H "Content-Type: application/json" \
  -d "{
        \"version\": \"$VERSION\",
        \"manifests\": $(cat config/new-manifests.json)
      }"
```
演练完成后，可通过 `/api/config/tools` GET 验证版本列表。

### 3.2 Secret 轮换演练
1. 更新 Secret 存储（例如更新 K8s Secret 或 systemd EnvironmentFile）。
2. 重启服务，确认日志输出“auth stage”成功加载新凭证。
3. 使用旧凭证访问应返回 401/403，新凭证可正常调用。

---

## 4. 常见问题 FAQ

| 问题 | 排查步骤 |
| --- | --- |
| `tool registry version already exists` | 使用 `/api/config/tools` GET 检查版本，换一个新版本号后重发 |
| Secret 文件/环境变量更新无效 | 确认有重启服务；若需热更新，可在 roadmap 中实现定期 reload |
| Docker 中配置不生效 | 确认 `-v` 挂载了正确的 `gateway.toml`，并导出 `GATEWAY_CONFIG_FILE` |
| systemd 日志丢失 | 修改 unit 文件加入 `StandardOutput=journal`，确保 `journalctl` 可查看 |

## 5. 合同测试

用于模拟 `soulseed-agi` 请求的契约测试位于 `tests/contracts/`，通过真实二进制启动网关并调用关键 API。

运行示例：

```bash
GATEWAY_TOKEN_LOCAL=test-token \
GATEWAY_HMAC_LOCAL=test-hmac \
cargo test -p soulbase-gateway contract_
```

测试覆盖以下场景：

1. `/api/tools/execute` Echo 工具返回 `status=ok`。
2. `/api/collab/execute` 返回 `status=in_progress` 并包含协作上下文。
3. `/api/llm/complete` 返回包含原始 Prompt 的响应。

如需在 CI 中运行，请确保 8080 端口可用（或通过环境变量覆盖）。

## 6. 集成测试

在契约基础上，还提供 `integration_repo_append_creates_graph_edge`、`integration_collab_and_observe_flow` 等场景，覆盖 Repo/Graph/Observe 等跨路由流程：

```bash
GATEWAY_TOKEN_LOCAL=test-token \
GATEWAY_HMAC_LOCAL=test-hmac \
cargo test -p soulbase-gateway integration_
```

这些测试会先调用 `/api/repo/append`，再读取 `/api/graph/recall` 验证边关系；另一个场景则串联 `/api/collab/execute` 与 `/api/observe/emit`。

---

## 7. 性能与压测

`crates/soulbase-gateway/tests/gateway_perf.rs` 提供了 `perf_*` 压测场景（工具、协作、LLM）。每个场景都会启动独立的 Gateway 进程，并在 `#[ignore]` 状态下发送高并发请求，统计平均耗时、P95/P99 与吞吐等指标。

### 7.1 运行方式

```bash
# 仅执行工具压测
cargo test -p soulbase-gateway \
  --test gateway_perf \
  perf_tools_execute_smoke -- --ignored

# 或一次性跑完全部 perf_* 场景
cargo test -p soulbase-gateway --test gateway_perf -- --ignored
```

默认参数：工具 200 次请求 / 16 并发，协作 150 / 12，并发，LLM 150 / 8。可通过以下环境变量覆盖：

| 环境变量 | 说明 |
| --- | --- |
| `GATEWAY_PERF_ITERATIONS_tools` | 指定 `/api/tools/execute` 请求次数 |
| `GATEWAY_PERF_CONCURRENCY_tools` | 指定工具场景并发 |
| `GATEWAY_PERF_ITERATIONS_collab` | 协作场景请求数 |
| `GATEWAY_PERF_CONCURRENCY_collab` | 协作场景并发 |
| `GATEWAY_PERF_ITERATIONS_llm` | LLM 场景请求数 |
| `GATEWAY_PERF_CONCURRENCY_llm` | LLM 场景并发 |

测试输出示例：

```
[perf] route=/api/tools/execute total=200 success=200 avg_ms=8.12 p95_ms=10.45 p99_ms=12.20 throughput_rps=320.1
```

运行前无需额外服务依赖；Harness 会生成临时配置并注入 `GATEWAY_TOKEN_LOCAL` / `GATEWAY_HMAC_LOCAL`。若需持续压测，可将结果采集到 Prometheus 或纳入 CI，同时关注系统资源（CPU、内存、悬挂 FD）以定位瓶颈。

---

## 5. 参考
- `docs/running-gateway.md`：运行方式与环境变量。
- `docs/secrets.md`：Secret 注入方案。
- `scripts/run-gateway.sh`：本地调试脚本。
