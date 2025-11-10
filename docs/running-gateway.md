# Soulbase Gateway 启动指南

本指南描述如何在本地开发、Docker 和生产 (systemd) 下运行 `soulbase-gateway`。

## 1. 本地开发脚本

```bash
scripts/run-gateway.sh
```

- 默认使用 `config/gateway.local.toml`，可通过 `GATEWAY_CONFIG_FILE` 覆盖。
- 支持传入附加参数，例如 `scripts/run-gateway.sh --help`。
- 运行前确保数据库/依赖服务已就绪，必要时更新配置中的路由和凭证。

## 2. Docker 镜像

仓库根目录提供 `Dockerfile`，构建并运行示例：

```bash
docker build -t soulbase-gateway .
docker run --rm -p 8080:8080 \
  -e GATEWAY_CONFIG_FILE=/etc/soulbase/gateway.toml \
  -v $(pwd)/config/gateway.local.toml:/etc/soulbase/gateway.toml \
  soulbase-gateway
```

可根据环境挂载不同配置文件，或通过 `ENV` 注入敏感信息。

## 3. systemd 服务

`deploy/systemd/soulbase-gateway.service` 演示如何以 systemd 管理进程：

1. 将可执行文件与配置部署至 `/opt/soul-base`、`/etc/soulbase/gateway.toml`。
2. 根据需要创建 `soulbase` 用户与用户组。
3. 复制 unit 文件到 `/etc/systemd/system/`，更新 `WorkingDirectory`/`ExecStart`.
4. 运行：
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable --now soulbase-gateway
   sudo systemctl status soulbase-gateway
   ```

- `GATEWAY_CONFIG_FILE`、`RUST_LOG` 等环境变量可在 unit 中调整。
- 建议在 CI/CD 中先运行 `cargo build --release -p soulbase-gateway` 并将生成的二进制复制到 `ExecStart` 指向的路径。

## 4. 常用环境变量

| 变量 | 说明 | 默认值 |
| --- | --- | --- |
| `GATEWAY_CONFIG_FILE` | 网关配置文件路径 | `config/gateway.local.toml` |
| `RUST_LOG` | 日志级别 | `info` |
| `GATEWAY_API_KEY` | 默认 API Key（可选） | 空 |

更多关于 API Token/HMAC 秘密注入方式，参见 `docs/secrets.md`；故障排查与演练脚本见 `docs/runbook.md`。

根据部署环境调整其他依赖服务的地址、凭证等配置项。
