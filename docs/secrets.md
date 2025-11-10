# 安全与凭证管理

Gateway 现支持通过环境变量或文件来源注入敏感凭证，避免将 API Token、HMAC Secret 等写入配置文件。

## 1. `auth.tokens` / `auth.hmac_keys`

每个 token/密钥条目都支持以下字段（按优先顺序读取）：

| 字段 | 说明 |
| --- | --- |
| `token` / `secret` | 直接在配置文件中填写的明文值（不推荐在生产使用） |
| `token_env` / `secret_env` | 指向环境变量名称，若设置则优先使用环境变量值 |
| `token_file` / `secret_file` | 指向包含秘密内容的文件路径，读取并 trim 后使用 |

示例（参见 `config/gateway.local.toml`）：

```toml
[[auth.tokens]]
name = "local-cli"
token_env = "GATEWAY_TOKEN_LOCAL"
tenant = "tenant-demo"
scopes = ["graph:read"]

[[auth.hmac_keys]]
key_id = "local-hmac"
secret_file = "/etc/soulbase/secrets/local-hmac"
tenant = "tenant-demo"
scopes = ["repo:write"]
```

部署时只需设置环境变量或挂载 Secret 文件，例如：

```bash
export GATEWAY_TOKEN_LOCAL="super-token"
echo "hmac-secret" > /etc/soulbase/secrets/local-hmac
```

若既未提供 literal、也没有 `*_env`/`*_file`，进程将在启动时返回错误。

## 2. 推荐实践

1. **本地开发**：可继续使用 `token` 字段，但建议配合 `.env` 或 shell 变量覆盖。
2. **Docker/K8s**：利用 `docker run -e GATEWAY_TOKEN_LOCAL=...` 或 `kubectl create secret`，容器内再通过 `secret_env`/`secret_file` 引用。
3. **systemd**：在 unit 文件中加入 `Environment=GATEWAY_TOKEN_LOCAL=...`，或使用 `EnvironmentFile=/etc/default/soulbase-gateway`。
4. **Vault 等外部管理器**：编写 sidecar/同步脚本，将凭证写入挂载文件，然后配置 `token_file`/`secret_file` 指向该路径。

## 3. 轮换与审计

- 当环境变量或文件内容更新后，重启 Gateway 即可加载新密钥；如需热更新，可结合 future roadmap 在 `GatewayAuthStage` 引入动态刷新。
- 建议在 CI/CD 或部署脚本中记录当前凭证版本（例如 `config:manage` scope），并使用 `docs/running-gateway.md` 中的 systemd/Docker 方案统一管理。
