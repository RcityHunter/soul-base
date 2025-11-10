use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use reqwest::Client;
use tempfile::TempDir;
use tokio::time::sleep;

const DEFAULT_CONFIG: &str = r#"
[server]
address = "127.0.0.1"
port = 0

[[services]]
name = "tool.execute"
route = "/api/tools/execute"
kind = "tool_execute"
[services.schema]
required_fields = ["tenant", "tool_id"]
[services.auth]
allow_anonymous = false
scopes = ["tools:execute"]

[[services]]
name = "collab.execute"
route = "/api/collab/execute"
kind = "collab_execute"
[services.schema]
required_fields = ["collab_id"]
[services.auth]
allow_anonymous = false
scopes = ["collab:execute"]

[[services]]
name = "llm.complete"
route = "/api/llm/complete"
kind = "llm_complete"
[services.schema]
required_fields = ["prompt"]
[services.auth]
allow_anonymous = false
scopes = ["llm:invoke"]

[tools]
[tools.registry]
kind = "in_memory"

[tools.idempotency]
enabled = false
[tools.evidence]
enabled = true
[tools.evidence.store]
kind = "memory"

[[tools.manifests]]
tenant = "tenant-contract"
id = "demo.echo"
version = "1.0.0"
display_name = "Echo Tool"
description = "Echoes input"
tags = []
input_schema = {}
output_schema = {}
scopes = []
side_effect = "None"
safety_class = "Low"
idempotency = "None"
concurrency = "Parallel"
[[tools.manifests.capabilities]]
domain = "tmp"
action = "use"
resource = "/tmp"
attrs = {}
[tools.manifests.consent]
required = false
[tools.manifests.limits]
timeout_ms = 1000
max_bytes_in = 4096
max_bytes_out = 4096
max_files = 0
max_depth = 0
max_concurrency = 1

[auth]

[[auth.tokens]]
name = "contract-cli"
token_env = "GATEWAY_TOKEN_LOCAL"
tenant = "tenant-contract"
scopes = ["tools:execute", "collab:execute", "llm:invoke"]
[auth.tokens.subject]
kind = "Service"
subject_id = "svc.contract"

[[auth.hmac_keys]]
key_id = "contract-hmac"
secret_env = "GATEWAY_HMAC_LOCAL"
tenant = "tenant-contract"
scopes = ["tools:execute"]
[auth.hmac_keys.subject]
kind = "Service"
subject_id = "hmac.contract"

[[services]]
name = "repo.append"
route = "/api/repo/append"
kind = "repo_append"
[services.schema]
required_fields = ["repo", "data"]
[services.auth]
allow_anonymous = false
scopes = ["repo:write"]

[[services]]
name = "graph.recall"
route = "/api/graph/recall"
kind = "graph_recall"
[services.schema]
required_fields = ["node_id"]
[services.auth]
allow_anonymous = false
scopes = ["graph:read"]

[[services]]
name = "observe.emit"
route = "/api/observe/emit"
kind = "observe_emit"
[services.schema]
required_fields = ["topic", "event"]
[services.auth]
allow_anonymous = false
scopes = ["observe:emit"]

[storage.repo]
kind = "memory"

[storage.observe]
kind = "memory"

[storage.graph]
kind = "memory"

[policy]
[[policy.rules]]
id = "allow-all"
description = "Allow contract tests"
resources = ["route:*"]
actions = []
effect = "Allow"
"#;

pub struct GatewayProcess {
    child: Child,
    pub base_url: String,
    pub token: String,
    _dir: TempDir,
}

impl GatewayProcess {
    pub async fn spawn() -> Self {
        Self::spawn_with_config(DEFAULT_CONFIG).await
    }

    pub async fn spawn_with_config(config: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test port");
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let tmp_dir = TempDir::new().expect("temp dir");
        let config_path = write_config(tmp_dir.path(), config);

        let token = format!("contract-token-{port}");
        let hmac = format!("contract-hmac-{port}");

        let mut child = Command::new(env!("CARGO_BIN_EXE_soulbase-gateway"))
            .env("GATEWAY_CONFIG_FILE", &config_path)
            .env("GATEWAY_TOKEN_LOCAL", &token)
            .env("GATEWAY_HMAC_LOCAL", &hmac)
            .env("GATEWAY__SERVER__ADDRESS", "127.0.0.1")
            .env("GATEWAY__SERVER__PORT", port.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn gateway process");

        let base_url = format!("http://127.0.0.1:{port}");
        wait_for_ready(&base_url, &mut child).await;

        Self {
            child,
            base_url,
            token,
            _dir: tmp_dir,
        }
    }
}

impl Drop for GatewayProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

pub fn authorized_client() -> Client {
    Client::new()
}

fn write_config(dir: &Path, contents: &str) -> PathBuf {
    let path = dir.join("gateway.toml");
    std::fs::write(&path, contents).expect("write config");
    path
}

async fn wait_for_ready(base_url: &str, child: &mut Child) {
    let client = Client::new();
    for _ in 0..100 {
        if let Some(status) = child.try_wait().expect("check gateway child status") {
            panic!("gateway process exited early with status {status}");
        }
        if let Ok(resp) = client.get(format!("{base_url}/health")).send().await {
            if resp.status().is_success() {
                return;
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
    panic!("gateway did not become ready at {base_url}");
}
