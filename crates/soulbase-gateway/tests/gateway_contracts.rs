#[path = "harness.rs"]
mod harness;

use harness::{authorized_client, GatewayProcess};
use serde_json::{json, Value};
use uuid::Uuid;

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn contract_tool_execute_echo() {
    let process = GatewayProcess::spawn().await;
    let client = authorized_client();
    let resp = client
        .post(format!("{}/api/tools/execute", process.base_url))
        .bearer_auth(&process.token)
        .json(&json!({
            "tenant": "tenant-contract",
            "tool_id": "demo.echo",
            "input": { "message": "hello" }
        }))
        .send()
        .await
        .expect("tool execute response");
    assert!(resp.status().is_success());
    let body: Value = resp.json().await.expect("json body");
    assert_eq!(body["status"], "ok");
}

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn contract_collab_execute() {
    let process = GatewayProcess::spawn().await;
    let client = authorized_client();
    let resp = client
        .post(format!("{}/api/collab/execute", process.base_url))
        .bearer_auth(&process.token)
        .json(&json!({
            "tenant": "tenant-contract",
            "collab_id": "collab-test",
            "participants": ["alice"],
            "context": {"topic": "demo"}
        }))
        .send()
        .await
        .expect("collab execute response");
    assert!(resp.status().is_success());
    let body: Value = resp.json().await.expect("json body");
    assert_eq!(body["status"], "in_progress");
    assert_eq!(body["collab_id"], "collab-test");
}

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn contract_llm_complete() {
    let process = GatewayProcess::spawn().await;
    let client = authorized_client();
    let resp = client
        .post(format!("{}/api/llm/complete", process.base_url))
        .bearer_auth(&process.token)
        .json(&json!({
            "tenant": "tenant-contract",
            "prompt": "contract hello"
        }))
        .send()
        .await
        .expect("llm response");
    assert!(resp.status().is_success());
    let body: Value = resp.json().await.expect("json body");
    assert!(body["response"]
        .as_str()
        .expect("response string")
        .contains("contract hello"));
}

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_repo_append_creates_graph_edge() {
    let process = GatewayProcess::spawn().await;
    let client = authorized_client();
    let record_id = format!("repo-item-{}", Uuid::new_v4());
    client
        .post(format!("{}/api/repo/append", process.base_url))
        .bearer_auth(&process.token)
        .json(&json!({
            "tenant": "tenant-contract",
            "repo": "timeline",
            "record_id": record_id,
            "data": { "summary": "integration" },
            "edges": [{
                "to": "doc:integration",
                "label": "follows",
                "props": { "weight": 0.7 }
            }]
        }))
        .send()
        .await
        .expect("repo append resp")
        .error_for_status()
        .expect("repo append ok");

    let graph_resp = client
        .post(format!("{}/api/graph/recall", process.base_url))
        .bearer_auth(&process.token)
        .json(&json!({
            "tenant": "tenant-contract",
            "node_id": record_id,
            "label": "follows",
            "limit": 10
        }))
        .send()
        .await
        .expect("graph resp")
        .error_for_status()
        .expect("graph ok");
    let body: Value = graph_resp.json().await.expect("graph json");
    let edges = body["edges"].as_array().expect("edges array");
    assert!(
        edges.iter().any(|edge| edge["to"] == "doc:integration"),
        "expected integration edge, body={body}"
    );
}

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_collab_and_observe_flow() {
    let process = GatewayProcess::spawn().await;
    let client = authorized_client();
    let collab_resp = client
        .post(format!("{}/api/collab/execute", process.base_url))
        .bearer_auth(&process.token)
        .json(&json!({
            "tenant": "tenant-contract",
            "collab_id": "collab-integration",
            "participants": ["alice"],
            "context": {"topic": "integration"}
        }))
        .send()
        .await
        .expect("collab resp")
        .error_for_status()
        .expect("collab ok");
    let collab_body: Value = collab_resp.json().await.expect("collab json");
    assert_eq!(collab_body["collab_id"], "collab-integration");

    client
        .post(format!("{}/api/observe/emit", process.base_url))
        .bearer_auth(&process.token)
        .json(&json!({
            "tenant": "tenant-contract",
            "topic": "integration.topic",
            "event": { "collab": "collab-integration" }
        }))
        .send()
        .await
        .expect("observe resp")
        .error_for_status()
        .expect("observe ok");
}
