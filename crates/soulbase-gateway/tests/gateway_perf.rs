use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[path = "harness.rs"]
mod harness;

use harness::{authorized_client, GatewayProcess};
use serde_json::{json, Value};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

struct LoadStats {
    total: usize,
    success: usize,
    avg_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    throughput_rps: f64,
    failures: Vec<String>,
}

struct LoadSample {
    ok: bool,
    latency: Duration,
    detail: Option<String>,
}

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "perf"]
async fn perf_tools_execute_smoke() {
    run_perf_case(
        "/api/tools/execute",
        json!({
            "tenant": "tenant-contract",
            "tool_id": "demo.echo",
            "input": { "message": "perf" }
        }),
        "tools",
        200,
        16,
    )
    .await;
}

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "perf"]
async fn perf_collab_execute_smoke() {
    run_perf_case(
        "/api/collab/execute",
        json!({
            "tenant": "tenant-contract",
            "collab_id": "collab-perf",
            "participants": ["alice", "bob"],
            "context": { "topic": "perf" }
        }),
        "collab",
        150,
        12,
    )
    .await;
}

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "perf"]
async fn perf_llm_complete_smoke() {
    run_perf_case(
        "/api/llm/complete",
        json!({
            "tenant": "tenant-contract",
            "prompt": "perf run says hello"
        }),
        "llm",
        150,
        8,
    )
    .await;
}

async fn run_perf_case(
    route: &str,
    payload: Value,
    scope: &str,
    default_iterations: usize,
    default_concurrency: usize,
) {
    let iterations = perf_setting(
        format!("GATEWAY_PERF_ITERATIONS_{scope}"),
        default_iterations,
    );
    let concurrency = perf_setting(
        format!("GATEWAY_PERF_CONCURRENCY_{scope}"),
        default_concurrency,
    );
    let process = GatewayProcess::spawn().await;
    let client = authorized_client();
    let url = format!("{}{}", process.base_url, route);
    let stats = run_load(
        &client,
        url,
        process.token.clone(),
        payload,
        iterations,
        concurrency,
    )
    .await;

    println!(
        "[perf] route={route} total={} success={} avg_ms={:.2} p95_ms={:.2} \
         p99_ms={:.2} throughput_rps={:.2}",
        stats.total, stats.success, stats.avg_ms, stats.p95_ms, stats.p99_ms, stats.throughput_rps
    );
    if !stats.failures.is_empty() {
        println!(
            "[perf] failures sample: {}",
            stats
                .failures
                .iter()
                .take(3)
                .cloned()
                .collect::<Vec<_>>()
                .join(" | ")
        );
    }
    assert_eq!(
        stats.total, stats.success,
        "performance scenario {route} hit errors"
    );
}

async fn run_load(
    client: &reqwest::Client,
    url: String,
    token: String,
    payload: Value,
    iterations: usize,
    concurrency: usize,
) -> LoadStats {
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let mut set = JoinSet::new();
    let start_total = Instant::now();

    for _ in 0..iterations {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let client = client.clone();
        let url = url.clone();
        let token = token.clone();
        let payload = payload.clone();
        set.spawn(async move {
            let _permit = permit;
            let begin = Instant::now();
            let resp = client
                .post(url)
                .bearer_auth(token)
                .json(&payload)
                .send()
                .await;
            match resp {
                Ok(resp) => {
                    let status = resp.status();
                    let ok = status.is_success();
                    let detail = if ok {
                        None
                    } else {
                        let body = resp.text().await.unwrap_or_default();
                        Some(format!("status={} body={}", status, body))
                    };
                    LoadSample {
                        ok,
                        latency: begin.elapsed(),
                        detail,
                    }
                }
                Err(err) => LoadSample {
                    ok: false,
                    latency: begin.elapsed(),
                    detail: Some(err.to_string()),
                },
            }
        });
    }

    let mut latencies = Vec::with_capacity(iterations);
    let mut success = 0usize;
    let mut failures = Vec::new();
    while let Some(sample) = set.join_next().await {
        match sample {
            Ok(sample) => {
                if sample.ok {
                    success += 1;
                } else if let Some(detail) = sample.detail {
                    failures.push(detail);
                }
                latencies.push(sample.latency);
            }
            Err(err) => failures.push(err.to_string()),
        }
    }

    latencies.sort();
    let total = latencies.len();
    let avg_ms = if total == 0 {
        0.0
    } else {
        latencies.iter().map(Duration::as_secs_f64).sum::<f64>() * 1000.0 / total as f64
    };
    let p95_ms = percentile(&latencies, 0.95);
    let p99_ms = percentile(&latencies, 0.99);
    let elapsed = start_total.elapsed().as_secs_f64().max(0.000001);
    let throughput = success as f64 / elapsed;

    LoadStats {
        total,
        success,
        avg_ms,
        p95_ms,
        p99_ms,
        throughput_rps: throughput,
        failures,
    }
}

fn percentile(latencies: &[Duration], pct: f64) -> f64 {
    if latencies.is_empty() {
        return 0.0;
    }
    let idx = ((latencies.len() as f64 - 1.0) * pct)
        .round()
        .clamp(0.0, (latencies.len() - 1) as f64) as usize;
    latencies[idx].as_secs_f64() * 1000.0
}

fn perf_setting(key: String, default: usize) -> usize {
    env::var(&key)
        .or_else(|_| env::var(key.replace(':', "_")))
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
