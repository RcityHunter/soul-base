use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use soulbase_net::prelude::*;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use url::Url;

type ResBody = Full<Bytes>;

async fn start_mock<F>(port: u16, handler: F) -> JoinHandle<()>
where
    F: Fn(Request<Incoming>) -> Response<ResBody> + Send + Sync + 'static,
{
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let listener = TcpListener::bind(addr).await.expect("bind mock");
    let handler = Arc::new(handler);

    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.expect("accept");
            let handler = handler.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let service = service_fn(move |req: Request<Incoming>| {
                    let handler = handler.clone();
                    async move { Ok::<_, hyper::Error>((handler)(req)) }
                });

                if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
                    eprintln!("mock server error: {err}");
                }
            });
        }
    })
}

fn bytes_body(content: &str) -> ResBody {
    Full::new(Bytes::from(content.as_bytes().to_vec()))
}

#[tokio::test]
async fn retry_succeeds_on_5xx_then_ok() {
    static HITS: AtomicUsize = AtomicUsize::new(0);
    let _server = start_mock(18080, |_req| {
        if HITS.fetch_add(1, Ordering::SeqCst) == 0 {
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(bytes_body("fail"))
                .unwrap()
        } else {
            Response::builder()
                .status(StatusCode::OK)
                .body(bytes_body("{\"ok\":true}"))
                .unwrap()
        }
    })
    .await;
    tokio::task::yield_now().await;

    let mut policy = NetPolicy::default();
    policy.security.deny_private = false;
    policy.retry.max_attempts = 3;

    let client = ClientBuilder::default()
        .with_policy(policy.clone())
        .with_interceptor(TraceUa::default())
        .with_interceptor(SandboxGuard {
            policy: policy.security.clone(),
        })
        .build()
        .expect("client");

    let mut request = NetRequest::default();
    request.method = http::Method::GET;
    request.url = Url::parse("http://127.0.0.1:18080/ok").unwrap();

    let response = client.send(request).await.expect("retry success");
    assert_eq!(response.status, StatusCode::OK);
}

#[tokio::test]
async fn circuit_breaker_opens_after_failures() {
    static FAILS: AtomicUsize = AtomicUsize::new(0);
    let _server = start_mock(18081, |_req| {
        FAILS.fetch_add(1, Ordering::SeqCst);
        Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(bytes_body("boom"))
            .unwrap()
    })
    .await;
    tokio::task::yield_now().await;

    let mut policy = NetPolicy::default();
    policy.security.deny_private = false;
    policy.retry.enabled = false;
    policy.cbreaker.min_samples = 3;
    policy.cbreaker.failure_ratio = 0.5;
    policy.cbreaker.open_for = std::time::Duration::from_millis(200);

    let client = ClientBuilder::default()
        .with_policy(policy.clone())
        .with_interceptor(TraceUa::default())
        .with_interceptor(SandboxGuard {
            policy: policy.security.clone(),
        })
        .build()
        .expect("client");

    for _ in 0..3 {
        let mut request = NetRequest::default();
        request.method = http::Method::GET;
        request.url = Url::parse("http://127.0.0.1:18081/fail").unwrap();
        let _ = client.send(request).await.err().expect("failure expected");
    }

    let mut request = NetRequest::default();
    request.method = http::Method::GET;
    request.url = Url::parse("http://127.0.0.1:18081/fail").unwrap();
    let err = client.send(request).await.err().expect("circuit open");
    assert!(format!("{err}").contains("Upstream unavailable"));
}

#[tokio::test]
async fn sandbox_guard_denies_private_when_not_allowed() {
    let policy = NetPolicy::default();
    let client = ClientBuilder::default()
        .with_policy(policy.clone())
        .with_interceptor(TraceUa::default())
        .with_interceptor(SandboxGuard {
            policy: policy.security.clone(),
        })
        .build()
        .expect("client");

    let mut request = NetRequest::default();
    request.method = http::Method::GET;
    request.url = Url::parse("http://127.0.0.1:1/").unwrap();
    let err = client.send(request).await.err().expect("sandbox blocked");
    assert!(format!("{err}").contains("private network"));
}
