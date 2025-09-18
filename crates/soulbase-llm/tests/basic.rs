use futures_util::StreamExt;
use soulbase_llm::prelude::*;

#[tokio::test]
async fn chat_sync_and_stream_consistency() {
    // 注册本地 Provider
    let mut reg = Registry::new();
    LocalProviderFactory::install(&mut reg);

    let chat = reg.chat("local:echo").expect("chat model");
    let req = ChatRequest {
        model_id: "local:echo".into(),
        messages: vec![
            Message {
                role: Role::System,
                segments: vec![ContentSegment::Text {
                    text: "You are echo.".into(),
                }],
                tool_calls: vec![],
            },
            Message {
                role: Role::User,
                segments: vec![ContentSegment::Text {
                    text: "hello".into(),
                }],
                tool_calls: vec![],
            },
        ],
        tool_specs: vec![],
        temperature: None,
        top_p: None,
        max_tokens: None,
        stop: vec![],
        seed: None,
        frequency_penalty: None,
        presence_penalty: None,
        logit_bias: Default::default(),
        response_format: None,
        idempotency_key: None,
        cache_hint: None,
        allow_sensitive: false,
        metadata: serde_json::json!({}),
    };

    // 同步
    let sync = chat
        .chat(req.clone(), &StructOutPolicy::Off)
        .await
        .expect("chat");
    let text_sync = match &sync.message.segments[0] {
        ContentSegment::Text { text } => text.clone(),
        _ => "".into(),
    };
    assert!(text_sync.starts_with("echo: "));

    // 流式
    let mut stream = chat
        .chat_stream(req, &StructOutPolicy::Off)
        .await
        .expect("stream");
    let mut concat = String::new();
    while let Some(delta) = stream.next().await {
        let d = delta.expect("delta ok");
        if let Some(t) = d.text_delta {
            concat.push_str(&t);
        }
    }
    assert_eq!(concat, text_sync);
}

#[tokio::test]
async fn chat_json_struct_out_validation() {
    let mut reg = Registry::new();
    LocalProviderFactory::install(&mut reg);
    let chat = reg.chat("local:echo").expect("chat model");

    let req = ChatRequest {
        model_id: "local:echo".into(),
        messages: vec![Message {
            role: Role::User,
            segments: vec![ContentSegment::Text { text: "hi".into() }],
            tool_calls: vec![],
        }],
        tool_specs: vec![],
        temperature: None,
        top_p: None,
        max_tokens: None,
        stop: vec![],
        seed: None,
        frequency_penalty: None,
        presence_penalty: None,
        logit_bias: Default::default(),
        response_format: Some(ResponseFormat {
            kind: ResponseKind::Json,
            json_schema: None,
            strict: true,
        }),
        idempotency_key: None,
        cache_hint: None,
        allow_sensitive: false,
        metadata: serde_json::json!({}),
    };

    let resp = chat
        .chat(req, &StructOutPolicy::StrictReject)
        .await
        .expect("ok");
    let seg = &resp.message.segments[0];
    let s = match seg {
        ContentSegment::Text { text } => text.clone(),
        _ => "".into(),
    };
    let v: serde_json::Value = serde_json::from_str(&s).expect("valid json");
    assert_eq!(v["echo"], "hi");
}

#[tokio::test]
async fn embeddings_and_rerank_work() {
    let mut reg = Registry::new();
    LocalProviderFactory::install(&mut reg);
    let emb = reg.embed("local:emb").expect("embed");
    let out = emb
        .embed(EmbedRequest {
            model_id: "local:emb".into(),
            items: vec![
                EmbedItem {
                    id: "a".into(),
                    text: "the cat sat".into(),
                },
                EmbedItem {
                    id: "b".into(),
                    text: "cat on mat".into(),
                },
            ],
            normalize: true,
            pooling: None,
        })
        .await
        .expect("embed ok");
    assert_eq!(out.dim, 8);
    assert_eq!(out.vectors.len(), 2);
    assert_eq!(out.dtype, VectorDType::F32);

    let rer = reg.rerank("local:rerank").expect("rerank");
    let rr = rer
        .rerank(RerankRequest {
            model_id: "local:rerank".into(),
            query: "cat mat".into(),
            candidates: vec!["the cat sat".into(), "cat on mat".into()],
        })
        .await
        .expect("rerank ok");
    assert_eq!(rr.ordering[0], 1); // 第二句与 query 更接近
}
