#[cfg(feature = "provider-openai")]
use futures_util::StreamExt;
#[cfg(feature = "provider-openai")]
use soulbase_llm::prelude::*;
#[cfg(feature = "provider-openai")]
use std::error::Error;

#[cfg(feature = "provider-openai")]
fn build_request(model_id: &str, prompt: &str) -> ChatRequest {
    ChatRequest {
        model_id: model_id.to_string(),
        messages: vec![
            Message {
                role: Role::System,
                segments: vec![ContentSegment::Text {
                    text: "You are a concise assistant.".into(),
                }],
                tool_calls: Vec::new(),
            },
            Message {
                role: Role::User,
                segments: vec![ContentSegment::Text {
                    text: prompt.to_string(),
                }],
                tool_calls: Vec::new(),
            },
        ],
        tool_specs: Vec::new(),
        temperature: Some(0.3),
        top_p: None,
        max_tokens: Some(128),
        stop: Vec::new(),
        seed: None,
        frequency_penalty: None,
        presence_penalty: None,
        logit_bias: serde_json::Map::new(),
        response_format: None,
        idempotency_key: None,
        cache_hint: None,
        allow_sensitive: false,
        metadata: serde_json::Value::Null,
    }
}

#[cfg(feature = "provider-openai")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let api_key = std::env::var("OPENAI_API_KEY")
        .map_err(|_| "set OPENAI_API_KEY in your environment before running this example")?;

    let cfg = OpenAiConfig::new(api_key)?
        // 使用别名方便调用时指定模型
        .with_alias("thin-waist", "gpt-4o-mini")?;
    let factory = OpenAiProviderFactory::new(cfg)?;

    let mut registry = Registry::new();
    factory.install(&mut registry);

    let model = registry
        .chat("openai:thin-waist")
        .expect("openai provider registered");

    println!("--- synchronous chat ---");
    let request = build_request("openai:thin-waist", "请用一句话介绍 Soulbase。");
    let response = model
        .chat(request, &StructOutPolicy::Off)
        .await
        .expect("chat response");
    println!(
        "assistant: {}",
        response.message.segments[0]
            .as_text()
            .unwrap_or("<non-text>")
    );
    println!(
        "usage: in={} out={}",
        response.usage.input_tokens, response.usage.output_tokens
    );

    println!("\n--- streaming chat ---");
    let stream_request = build_request(
        "openai:thin-waist",
        "请逐步列出 Soulbase 的核心组件，并在最后总结。",
    );
    let mut stream = model
        .chat_stream(stream_request, &StructOutPolicy::Off)
        .await
        .expect("stream established");

    while let Some(delta) = stream.next().await {
        let delta = delta?;
        if let Some(text) = delta.text_delta {
            print!("{text}");
        }
        if let Some(call) = delta.tool_call_delta {
            println!("\n[tool-call] {} -> {}", call.call_id.0, call.name);
        }
        if let Some(usage) = delta.usage_partial {
            println!(
                "\n[usage] in={} out={} finish={:?}",
                usage.input_tokens, usage.output_tokens, delta.finish
            );
        }
    }

    Ok(())
}

#[cfg(feature = "provider-openai")]
trait SegmentExt {
    fn as_text(&self) -> Option<&str>;
}

#[cfg(feature = "provider-openai")]
impl SegmentExt for ContentSegment {
    fn as_text(&self) -> Option<&str> {
        match self {
            ContentSegment::Text { text } => Some(text),
            _ => None,
        }
    }
}

#[cfg(not(feature = "provider-openai"))]
fn main() {
    eprintln!("Enable the `provider-openai` feature to run this example");
}
