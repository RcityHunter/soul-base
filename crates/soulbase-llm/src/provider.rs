use crate::{
    chat::*,
    cost::{estimate_usage, zero_cost},
    embed::*,
    errors::LlmError,
    jsonsafe::{enforce_json, StructOutPolicy},
    model::*,
    rerank::*,
};
use futures_util::stream::{self, StreamExt};
use std::collections::{BTreeSet, HashMap};

type ChatBoxStream = futures_util::stream::BoxStream<'static, Result<ChatDelta, LlmError>>;
type DynChatModel = dyn ChatModel<Stream = ChatBoxStream>;
type DynEmbedModel = dyn EmbedModel;
type DynRerankModel = dyn RerankModel;

pub struct ProviderCfg {
    pub name: String,
}

pub struct ProviderCaps {
    pub chat: bool,
    pub stream: bool,
    pub tools: bool,
    pub embeddings: bool,
    pub rerank: bool,
    pub multimodal: bool,
    pub json_schema: bool,
}

#[async_trait::async_trait]
pub trait ProviderFactory: Send + Sync {
    fn name(&self) -> &'static str;
    fn caps(&self) -> ProviderCaps;
    fn create_chat(&self, _model: &str, _cfg: &ProviderCfg) -> Option<Box<DynChatModel>> {
        None
    }
    fn create_embed(&self, _model: &str, _cfg: &ProviderCfg) -> Option<Box<DynEmbedModel>> {
        None
    }
    fn create_rerank(&self, _model: &str, _cfg: &ProviderCfg) -> Option<Box<DynRerankModel>> {
        None
    }
}

pub struct Registry {
    inner: HashMap<String, Box<dyn ProviderFactory>>,
}

impl Registry {
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }

    pub fn register(&mut self, fac: Box<dyn ProviderFactory>) {
        self.inner.insert(fac.name().to_string(), fac);
    }

    fn split_model(model_id: &str) -> Option<(&str, &str)> {
        model_id.split_once(':')
    }

    pub fn chat(&self, model_id: &str) -> Option<Box<DynChatModel>> {
        let (prov, model) = Self::split_model(model_id)?;
        let fac = self.inner.get(prov)?;
        fac.create_chat(model, &ProviderCfg { name: prov.into() })
    }

    pub fn embed(&self, model_id: &str) -> Option<Box<DynEmbedModel>> {
        let (prov, model) = Self::split_model(model_id)?;
        let fac = self.inner.get(prov)?;
        fac.create_embed(model, &ProviderCfg { name: prov.into() })
    }

    pub fn rerank(&self, model_id: &str) -> Option<Box<DynRerankModel>> {
        let (prov, model) = Self::split_model(model_id)?;
        let fac = self.inner.get(prov)?;
        fac.create_rerank(model, &ProviderCfg { name: prov.into() })
    }
}

/* ---------------------------
Local Provider (demo implementation)
--------------------------- */

pub struct LocalProviderFactory;

impl LocalProviderFactory {
    pub fn install(reg: &mut Registry) {
        reg.register(Box::new(Self));
    }
}

#[async_trait::async_trait]
impl ProviderFactory for LocalProviderFactory {
    fn name(&self) -> &'static str {
        "local"
    }

    fn caps(&self) -> ProviderCaps {
        ProviderCaps {
            chat: true,
            stream: true,
            tools: false,
            embeddings: true,
            rerank: true,
            multimodal: false,
            json_schema: true,
        }
    }

    fn create_chat(&self, _model: &str, _cfg: &ProviderCfg) -> Option<Box<DynChatModel>> {
        Some(Box::new(LocalChat))
    }

    fn create_embed(&self, _model: &str, _cfg: &ProviderCfg) -> Option<Box<DynEmbedModel>> {
        Some(Box::new(LocalEmbed))
    }

    fn create_rerank(&self, _model: &str, _cfg: &ProviderCfg) -> Option<Box<DynRerankModel>> {
        Some(Box::new(LocalRerank))
    }
}

/* Chat */

struct LocalChat;

#[async_trait::async_trait]
impl ChatModel for LocalChat {
    type Stream = ChatBoxStream;

    async fn chat(
        &self,
        req: ChatRequest,
        enforce: &StructOutPolicy,
    ) -> Result<ChatResponse, LlmError> {
        // Use the last user utterance as the echo payload.
        let mut last_user = String::new();
        for m in &req.messages {
            if matches!(m.role, Role::User) {
                for segment in &m.segments {
                    if let ContentSegment::Text { text } = segment {
                        last_user = text.clone();
                    }
                }
            }
        }

        let mut text_out = format!("echo: {}", last_user);
        if let Some(fmt) = &req.response_format {
            if matches!(fmt.kind, ResponseKind::Json | ResponseKind::JsonSchema) {
                let candidate = format!(r#"{{"echo":"{}"}}"#, last_user.replace('"', "\\\""));
                enforce_json(&candidate, enforce)?;
                text_out = candidate;
            }
        }

        let usage = estimate_usage(&[&last_user], &text_out);
        Ok(ChatResponse {
            model_id: req.model_id.clone(),
            message: Message {
                role: Role::Assistant,
                segments: vec![ContentSegment::Text {
                    text: text_out.clone(),
                }],
                tool_calls: vec![],
            },
            usage,
            cost: zero_cost(),
            finish: FinishReason::Stop,
            provider_meta: serde_json::json!({ "provider": "local" }),
        })
    }

    async fn chat_stream(
        &self,
        req: ChatRequest,
        enforce: &StructOutPolicy,
    ) -> Result<Self::Stream, LlmError> {
        let mut last_user = String::new();
        for m in &req.messages {
            if matches!(m.role, Role::User) {
                for segment in &m.segments {
                    if let ContentSegment::Text { text } = segment {
                        last_user = text.clone();
                    }
                }
            }
        }

        let first = ChatDelta {
            text_delta: Some("echo: ".into()),
            tool_call_delta: None,
            usage_partial: None,
            finish: None,
            first_token_ms: Some(10),
        };

        let make_json = |raw: String| -> Result<String, LlmError> {
            let candidate = format!(r#"{{"echo":"{}"}}"#, raw.replace('"', "\\\""));
            enforce_json(&candidate, enforce)?;
            Ok(candidate)
        };

        let second_text = if let Some(fmt) = &req.response_format {
            if matches!(fmt.kind, ResponseKind::Json | ResponseKind::JsonSchema) {
                make_json(last_user.clone())?
            } else {
                last_user.clone()
            }
        } else {
            last_user.clone()
        };

        let second = ChatDelta {
            text_delta: Some(second_text),
            tool_call_delta: None,
            usage_partial: Some(estimate_usage(&[&last_user], "")),
            finish: Some(FinishReason::Stop),
            first_token_ms: None,
        };

        Ok(stream::iter(vec![Ok(first), Ok(second)]).boxed())
    }
}

/* Embeddings */

struct LocalEmbed;

#[async_trait::async_trait]
impl EmbedModel for LocalEmbed {
    async fn embed(&self, req: EmbedRequest) -> Result<EmbedResponse, LlmError> {
        let dim = 8u32;
        let mut vectors = Vec::with_capacity(req.items.len());
        for item in &req.items {
            let mut v = vec![0.0f32; dim as usize];
            let len = v.len();
            for (idx, ch) in item.text.chars().enumerate() {
                v[idx % len] += (ch as u32 % 13) as f32 / 13.0;
            }
            if req.normalize {
                let norm = (v.iter().map(|x| x * x).sum::<f32>()).sqrt().max(1e-6);
                for value in v.iter_mut() {
                    *value /= norm;
                }
            }
            vectors.push(v);
        }

        Ok(EmbedResponse {
            dim,
            dtype: VectorDType::F32,
            vectors,
            usage: Usage {
                input_tokens: req
                    .items
                    .iter()
                    .map(|i| (i.text.len() as u32).div_ceil(4))
                    .sum(),
                output_tokens: 0,
                cached_tokens: None,
                image_units: None,
                audio_seconds: None,
                requests: 1,
            },
            cost: zero_cost(),
            provider_meta: serde_json::json!({ "provider": "local" }),
        })
    }
}

/* Rerank */

struct LocalRerank;

#[async_trait::async_trait]
impl RerankModel for LocalRerank {
    async fn rerank(&self, req: RerankRequest) -> Result<RerankResponse, LlmError> {
        fn score(query: &str, candidate: &str) -> f32 {
            let q_words: BTreeSet<_> = query.split_whitespace().collect();
            let c_words: BTreeSet<_> = candidate.split_whitespace().collect();
            let inter = q_words.intersection(&c_words).count() as f32;
            let union = (q_words.len() + c_words.len()) as f32 - inter;
            if union <= 0.0 {
                0.0
            } else {
                inter / union
            }
        }

        let mut ordering: Vec<usize> = (0..req.candidates.len()).collect();
        let scores: Vec<f32> = req
            .candidates
            .iter()
            .map(|c| score(&req.query, c))
            .collect();
        ordering.sort_by(|&a, &b| {
            scores[b]
                .partial_cmp(&scores[a])
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let scores_sorted: Vec<f32> = ordering.iter().map(|&i| scores[i]).collect();

        Ok(RerankResponse {
            scores: scores_sorted,
            ordering,
            usage: Usage {
                input_tokens: (req.query.len() as u32).div_ceil(4),
                output_tokens: 0,
                cached_tokens: None,
                image_units: None,
                audio_seconds: None,
                requests: 1,
            },
            cost: zero_cost(),
            provider_meta: serde_json::json!({ "provider": "local" }),
        })
    }
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}
