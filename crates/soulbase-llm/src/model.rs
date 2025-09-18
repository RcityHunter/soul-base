use serde::{Deserialize, Serialize};
use soulbase_types::prelude::*;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum Role {
    System,
    User,
    Assistant,
    Tool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum ContentSegment {
    Text {
        text: String,
    },
    ImageRef {
        uri: String,
        mime: String,
        width: Option<u32>,
        height: Option<u32>,
    },
    AudioRef {
        uri: String,
        seconds: f32,
        sample_rate: Option<u32>,
        mime: Option<String>,
    },
    AttachmentRef {
        uri: String,
        bytes: Option<u64>,
        mime: Option<String>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ToolCallProposal {
    pub name: String,
    pub call_id: Id,
    pub arguments: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Message {
    pub role: Role,
    #[serde(default)]
    pub segments: Vec<ContentSegment>,
    #[serde(default)]
    pub tool_calls: Vec<ToolCallProposal>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Usage {
    pub input_tokens: u32,
    pub output_tokens: u32,
    #[serde(default)]
    pub cached_tokens: Option<u32>,
    #[serde(default)]
    pub image_units: Option<u32>,
    #[serde(default)]
    pub audio_seconds: Option<f32>,
    pub requests: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CostBreakdown {
    pub input: f32,
    pub output: f32,
    pub image: f32,
    pub audio: f32,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct Cost {
    pub usd: f32,
    pub currency: &'static str,
    pub breakdown: CostBreakdown,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum FinishReason {
    Stop,
    Length,
    Tool,
    Safety,
    Other(String),
}
