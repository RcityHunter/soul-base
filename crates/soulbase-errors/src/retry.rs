#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RetryClass {
    None,
    Transient,
    Permanent,
}

impl RetryClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            RetryClass::None => "none",
            RetryClass::Transient => "transient",
            RetryClass::Permanent => "permanent",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BackoffHint {
    pub initial_ms: u64,
    pub max_ms: u64,
}

impl BackoffHint {
    pub const fn new(initial_ms: u64, max_ms: u64) -> Self {
        Self { initial_ms, max_ms }
    }
}
