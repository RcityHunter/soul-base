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
