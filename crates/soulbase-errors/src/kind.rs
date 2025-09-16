#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ErrorKind {
    Auth,
    Quota,
    Schema,
    PolicyDeny,
    Sandbox,
    Provider,
    Storage,
    Timeout,
    Conflict,
    NotFound,
    Precondition,
    Serialization,
    Network,
    RateLimit,
    QosBudgetExceeded,
    ToolError,
    LlmError,
    A2AError,
    Unknown,
}

impl ErrorKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            ErrorKind::Auth => "Auth",
            ErrorKind::Quota => "Quota",
            ErrorKind::Schema => "Schema",
            ErrorKind::PolicyDeny => "PolicyDeny",
            ErrorKind::Sandbox => "Sandbox",
            ErrorKind::Provider => "Provider",
            ErrorKind::Storage => "Storage",
            ErrorKind::Timeout => "Timeout",
            ErrorKind::Conflict => "Conflict",
            ErrorKind::NotFound => "NotFound",
            ErrorKind::Precondition => "Precondition",
            ErrorKind::Serialization => "Serialization",
            ErrorKind::Network => "Network",
            ErrorKind::RateLimit => "RateLimit",
            ErrorKind::QosBudgetExceeded => "QosBudgetExceeded",
            ErrorKind::ToolError => "ToolError",
            ErrorKind::LlmError => "LlmError",
            ErrorKind::A2AError => "A2AError",
            ErrorKind::Unknown => "Unknown",
        }
    }
}
