use serde::{Deserialize, Serialize};
use soulbase_types::prelude::{Id, TenantId};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct MsgId(pub String);

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum OutboxStatus {
    Pending,
    Leased { worker: String, lease_until: i64 },
    Delivered,
    Dead,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OutboxMessage {
    pub id: MsgId,
    pub tenant: TenantId,
    pub channel: String,
    pub payload: serde_json::Value,
    pub attempts: u32,
    pub status: OutboxStatus,
    pub visible_at: i64,
    pub last_error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub dispatch_key: Option<String>,
    pub envelope_id: Option<Id>,
}

impl OutboxMessage {
    pub fn new(
        tenant: TenantId,
        id: MsgId,
        channel: String,
        payload: serde_json::Value,
        now_ms: i64,
    ) -> Self {
        OutboxMessage {
            id,
            tenant,
            channel,
            payload,
            attempts: 0,
            status: OutboxStatus::Pending,
            visible_at: now_ms,
            last_error: None,
            created_at: now_ms,
            updated_at: now_ms,
            dispatch_key: None,
            envelope_id: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum DeadKind {
    Outbox,
    Saga,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct DeadLetterRef {
    pub tenant: TenantId,
    pub kind: DeadKind,
    pub id: MsgId,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeadLetter {
    pub reference: DeadLetterRef,
    pub last_error: Option<String>,
    pub stored_at: i64,
    pub note: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct IdempoKey {
    pub tenant: TenantId,
    pub key: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum IdempoState {
    InFlight {
        hash: String,
        expires_at: i64,
    },
    Succeeded {
        hash: String,
        digest: String,
    },
    Failed {
        hash: String,
        error: String,
        expires_at: i64,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SagaStepDef {
    pub name: String,
    pub action_uri: String,
    pub compensate_uri: Option<String>,
    pub timeout_ms: i64,
    pub idempotent: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SagaDefinition {
    pub name: String,
    pub steps: Vec<SagaStepDef>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum SagaState {
    Pending,
    InProgress,
    Compensating,
    Completed,
    Cancelled,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct SagaId(pub String);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SagaInstance {
    pub id: SagaId,
    pub tenant: TenantId,
    pub definition: SagaDefinition,
    pub state: SagaState,
    pub current_step: usize,
    pub history: Vec<SagaStepRecord>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SagaStepStatus {
    Pending,
    Executed,
    Compensated,
    Failed(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SagaStepRecord {
    pub name: String,
    pub status: SagaStepStatus,
    pub attempts: u32,
    pub last_updated: i64,
}

impl SagaInstance {
    pub fn new(id: SagaId, tenant: TenantId, definition: SagaDefinition, now: i64) -> Self {
        SagaInstance {
            history: definition
                .steps
                .iter()
                .map(|step| SagaStepRecord {
                    name: step.name.clone(),
                    status: SagaStepStatus::Pending,
                    attempts: 0,
                    last_updated: now,
                })
                .collect(),
            id,
            tenant,
            definition,
            state: SagaState::Pending,
            current_step: 0,
            created_at: now,
            updated_at: now,
        }
    }
}
