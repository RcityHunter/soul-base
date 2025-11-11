#![cfg(feature = "surreal")]

use crate::model::{
    DeadKind, DeadLetter, DeadLetterRef, IdempoState, MsgId, OutboxMessage, OutboxStatus,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use soulbase_types::prelude::TenantId;

#[derive(Serialize, Deserialize)]
struct OutboxRecord {
    msg_id: String,
    tenant: String,
    channel: String,
    payload: Value,
    attempts: u32,
    status: String,
    visible_at: i64,
    lease_worker: Option<String>,
    lease_until: Option<i64>,
    last_error: Option<String>,
    dispatch_key: Option<String>,
    envelope_id: Option<String>,
    created_at: i64,
    updated_at: i64,
}

#[derive(Serialize, Deserialize)]
struct DeadLetterRecord {
    letter_id: String,
    tenant: String,
    kind: String,
    last_error: Option<String>,
    note: Option<String>,
    stored_at: i64,
}

#[derive(Serialize, Deserialize)]
pub struct IdempoRecord {
    pub tenant: String,
    pub key: String,
    pub hash: String,
    pub state: String,
    pub digest: Option<String>,
    pub error: Option<String>,
    pub expires_at: Option<i64>,
    pub updated_at: i64,
}

pub fn encode_outbox(message: &OutboxMessage) -> Value {
    let (status, lease_worker, lease_until) = encode_status(&message.status);
    json!({
        "msg_id": message.id.0,
        "tenant": message.tenant.0,
        "channel": message.channel,
        "payload": message.payload,
        "attempts": message.attempts,
        "status": status,
        "visible_at": message.visible_at,
        "lease_worker": lease_worker,
        "lease_until": lease_until,
        "last_error": message.last_error,
        "dispatch_key": message.dispatch_key,
        "envelope_id": message.envelope_id.as_ref().map(|id| id.0.clone()),
        "created_at": message.created_at,
        "updated_at": message.updated_at,
    })
}

pub fn decode_outbox_list(value: Value) -> Result<Vec<OutboxMessage>, serde_json::Error> {
    match value {
        Value::Array(items) => items
            .into_iter()
            .filter(|v| !v.is_null())
            .map(decode_outbox)
            .collect(),
        Value::Null => Ok(Vec::new()),
        other => decode_outbox(other).map(|msg| vec![msg]),
    }
}

pub fn decode_outbox(value: Value) -> Result<OutboxMessage, serde_json::Error> {
    let record: OutboxRecord = serde_json::from_value(value)?;
    let status = decode_status(
        &record.status,
        record.lease_worker.clone(),
        record.lease_until,
    );
    let envelope_id = record
        .envelope_id
        .filter(|s| !s.is_empty())
        .map(soulbase_types::prelude::Id);
    Ok(OutboxMessage {
        id: MsgId(record.msg_id),
        tenant: TenantId(record.tenant),
        channel: record.channel,
        payload: record.payload,
        attempts: record.attempts,
        status,
        visible_at: record.visible_at,
        last_error: record.last_error,
        created_at: record.created_at,
        updated_at: record.updated_at,
        dispatch_key: record.dispatch_key,
        envelope_id,
    })
}

pub fn encode_dead(letter: &DeadLetter) -> Value {
    json!({
        "letter_id": letter.reference.id.0,
        "tenant": letter.reference.tenant.0,
        "kind": encode_dead_kind(&letter.reference.kind),
        "last_error": letter.last_error,
        "note": letter.note,
        "stored_at": letter.stored_at,
    })
}

pub fn decode_dead_list(value: Value) -> Result<Vec<DeadLetter>, serde_json::Error> {
    match value {
        Value::Array(items) => items
            .into_iter()
            .filter(|v| !v.is_null())
            .map(decode_dead)
            .collect(),
        Value::Null => Ok(Vec::new()),
        other => decode_dead(other).map(|msg| vec![msg]),
    }
}

pub fn decode_dead(value: Value) -> Result<DeadLetter, serde_json::Error> {
    let record: DeadLetterRecord = serde_json::from_value(value)?;
    Ok(DeadLetter {
        reference: DeadLetterRef {
            tenant: TenantId(record.tenant),
            kind: decode_dead_kind(&record.kind),
            id: MsgId(record.letter_id),
        },
        last_error: record.last_error,
        stored_at: record.stored_at,
        note: record.note,
    })
}

pub fn encode_dead_ref(reference: &DeadLetterRef) -> Value {
    json!({
        "letter_id": reference.id.0,
        "tenant": reference.tenant.0,
        "kind": encode_dead_kind(&reference.kind),
    })
}

pub fn encode_idempo_record(
    tenant: &TenantId,
    key: &str,
    state: IdempoState,
    now_ms: i64,
) -> Value {
    let (status, hash, digest, error, expires_at) = encode_idempo_state(&state);
    json!({
        "tenant": tenant.0,
        "key": key,
        "hash": hash,
        "state": status,
        "digest": digest,
        "error": error,
        "expires_at": expires_at,
        "updated_at": now_ms,
    })
}

pub fn decode_idempo(value: Value) -> Result<IdempoRecord, serde_json::Error> {
    serde_json::from_value(value)
}

pub fn idempo_state_from_record(record: &IdempoRecord) -> IdempoState {
    match record.state.as_str() {
        "InFlight" => IdempoState::InFlight {
            hash: record.hash.clone(),
            expires_at: record.expires_at.unwrap_or_default(),
        },
        "Succeeded" => IdempoState::Succeeded {
            hash: record.hash.clone(),
            digest: record.digest.clone().unwrap_or_default(),
        },
        "Failed" => IdempoState::Failed {
            hash: record.hash.clone(),
            error: record.error.clone().unwrap_or_default(),
            expires_at: record.expires_at.unwrap_or_default(),
        },
        _ => IdempoState::Failed {
            hash: record.hash.clone(),
            error: record.error.clone().unwrap_or_default(),
            expires_at: record.expires_at.unwrap_or_default(),
        },
    }
}

fn encode_status(status: &OutboxStatus) -> (String, Option<String>, Option<i64>) {
    match status {
        OutboxStatus::Pending => ("Pending".to_string(), None, None),
        OutboxStatus::Leased {
            worker,
            lease_until,
        } => (
            "Leased".to_string(),
            Some(worker.clone()),
            Some(*lease_until),
        ),
        OutboxStatus::Delivered => ("Delivered".to_string(), None, None),
        OutboxStatus::Dead => ("Dead".to_string(), None, None),
    }
}

fn decode_status(status: &str, worker: Option<String>, lease_until: Option<i64>) -> OutboxStatus {
    match status {
        "Pending" => OutboxStatus::Pending,
        "Leased" => {
            if let (Some(worker), Some(until)) = (worker, lease_until) {
                OutboxStatus::Leased {
                    worker,
                    lease_until: until,
                }
            } else {
                OutboxStatus::Pending
            }
        }
        "Delivered" => OutboxStatus::Delivered,
        "Dead" => OutboxStatus::Dead,
        other => {
            tracing::warn!(target = "soulbase::tx::surreal", status = %other, "unknown outbox status");
            OutboxStatus::Pending
        }
    }
}

pub fn encode_dead_kind(kind: &DeadKind) -> String {
    match kind {
        DeadKind::Outbox => "Outbox".to_string(),
        DeadKind::Saga => "Saga".to_string(),
    }
}

pub fn decode_dead_kind(kind: &str) -> DeadKind {
    match kind {
        "Saga" => DeadKind::Saga,
        _ => DeadKind::Outbox,
    }
}

fn encode_idempo_state(
    state: &IdempoState,
) -> (&str, String, Option<String>, Option<String>, Option<i64>) {
    match state {
        IdempoState::InFlight { hash, expires_at } => {
            ("InFlight", hash.clone(), None, None, Some(*expires_at))
        }
        IdempoState::Succeeded { hash, digest } => {
            ("Succeeded", hash.clone(), Some(digest.clone()), None, None)
        }
        IdempoState::Failed {
            hash,
            error,
            expires_at,
        } => (
            "Failed",
            hash.clone(),
            None,
            Some(error.clone()),
            Some(*expires_at),
        ),
    }
}
