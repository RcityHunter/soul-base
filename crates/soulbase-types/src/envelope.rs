#[cfg(feature = "schema")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    id::{CausationId, CorrelationId, Id},
    scope::Consent,
    subject::Subject,
    time::Timestamp,
    trace::TraceContext,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
pub struct Envelope<T> {
    pub envelope_id: Id,
    pub produced_at: Timestamp,
    pub partition_key: String,
    #[serde(default)]
    pub causation_id: Option<CausationId>,
    #[serde(default)]
    pub correlation_id: Option<CorrelationId>,
    pub actor: Subject,
    #[serde(default)]
    pub consent: Option<Consent>,
    pub schema_ver: String,
    #[serde(default)]
    pub trace: Option<TraceContext>,
    pub payload: T,
}

impl<T> Envelope<T> {
    pub fn new(
        envelope_id: Id,
        produced_at: Timestamp,
        partition_key: String,
        actor: Subject,
        schema_ver: impl Into<String>,
        payload: T,
    ) -> Self {
        Self {
            envelope_id,
            produced_at,
            partition_key,
            causation_id: None,
            correlation_id: None,
            actor,
            consent: None,
            schema_ver: schema_ver.into(),
            trace: None,
            payload,
        }
    }

    pub fn with_correlation(mut self, correlation: CorrelationId) -> Self {
        self.correlation_id = Some(correlation);
        self
    }

    pub fn with_causation(mut self, causation: CausationId) -> Self {
        self.causation_id = Some(causation);
        self
    }

    pub fn with_consent(mut self, consent: Consent) -> Self {
        self.consent = Some(consent);
        self
    }

    pub fn with_trace(mut self, trace: TraceContext) -> Self {
        self.trace = Some(trace);
        self
    }
}
