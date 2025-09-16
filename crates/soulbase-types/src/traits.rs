use crate::{
    envelope::Envelope,
    id::{CausationId, CorrelationId},
    subject::Subject,
    time::Timestamp,
};

pub trait Versioned {
    fn schema_version(&self) -> &str;
}

pub trait Partitioned {
    fn partition_key(&self) -> &str;
}

pub trait Auditable {
    fn actor(&self) -> &Subject;
    fn produced_at(&self) -> Timestamp;
}

pub trait Causal {
    fn causation_id(&self) -> Option<&CausationId>;
    fn correlation_id(&self) -> Option<&CorrelationId>;
}

impl<T> Versioned for Envelope<T> {
    fn schema_version(&self) -> &str {
        &self.schema_ver
    }
}

impl<T> Partitioned for Envelope<T> {
    fn partition_key(&self) -> &str {
        &self.partition_key
    }
}

impl<T> Auditable for Envelope<T> {
    fn actor(&self) -> &Subject {
        &self.actor
    }

    fn produced_at(&self) -> Timestamp {
        self.produced_at
    }
}

impl<T> Causal for Envelope<T> {
    fn causation_id(&self) -> Option<&CausationId> {
        self.causation_id.as_ref()
    }

    fn correlation_id(&self) -> Option<&CorrelationId> {
        self.correlation_id.as_ref()
    }
}
