use crate::model::{LogEvent, MetricSpec};
use crate::sdk::evidence::EvidenceEvent;
use crate::{ctx::ObserveCtx, ObserveError};

#[derive(Clone, Debug)]
pub enum RouteDecision {
    Deliver(Vec<String>),
    Drop,
}

pub trait ObserveRouter: Send + Sync {
    fn route_log(&self, ctx: &ObserveCtx, event: &LogEvent) -> RouteDecision;
    fn route_metric(&self, spec: &MetricSpec) -> RouteDecision;
    fn route_evidence<T>(&self, _event: &EvidenceEvent<T>) -> RouteDecision
    where
        T: serde::Serialize;
}

#[derive(Default)]
pub struct BroadcastRouter;

impl ObserveRouter for BroadcastRouter {
    fn route_log(&self, _ctx: &ObserveCtx, _event: &LogEvent) -> RouteDecision {
        RouteDecision::Deliver(Vec::new())
    }

    fn route_metric(&self, _spec: &MetricSpec) -> RouteDecision {
        RouteDecision::Deliver(Vec::new())
    }

    fn route_evidence<T>(&self, _event: &EvidenceEvent<T>) -> RouteDecision
    where
        T: serde::Serialize,
    {
        RouteDecision::Deliver(Vec::new())
    }
}

pub fn apply_route(decision: RouteDecision) -> Result<(), ObserveError> {
    match decision {
        RouteDecision::Deliver(_targets) => Ok(()),
        RouteDecision::Drop => Ok(()),
    }
}
