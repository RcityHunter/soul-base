use crate::model::SpanCtx;

#[derive(Clone, Debug, Default)]
pub struct ObserveCtx {
    pub tenant: String,
    pub subject_kind: Option<String>,
    pub route_id: Option<String>,
    pub resource: Option<String>,
    pub action: Option<String>,
    pub code: Option<String>,
    pub config_version: Option<String>,
    pub config_checksum: Option<String>,
    pub span: SpanCtx,
}

impl ObserveCtx {
    pub fn for_tenant<T: Into<String>>(tenant: T) -> Self {
        Self {
            tenant: tenant.into(),
            ..Default::default()
        }
    }

    pub fn with_span(mut self, span: SpanCtx) -> Self {
        self.span = span;
        self
    }
}
