use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD_NO_PAD;
use http::HeaderValue;
use serde_json::Value;

use crate::errors::NetError;
use crate::interceptors::Interceptor;
use crate::types::NetRequest;
use soulbase_auth::AuthFacade;

#[derive(Clone)]
pub struct AuthzInterceptor {
    facade: Arc<AuthFacade>,
    default_attrs: Value,
    propagate_obligations: bool,
}

impl AuthzInterceptor {
    pub fn new(facade: Arc<AuthFacade>) -> Self {
        Self {
            facade,
            default_attrs: Value::Object(Default::default()),
            propagate_obligations: false,
        }
    }

    pub fn with_default_attrs(mut self, attrs: Value) -> Self {
        self.default_attrs = attrs;
        self
    }

    pub fn propagate_obligations(mut self, enabled: bool) -> Self {
        self.propagate_obligations = enabled;
        self
    }
}

#[async_trait]
impl Interceptor for AuthzInterceptor {
    async fn before_send(&self, request: &mut NetRequest) -> Result<(), NetError> {
        let Some(ctx) = request.auth.as_ref() else {
            return Ok(());
        };

        let attrs = if ctx.attrs.is_null() {
            self.default_attrs.clone()
        } else {
            ctx.attrs.clone()
        };

        let decision = self
            .facade
            .authorize(
                ctx.input.clone(),
                ctx.resource.clone(),
                ctx.action.clone(),
                attrs,
                ctx.consent.clone(),
                ctx.correlation_id.clone(),
            )
            .await
            .map_err(|err| NetError::from(err.into_inner()))?;

        if !decision.allow {
            let reason = decision.reason.as_deref().unwrap_or("Authorization denied");
            return Err(NetError::forbidden(reason));
        }

        if self.propagate_obligations && !decision.obligations.is_empty() {
            if let Ok(binary) = serde_json::to_vec(&decision.obligations) {
                let encoded = STANDARD_NO_PAD.encode(binary);
                if let Ok(value) = HeaderValue::from_str(&encoded) {
                    request.headers.insert("x-soul-obligations", value);
                }
            }
        }

        Ok(())
    }
}
