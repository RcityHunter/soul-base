use crate::context::{InterceptContext, ProtoRequest, ProtoResponse};
use crate::errors::InterceptError;
use crate::stages::InterceptorChain;
use async_trait::async_trait;
use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use axum::response::Response;
use futures::FutureExt;
use http::header::HeaderName;
use std::str::FromStr;

pub struct AxumReq<'a> {
    pub req: &'a mut Request<Body>,
    pub cached_json: Option<serde_json::Value>,
}

pub struct AxumRes {
    pub headers: http::HeaderMap,
    pub status: StatusCode,
    pub body: Option<serde_json::Value>,
}

#[async_trait]
impl ProtoRequest for AxumReq<'_> {
    fn method(&self) -> &str {
        self.req.method().as_str()
    }

    fn path(&self) -> &str {
        self.req.uri().path()
    }

    fn header(&self, name: &str) -> Option<String> {
        self.req
            .headers()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
    }

    async fn read_json(&mut self) -> Result<serde_json::Value, InterceptError> {
        if let Some(value) = self.cached_json.clone() {
            return Ok(value);
        }

        let body = std::mem::take(self.req.body_mut());
        let bytes = to_bytes(body, 1_048_576)
            .await
            .map_err(|e| InterceptError::internal(&format!("read body: {e}")))?;
        if bytes.is_empty() {
            *self.req.body_mut() = Body::empty();
            return Ok(serde_json::json!({}));
        }
        let value: serde_json::Value = serde_json::from_slice(&bytes)
            .map_err(|e| InterceptError::schema(&format!("json parse: {e}")))?;
        *self.req.body_mut() = Body::from(bytes.clone());
        self.cached_json = Some(value.clone());
        Ok(value)
    }
}

#[async_trait]
impl ProtoResponse for AxumRes {
    fn set_status(&mut self, code: u16) {
        self.status = StatusCode::from_u16(code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    }

    fn insert_header(&mut self, name: &str, value: &str) {
        if let (Ok(header_name), Ok(header_value)) = (HeaderName::from_str(name), value.parse()) {
            self.headers.insert(header_name, header_value);
        }
    }

    async fn write_json(&mut self, body: &serde_json::Value) -> Result<(), InterceptError> {
        self.body = Some(body.clone());
        Ok(())
    }
}

pub async fn handle_with_chain<F, Fut>(
    mut req: Request<Body>,
    chain: &InterceptorChain,
    handler: F,
) -> Response
where
    F: FnOnce(&mut InterceptContext, &mut dyn ProtoRequest) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<serde_json::Value, InterceptError>> + Send + 'static,
{
    let cx = InterceptContext::default();
    let mut preq = AxumReq {
        req: &mut req,
        cached_json: None,
    };
    let mut pres = AxumRes {
        headers: http::HeaderMap::new(),
        status: StatusCode::OK,
        body: None,
    };

    match chain
        .run_with_handler(cx, &mut preq, &mut pres, |ctx, req| {
            handler(ctx, req).boxed()
        })
        .await
    {
        Ok(()) => {
            let mut response = Response::builder()
                .status(pres.status)
                .body(Body::empty())
                .unwrap();
            if let Some(body) = pres.body {
                let bytes = serde_json::to_vec(&body).unwrap_or_default();
                *response.body_mut() = Body::from(bytes);
            }
            *response.headers_mut() = pres.headers;
            response
        }
        Err(err) => {
            let (status, json) = crate::errors::to_http_response(&err);
            let bytes = serde_json::to_vec(&json).unwrap_or_default();
            Response::builder()
                .status(StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
                .header(http::header::CONTENT_TYPE, "application/json")
                .body(Body::from(bytes))
                .unwrap()
        }
    }
}
