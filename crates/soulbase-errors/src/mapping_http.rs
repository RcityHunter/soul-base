pub fn to_http_status(err: &crate::model::ErrorObj) -> http::StatusCode {
    http::StatusCode::from_u16(err.http_status).unwrap_or(http::StatusCode::INTERNAL_SERVER_ERROR)
}
