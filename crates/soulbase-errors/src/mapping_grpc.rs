pub fn to_grpc_status(err: &crate::model::ErrorObj) -> tonic::Status {
    use tonic::Code;

    let code = match err.grpc_status {
        Some(value) => Code::from_i32(value),
        None => Code::Unknown,
    };
    tonic::Status::new(code, err.message_user.clone())
}
