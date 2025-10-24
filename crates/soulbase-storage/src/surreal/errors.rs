use crate::errors::StorageError;
use surrealdb::Error as SurrealError;

pub fn map_surreal_error(err: SurrealError, context: &str) -> StorageError {
    let message = err.to_string();
    if message.contains("already exists") || message.contains("duplicate") {
        StorageError::conflict(&format!("{context}: {message}"))
    } else if message.contains("does not exist") || message.contains("not found") {
        StorageError::not_found(&format!("{context}: {message}"))
    } else if matches!(err, SurrealError::Api(_)) {
        StorageError::provider_unavailable(&format!("{context}: {message}"))
    } else {
        StorageError::internal(&format!("{context}: {message}"))
    }
}

pub fn not_implemented(feature: &str) -> StorageError {
    StorageError::internal(&format!("Surreal adapter missing feature: {feature}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use soulbase_errors::code::codes;

    fn code_of(err: StorageError) -> &'static str {
        err.into_inner().code.0
    }

    #[test]
    fn duplicate_error_maps_to_conflict() {
        let err = SurrealError::Db(surrealdb::error::Db::TxKeyAlreadyExists);
        let mapped = map_surreal_error(err, "ctx");
        assert_eq!(code_of(mapped), codes::STORAGE_CONFLICT.0);
    }

    #[test]
    fn missing_error_maps_to_not_found() {
        let err = SurrealError::Db(surrealdb::error::Db::Tx("record does not exist".into()));
        let mapped = map_surreal_error(err, "ctx");
        assert_eq!(code_of(mapped), codes::STORAGE_NOT_FOUND.0);
    }

    #[test]
    fn api_error_maps_to_provider_unavailable() {
        let err = SurrealError::Api(surrealdb::error::Api::Ws("disconnected".into()));
        let mapped = map_surreal_error(err, "ctx");
        assert_eq!(code_of(mapped), codes::PROVIDER_UNAVAILABLE.0);
    }

    #[test]
    fn other_errors_map_to_internal() {
        let err = SurrealError::Db(surrealdb::error::Db::Unreachable("boom".into()));
        let mapped = map_surreal_error(err, "ctx");
        assert_eq!(code_of(mapped), codes::UNKNOWN_INTERNAL.0);
    }

    #[test]
    fn not_implemented_returns_internal_error() {
        let err = not_implemented("vector-search");
        assert_eq!(code_of(err), codes::UNKNOWN_INTERNAL.0);
    }
}
