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
