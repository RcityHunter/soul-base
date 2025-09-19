use crate::errors::StorageError;

pub fn not_implemented(feature: &str) -> StorageError {
    StorageError::internal(&format!("Surreal adapter missing feature: {feature}"))
}
