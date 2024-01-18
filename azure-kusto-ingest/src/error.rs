//! Defines [Error] for representing failures in various operations.

/// Error type for kusto ingestion operations.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Error raised when failing to obtain ingestion resources.
    #[error("Error obtaining ingestion resources: {0}")]
    ResourceManagerError(#[from] super::resource_manager::ResourceManagerError),

    /// Error relating to (de-)serialization of JSON data
    #[error("Error in JSON serialization/deserialization: {0}")]
    JsonError(#[from] serde_json::Error),

    /// Error occurring within core azure crates
    #[error("Error in azure-core: {0}")]
    AzureError(#[from] azure_core::error::Error),
}

/// Result type for kusto ingest operations.
pub type Result<T> = std::result::Result<T, Error>;
