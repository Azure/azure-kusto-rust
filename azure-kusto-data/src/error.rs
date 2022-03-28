//! Defines `KustoRsError` for representing failures in various operations.
use http::StatusCode;
use std::fmt::Debug;
use thiserror;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Error converting Kusto response for {0}")]
    ConversionError(String),

    /// Error in external crate
    #[error("Error in external crate {0}")]
    ExternalError(String),

    /// Error in HTTP
    #[error("Error in HTTP: {0} {1}")]
    HttpError(StatusCode, String),

    /// Error raised when an invalid argument / option is provided.
    #[error("Type conversion not available")]
    InvalidArgumentError(String),

    /// Error raised when specific functionality is not (yet) implemented
    #[error("Feature not implemented")]
    NotImplemented(String),

    /// Error relating to (de-)serialization of JSON data
    #[error(transparent)]
    JsonError(#[from] serde_json::Error),

    /// Error occurring within core azure crates
    #[error(transparent)]
    AzureError(#[from] azure_core::Error),

    /// Errors raised when parsing connection information
    #[error("Configuration error: {0}")]
    ConfigurationError(#[from] crate::connection_string::ConnectionStringError),

    /// Error when streaming
    #[error(transparent)]
    StreamError(#[from] azure_core::StreamError),

    /// Error when parsing URI
    #[error(transparent)]
    InvalidUri(#[from] http::uri::InvalidUri),
}

pub type Result<T> = std::result::Result<T, Error>;
