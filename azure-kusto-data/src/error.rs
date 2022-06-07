//! Defines `KustoRsError` for representing failures in various operations.
use http::uri::InvalidUri;
use std::fmt::Debug;
use thiserror;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Error converting Kusto response for {0}")]
    ConversionError(String),

    /// Error in external crate
    #[error("Error in external crate {0}")]
    ExternalError(String),

    /// Error raised when an invalid argument / option is provided.
    #[error("Invalid argument {0}")]
    InvalidArgumentError(#[from] InvalidArgumentError),

    /// Error raised when specific functionality is not (yet) implemented
    #[error("Feature not implemented")]
    NotImplemented(String),

    /// Error relating to (de-)serialization of JSON data
    #[error(transparent)]
    JsonError(#[from] serde_json::Error),

    /// Error occurring within core azure crates
    #[error(transparent)]
    AzureError(#[from] azure_core::error::Error),

    /// Errors raised when parsing connection information
    #[error("Configuration error: {0}")]
    ConfigurationError(#[from] crate::connection_string::ConnectionStringError),
}

#[derive(thiserror::Error, Debug)]
pub enum InvalidArgumentError {
    #[error(transparent)]
    InvalidUri(#[from] InvalidUri),
    #[error("{0} is not a valid duration")]
    InvalidDuration(String),
}

pub type Result<T> = std::result::Result<T, Error>;
