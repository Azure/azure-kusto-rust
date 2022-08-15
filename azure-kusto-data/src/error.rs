//! Defines [Error] for representing failures in various operations.
use http::StatusCode;
use std::fmt::Debug;
use std::num::TryFromIntError;

use thiserror;

/// Error type for kusto operations.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Raised when failing to convert a kusto response to the expected type.
    #[error("Error converting Kusto response for {0}")]
    ConversionError(String),

    /// Error in an external crate
    #[error("Error in external crate {0}")]
    ExternalError(String),

    /// Error in HTTP
    #[error("Error in HTTP: {0} {1}")]
    HttpError(StatusCode, String),

    /// Error raised when an invalid argument / option is provided.
    #[error("Invalid argument {0}")]
    InvalidArgumentError(#[from] InvalidArgumentError),

    /// Error raised when specific functionality is not (yet) implemented
    #[error("Feature not implemented")]
    NotImplemented(String),

    /// Error relating to (de-)serialization of JSON data
    #[error("Error in JSON serialization/deserialization: {0}")]
    JsonError(#[from] serde_json::Error),

    /// Error occurring within core azure crates
    #[error("Error in azure-core: {0}")]
    AzureError(#[from] azure_core::error::Error),

    /// Errors raised when parsing connection information
    #[error("Connection string error: {0}")]
    ConnectionStringError(#[from] ConnectionStringError),

    /// Errors raised when the operation is not supported
    #[error("Operation not supported: {0}")]
    UnsupportedOperation(String),

    /// Invalid uri error
    #[error("Invalid uri: {0}")]
    InvalidUri(#[from] http::uri::InvalidUri),
}

/// Errors raised when an invalid argument or option is provided.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum InvalidArgumentError {
    /// Error raised when a string denoting a duration is not valid.
    #[error("{0} is not a valid duration")]
    InvalidDuration(String),
    /// Error raised when failing to convert a number to u32.
    #[error("{0} is too large to fit in a u32")]
    PayloadTooLarge(#[from] TryFromIntError),
}

/// Errors raised when parsing connection strings.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum ConnectionStringError {
    /// Raised when a connection string is missing a required key.
    #[error("Missing value for key '{}'", key)]
    MissingValue {
        /// The key that is missing.
        key: String,
    },
    /// Raised when a connection string has an unexpected key.
    #[error("Unexpected key '{}'", key)]
    UnexpectedKey {
        /// The key that is unexpected.
        key: String,
    },
    /// Raised when a connection string has an invalid value.
    #[error("Parsing error: {}", msg)]
    Parsing {
        /// The error message.
        msg: String,
    },
}

impl ConnectionStringError {
    pub(crate) fn from_missing_value(key: impl Into<String>) -> Self {
        Self::MissingValue { key: key.into() }
    }
    pub(crate) fn from_unexpected_key(key: impl Into<String>) -> Self {
        Self::UnexpectedKey { key: key.into() }
    }
    pub(crate) fn from_parsing_error(msg: impl Into<String>) -> Self {
        Self::Parsing { msg: msg.into() }
    }
}

/// Result type for kusto operations.
pub type Result<T> = std::result::Result<T, Error>;
