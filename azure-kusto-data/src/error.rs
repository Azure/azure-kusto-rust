//! Defines `KustoRsError` for representing failures in various operations.
use std::fmt::Debug;
use std::num::TryFromIntError;
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
    #[error("Error in JSON serialization/deserialization: {0}")]
    JsonError(#[from] serde_json::Error),

    /// Error occurring within core azure crates
    #[error("Error in azure-core: {0}")]
    AzureError(#[from] azure_core::error::Error),

    /// Errors raised when parsing connection information
    #[error("Connection string error: {0}")]
    ConnectionStringError(#[from] ConnectionStringError),
}

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum InvalidArgumentError {
    #[error("{0} is not a valid duration")]
    InvalidDuration(String),
    #[error("{0} is too large to fit in a u32")]
    PayloadTooLarge(#[from] TryFromIntError),
}

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum ConnectionStringError {
    #[error("Missing value for key '{}'", key)]
    MissingValue { key: String },
    #[error("Unexpected key '{}'", key)]
    UnexpectedKey { key: String },
    #[error("Parsing error: {}", msg)]
    ParsingError { msg: String },
}

pub type Result<T> = std::result::Result<T, Error>;
