//! Defines [Error] for representing failures in various operations.
use azure_core::StatusCode;
use std::fmt::Debug;

use crate::models::v2::OneApiError;
use thiserror;
/// Error type for kusto operations.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Raised when failing to convert a kusto response to the expected type.
    #[error("Error converting Kusto response for {0}")]
    ConversionError(String),

    /// Error in an external crate
    #[error("Error in external crate {0}")]
    ExternalError(Box<dyn std::error::Error + Send + Sync>),

    /// Error in HTTP
    #[error("Error in HTTP: {0} {1}")]
    HttpError(StatusCode, String),

    /// Error in parsing
    #[error("Error in parsing: {0}")]
    ParseError(#[from] ParseError),

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

    /// Errors raised when the query is invalid
    #[error("Invalid query: {0}")]
    QueryError(String),

    /// Errors raised for IO operations
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    /// Errors raised from the api calls to kusto
    #[error("Query API error: {0}")]
    QueryApiError(OneApiError),

    /// Multiple errors
    #[error("Multiple errors: {0:?}")]
    MultipleErrors(Vec<Error>),
}

impl<T> Into<Partial<T>> for Error {
    fn into(self) -> Partial<T> {
        Err((None, self))
    }
}
impl From<Vec<Error>> for Error {
    fn from(errors: Vec<Error>) -> Self {
        if errors.len() == 1 {
            Error::from(errors.into_iter().next().unwrap())
        } else {
            Error::MultipleErrors(errors)
        }
    }
}

/// Errors raised when parsing values.
#[derive(thiserror::Error, Debug)]
pub enum ParseError {
    /// Raised when a value is null, but the type is not nullable.
    #[error("Error parsing null value for {0}")]
    ValueNull(String),
    /// Raised when an int value is failed to be parsed.
    #[error("Error parsing int: {0}")]
    Int(#[from] std::num::ParseIntError),
    /// Raised when a long value is failed to be parsed.
    #[error("Error parsing float: {0}")]
    Float(#[from] std::num::ParseFloatError),
    /// Raised when a bool value is failed to be parsed.
    #[error("Error parsing bool: {0}")]
    Bool(#[from] std::str::ParseBoolError),
    /// Raised when a timespan value is failed to be parsed.
    #[error("Error parsing timespan: {0}")]
    Timespan(String),
    /// Raised when a datetime value is failed to be parsed.
    #[error("Error parsing datetime: {0}")]
    DateTime(#[from] time::error::Parse),
    /// Raised when a guid value is failed to be parsed.
    #[error("Error parsing guid: {0}")]
    Guid(#[from] uuid::Error),
    /// Raised when a decimal value is failed to be parsed.
    #[error("Error parsing decimal")]
    Decimal(#[from] rust_decimal::Error),
    /// Raised when a dynamic value is failed to be parsed.
    #[error("Error parsing dynamic: {0}")]
    Dynamic(#[from] serde_json::Error),

    #[error("Error parsing Frame: {0}")]
    Frame(String),
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
pub type Partial<T> = std::result::Result<T, (Option<T>, Error)>;

pub(crate) trait PartialExt<T> {
    fn ignore_partial_results(self) -> Result<T>;
}

impl<T> PartialExt<T> for Partial<T> {
    fn ignore_partial_results(self) -> Result<T> {
        match self {
            Ok(v) => Ok(v),
            Err((_, e)) => Err(e),
        }
    }
}

pub fn partial_from_tuple<T>(t: (Option<T>, Option<Error>)) -> Partial<T> {
    match t {
        (Some(v), None) => Ok(v),
        (None, Some(e)) => Err((None, e)),
        (Some(v), Some(e)) => Err((Some(v), e)),
        (None, None) => Err((None, Error::NotImplemented("No value and no error".to_string()))),
    }
}


impl<T: Send + Sync + 'static> From<tokio::sync::mpsc::error::SendError<T>> for Error {
    fn from(e: tokio::sync::mpsc::error::SendError<T>) -> Self {
        Error::ExternalError(Box::new(e))
    }
}
