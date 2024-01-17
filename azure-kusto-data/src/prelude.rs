//! The kusto prelude.
//!
//! The prelude re-exports most commonly used items from this crate.
//!
//! # Examples
//!
//! Import the prelude with:
//!
//! ```
//! # #[allow(unused_imports)]
//! use azure_kusto_data::prelude::*;
//! ```

pub use crate::client::{KustoClient, KustoClientOptions, QueryKind};
pub use crate::connection_string::{
    ConnectionString, ConnectionStringAuth, DeviceCodeFunction, TokenCallbackFunction,
};
pub use crate::error::Error;
pub use crate::models::{DataTable, V2QueryResult};
pub use crate::operations::query::{KustoResponse, KustoResponseDataSetV1, KustoResponseDataSetV2};
pub use crate::request_options::{
    ClientRequestProperties, ClientRequestPropertiesBuilder, Options, OptionsBuilder,
};

// Token credentials are re-exported for user convenience
pub use azure_identity::{
    AzureCliCredential, ClientSecretCredential,
    DefaultAzureCredential, DefaultAzureCredentialBuilder, EnvironmentCredential,
    TokenCredentialOptions,
};
