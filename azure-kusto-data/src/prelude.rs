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

pub use crate::client::{KustoClient, KustoClientOptions};
pub use crate::connection_string::ConnectionStringBuilder;
pub use crate::operations::query::{
    KustoResponse, KustoResponseDataSetV1, KustoResponseDataSetV2, ResultTable,
};
// Token credentials are re-exported for user convenience
pub use azure_identity::token_credentials::{
    AutoRefreshingTokenCredential, AzureCliCredential, ClientSecretCredential,
    DefaultAzureCredential, DefaultAzureCredentialBuilder, EnvironmentCredential,
    ManagedIdentityCredentialError, TokenCredentialOptions,
};
