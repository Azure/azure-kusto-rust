#![warn(missing_docs)]

//! # Azure Data Explorer Client Library
//! Query and explore data from Azure Data Explorer (Kusto).
//! Learn more about Azure Data Explorer at [https://docs.microsoft.com/en-us/azure/data-explorer/](https://docs.microsoft.com/en-us/azure/data-explorer/).

#[cfg(feature = "arrow")]
mod arrow;
mod authorization_policy;
pub mod client;
mod cloud_info;
pub mod connection_string;
pub mod error;
pub mod models;
mod operations;
pub mod prelude;
pub mod request_options;
pub mod types;
