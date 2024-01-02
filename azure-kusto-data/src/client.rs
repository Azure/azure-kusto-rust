//! This module contains the client for the Azure Kusto Data service.

use crate::authorization_policy::AuthorizationPolicy;
use crate::connection_string::{ConnectionString, ConnectionStringAuth};
use crate::error::{Error, Result};
use crate::operations::query::{QueryRunner, QueryRunnerBuilder, V1QueryRunner, V2QueryRunner};

use azure_core::{ClientOptions, Pipeline};

use crate::client_details::ClientDetails;
use crate::prelude::ClientRequestProperties;
use azure_core::headers::Headers;
use azure_core::prelude::{Accept, AcceptEncoding, ClientVersion, ContentType};
use serde::de::DeserializeOwned;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::sync::Arc;
use serde_json::Value;
use crate::models::v2::Row;

/// Options for specifying how a Kusto client will behave
#[derive(Clone, Default)]
pub struct KustoClientOptions {
    options: ClientOptions,
}

impl From<ClientOptions> for KustoClientOptions {
    fn from(c: ClientOptions) -> Self {
        Self { options: c }
    }
}

impl KustoClientOptions {
    /// Create new options
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

fn new_pipeline_from_options(
    auth: ConnectionStringAuth,
    resource: String,
    options: KustoClientOptions,
) -> Pipeline {
    let auth_policy = Arc::new(AuthorizationPolicy::new(auth, resource));
    // take care of adding the AuthorizationPolicy as **last** retry policy.
    let per_retry_policies: Vec<Arc<(dyn azure_core::Policy + 'static)>> = vec![auth_policy];

    Pipeline::new(
        option_env!("CARGO_PKG_NAME"),
        option_env!("CARGO_PKG_VERSION"),
        options.options,
        Vec::new(),
        per_retry_policies,
    )
}

/// Kusto client for Rust.
/// The client is a wrapper around the Kusto REST API.
/// To read more about it, go to [https://docs.microsoft.com/en-us/azure/kusto/api/rest/](https://docs.microsoft.com/en-us/azure/kusto/api/rest/)
///
/// The primary methods are:
/// `execute_query`:  executes a KQL query against the Kusto service.
#[derive(Clone, Debug)]
pub struct KustoClient {
    pipeline: Arc<Pipeline>,
    query_url: Arc<String>,
    management_url: Arc<String>,
    default_headers: Arc<Headers>,
}

/// Denotes what kind of query is being executed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryKind {
    /// A Management query. The returned type is [`KustoResponse::V1`](crate::operations::query::KustoResponse::V1)
    Management,
    /// A KQL query. The returned type is [`KustoResponse::V2`](crate::operations::query::KustoResponse::V2)
    Query,
}

impl KustoClient {
    /// Create a new Kusto client.
    /// This method accepts a connection string, that includes the Kusto cluster and the authentication information for the cluster.
    /// # Example
    /// ```rust
    /// use azure_kusto_data::prelude::*;
    ///
    /// let client = KustoClient::new(
    ///    ConnectionString::with_default_auth("https://mycluster.region.kusto.windows.net/"),
    ///    KustoClientOptions::default());
    ///
    /// assert!(client.is_ok());
    /// ```
    pub fn new(connection_string: ConnectionString, options: KustoClientOptions) -> Result<Self> {
        let default_headers = Arc::new(Self::default_headers(connection_string.client_details()));
        let (data_source, credentials) = connection_string.into_data_source_and_auth();
        let service_url = Arc::new(data_source.trim_end_matches('/').to_string());
        let query_url = format!("{service_url}/v2/rest/query");
        let management_url = format!("{service_url}/v1/rest/mgmt");
        let pipeline = new_pipeline_from_options(credentials, (*service_url).clone(), options);

        Ok(Self {
            pipeline: pipeline.into(),
            query_url: query_url.into(),
            management_url: management_url.into(),
            default_headers,
        })
    }

    pub(crate) fn default_headers(details: ClientDetails) -> Headers {
        let mut headers = Headers::new();
        const API_VERSION: &str = "2019-02-13";
        headers.insert("x-ms-kusto-api-version", API_VERSION);
        headers.insert("x-ms-app", details.application);
        headers.insert("x-ms-user", details.user);
        headers.add(Accept::from("application/json"));
        headers.add(ContentType::new("application/json; charset=utf-8"));
        headers.add(AcceptEncoding::from("gzip"));
        headers.add(ClientVersion::from(details.version));
        headers.insert("connection", "Keep-Alive");

        headers
    }

    pub(crate) fn query_url(&self) -> &str {
        &self.query_url
    }

    pub(crate) fn management_url(&self) -> &str {
        &self.management_url
    }

    pub(crate) fn pipeline(&self) -> &Pipeline {
        &self.pipeline
    }

    /// Execute a query against the Kusto cluster.
    /// The `kind` parameter determines whether the request is a query (retrieves data from the tables) or a management query (commands to monitor and manage the cluster).
    /// This method should only be used if the query kind is not known at compile time, otherwise use [execute](#method.execute) or [execute_command](#method.execute_command).
    /// # Example
    /// ```no_run
    /// use azure_kusto_data::prelude::*;
    /// # #[tokio::main] async fn main() -> Result<(), Error> {
    ///
    /// let client = KustoClient::new(
    ///   ConnectionString::with_default_auth("https://mycluster.region.kusto.windows.net/"),
    ///   KustoClientOptions::default())?;
    ///
    ///  let result = client.execute_with_options("some_database", ".show version", QueryKind::Management, None).await?;
    ///
    /// assert!(matches!(result, KustoResponse::V1(..)));
    /// # Ok(())}
    /// ```
    #[must_use]
    pub fn execute_with_options(
        &self,
        database: impl Into<String>,
        query: impl Into<String>,
        kind: QueryKind,
        client_request_properties: Option<ClientRequestProperties>,
    ) -> QueryRunner {
        QueryRunnerBuilder::default()
            .with_kind(kind)
            .with_client(self.clone())
            .with_database(database)
            .with_query(query)
            .with_default_headers(self.default_headers.clone())
            .with_client_request_properties(client_request_properties)
            .build()
            .expect("Unexpected error when building query runner - please report this issue to the Kusto team")
    }

    /// Execute a KQL query with additional request options.
    /// To learn more about KQL go to [https://docs.microsoft.com/en-us/azure/kusto/query/](https://docs.microsoft.com/en-us/azure/kusto/query)
    ///
    /// # Example
    /// ```no_run
    /// use azure_kusto_data::prelude::*;
    /// # #[tokio::main] async fn main() -> Result<(), Error> {
    /// use azure_kusto_data::client::QueryKind;
    ///
    /// let client = KustoClient::new(
    ///    ConnectionString::with_default_auth("https://mycluster.region.kusto.windows.net/"),
    ///    KustoClientOptions::default())?;
    ///    let result = client.execute_query(
    ///         "some_database",
    ///         "MyTable | take 10",
    ///         Some(OptionsBuilder::default().with_request_app_name("app name").build().unwrap().into()))
    ///     .await?;
    ///
    ///   for table in result.into_primary_results() {
    ///        println!("{}", table.table_name);
    ///    }
    /// # Ok(())}
    /// ```
    ///
    #[must_use]
    pub fn execute_query(
        &self,
        database: impl Into<String>,
        query: impl Into<String>,
        options: Option<ClientRequestProperties>,
    ) -> V2QueryRunner {
        V2QueryRunner(self.execute_with_options(database, query, QueryKind::Query, options))
    }

    /// Execute a KQL query into an array of structs.
    /// To learn more about KQL go to [https://docs.microsoft.com/en-us/azure/kusto/query/](https://docs.microsoft.com/en-us/azure/kusto/query)
    ///
    /// This method is the simplest way to just convert your data into a struct.
    /// It assumes there is one primary result table.
    ///
    /// Your struct should implement the [serde::DeserializeOwned](https://docs.serde.rs/serde/trait.DeserializeOwned.html) trait.
    ///
    /// # Example
    /// ```no_run
    /// use azure_kusto_data::prelude::*;
    /// use serde::Deserialize;
    ///
    /// #[derive(serde::Deserialize, Debug)]
    /// struct MyStruct {
    ///    name: String,
    ///    age: u32,
    /// }
    ///
    /// # #[tokio::main] async fn main() -> Result<(), Error> {
    /// let client = KustoClient::new(
    ///    ConnectionString::with_default_auth("https://mycluster.region.kusto.windows.net/"),
    ///    KustoClientOptions::default())?;
    ///
    ///    let result: Vec<MyStruct> = client.execute_query_to_struct("some_database", "MyTable | take 10", None).await?;
    ///    println!("{:?}", result); // prints [MyStruct { name: "foo", age: 42 }, MyStruct { name: "bar", age: 43 }]
    ///
    /// # Ok(())}
    /// ```
    pub async fn execute_query_to_struct<T: DeserializeOwned>(
        &self,
        database: impl Into<String>,
        query: impl Into<String>,
        client_request_properties: Option<ClientRequestProperties>,
    ) -> Result<Vec<T>> {
        let response = self
            .execute_query(database, query, client_request_properties)
            .await?;

        let results = response
            .into_primary_results()
            .next()
            .ok_or_else(|| Error::QueryError("No primary results found".into()))?
            .rows
            .into_iter()
            .map(|row| match row {
                Row::Values(v) => serde_json::from_value(Value::Array(v)).map_err(Error::from),
                Row::Error(e) => Err(Error::QueryApiError(e)),
            })
            .collect::<Result<Vec<T>>>()?;

        Ok(results)
    }

    /// Execute a management command with additional options.
    /// To learn more about see [commands](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/management/)
    ///
    /// # Example
    /// ```no_run
    /// use azure_kusto_data::prelude::*;
    /// # #[tokio::main] async fn main() -> Result<(), Error> {
    /// let client = KustoClient::new(
    ///    ConnectionString::with_default_auth("https://mycluster.region.kusto.windows.net/"),
    ///    KustoClientOptions::default())?;
    ///
    ///    let result = client.execute_command("some_database", ".show version",
    ///     Some(OptionsBuilder::default().with_request_app_name("app name").build().unwrap().into()))
    ///     .await?;
    ///
    /// for table in result.tables {
    ///        println!("{}", table.table_name);
    ///    }
    /// # Ok(())}
    /// ```
    #[must_use]
    pub fn execute_command(
        &self,
        database: impl Into<String>,
        query: impl Into<String>,
        options: Option<ClientRequestProperties>,
    ) -> V1QueryRunner {
        V1QueryRunner(self.execute_with_options(database, query, QueryKind::Management, options))
    }
}

impl TryFrom<ConnectionString> for KustoClient {
    type Error = Error;

    fn try_from(value: ConnectionString) -> Result<Self> {
        Self::new(value, KustoClientOptions::new())
    }
}
