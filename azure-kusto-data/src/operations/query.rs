#[cfg(feature = "arrow")]
use crate::arrow::convert_table;
use crate::client::KustoClient;
#[cfg(feature = "arrow")]
use arrow::record_batch::RecordBatch;
use std::io::ErrorKind;

use azure_core::prelude::*;
use azure_core::setters;
use azure_core::{collect_pinned_stream, Response as HttpResponse};
use futures::future::BoxFuture;
use futures::io::BufReader;
use futures::{io, pin_mut, stream, AsyncRead, AsyncReadExt, Stream, TryStreamExt};
use serde::{Deserialize, Serialize};

use std::pin::Pin;

type ExecuteQuery = BoxFuture<'static, crate::error::Result<KustoResponseDataSetV2>>;

#[derive(Debug, Serialize, Deserialize)]
struct QueryBody {
    /// Name of the database in scope that is the target of the query or control command
    db: String,
    /// Text of the query or control command to execute
    csl: String,
}

#[derive(Debug, Clone)]
pub struct ExecuteQueryBuilder {
    client: KustoClient,
    database: String,
    query: String,
    client_request_id: Option<ClientRequestId>,
    app: Option<App>,
    user: Option<User>,
    context: Context,
}

impl ExecuteQueryBuilder {
    pub(crate) fn new(
        client: KustoClient,
        database: String,
        query: String,
        context: Context,
    ) -> Self {
        Self {
            client,
            database,
            query: query.trim().into(),
            client_request_id: None,
            app: None,
            user: None,
            context,
        }
    }

    setters! {
        client_request_id: ClientRequestId => Some(client_request_id),
        app: App => Some(app),
        user: User => Some(user),
        query: String => query,
        database: String => database,
        context: Context => context,
    }

    pub async fn into_response(self) -> crate::error::Result<HttpResponse> {
        let url = self.client.query_url();
        let mut request = self
            .client
            .prepare_request(url.parse()?, http::Method::POST);

        if let Some(request_id) = &self.client_request_id {
            request.insert_headers(request_id);
        };
        if let Some(app) = &self.app {
            request.insert_headers(app);
        };
        if let Some(user) = &self.user {
            request.insert_headers(user);
        };

        let body = QueryBody {
            db: self.database,
            csl: self.query,
        };
        let bytes = bytes::Bytes::from(serde_json::to_string(&body)?);
        request.insert_headers(&ContentLength::new(bytes.len() as i32));
        request.set_body(bytes.into());

        let response = self
            .client
            .pipeline()
            .send(&mut self.context.clone(), &mut request)
            .await?;

        Ok(response)
    }

    pub async fn into_stream(
        self,
    ) -> crate::error::Result<impl Stream<Item = Result<ResultTable, io::Error>>> {
        let response = self.into_response().await?;
        let (_status_code, _header_map, pinned_stream) = response.deconstruct();
        let reader = pinned_stream
            .map_err(|e| std::io::Error::new(ErrorKind::Other, e))
            .into_async_read();

        Ok(async_deserializer::iter_results::<ResultTable, _>(reader))
    }

    pub fn into_future(self) -> ExecuteQuery {
        let this = self;

        Box::pin(async move {
            let response = this.into_response().await?;
            <KustoResponseDataSetV2 as async_convert::TryFrom<HttpResponse>>::try_from(response)
                .await
        })
    }
}

use crate::operations::async_deserializer;
use serde::de::DeserializeOwned;
use serde_json::{self};

// TODO enable once in stable
// #[cfg(feature = "into_future")]
// impl std::future::IntoFuture for ExecuteQueryBuilder {
//     type IntoFuture = ExecuteQuery;
//     type Output = <ExecuteQuery as std::future::Future>::Output;
//     fn into_future(self) -> Self::IntoFuture {
//         Self::into_future(self)
//     }
// }

#[derive(Debug, Clone)]
pub struct KustoResponseDataSetV2 {
    pub tables: Vec<ResultTable>,
}

#[async_convert::async_trait]
impl async_convert::TryFrom<HttpResponse> for KustoResponseDataSetV2 {
    type Error = crate::error::Error;

    async fn try_from(response: HttpResponse) -> Result<Self, crate::error::Error> {
        let (_status_code, _header_map, pinned_stream) = response.deconstruct();
        let data = collect_pinned_stream(pinned_stream).await?;
        let tables: Vec<ResultTable> = serde_json::from_slice(&data.to_vec())?;
        Ok(Self { tables })
    }
}

impl KustoResponseDataSetV2 {
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    /// Consumes the response into an iterator over all PrimaryResult tables within the response dataset
    pub fn into_primary_results(self) -> impl Iterator<Item = DataTable> {
        self.tables.into_iter().filter_map(|table| match table {
            ResultTable::DataTable(table) if table.table_kind == TableKind::PrimaryResult => {
                Some(table)
            }
            _ => None,
        })
    }

    #[cfg(feature = "arrow")]
    pub fn into_record_batches(self) -> impl Iterator<Item = crate::error::Result<RecordBatch>> {
        self.into_primary_results().map(convert_table)
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "PascalCase", tag = "FrameType")]
#[allow(clippy::enum_variant_names)]
pub enum ResultTable {
    DataSetHeader(DataSetHeader),
    DataTable(DataTable),
    DataSetCompletion(DataSetCompletion),
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct DataSetHeader {
    pub is_progressive: bool,
    pub version: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct DataTable {
    pub table_id: i32,
    pub table_name: String,
    pub table_kind: TableKind,
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<serde_json::Value>>,
}

/// Categorizes data tables according to the role they play in the data set that a Kusto query returns.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum TableKind {
    PrimaryResult,
    QueryCompletionInformation,
    QueryTraceLog,
    QueryPerfLog,
    TableOfContents,
    QueryProperties,
    QueryPlan,
    Unknown,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct Column {
    pub column_name: String,
    pub column_type: ColumnType,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum ColumnType {
    Bool,
    Boolean,
    Datetime,
    Date,
    Dynamic,
    Guid,
    Int,
    Long,
    Real,
    String,
    Timespan,
    Time,
    Decimal,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct DataSetCompletion {
    pub has_errors: bool,
    pub cancelled: bool,
}
