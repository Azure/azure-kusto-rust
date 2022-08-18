#[cfg(feature = "arrow")]
use crate::arrow::convert_table;
use crate::client::{KustoClient, QueryKind};

use crate::error::{Error, InvalidArgumentError, Result};
use crate::models::{
    DataTable, QueryBody, RequestProperties, TableFragmentType, TableKind, TableV1, V2QueryResult,
};
use crate::operations::async_deserializer;
use crate::request_options::RequestOptions;
#[cfg(feature = "arrow")]
use arrow::record_batch::RecordBatch;
use async_convert::TryFrom;
use azure_core::error::Error as CoreError;
use azure_core::prelude::*;
use azure_core::{Method, Request, Response as HttpResponse, Response, Url};
use futures::future::BoxFuture;
use futures::{Stream, TryFutureExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::ErrorKind;

type QueryRun = BoxFuture<'static, Result<KustoResponse>>;
type V1QueryRun = BoxFuture<'static, Result<KustoResponseDataSetV1>>;
type V2QueryRun = BoxFuture<'static, Result<KustoResponseDataSetV2>>;

#[derive(Debug, Clone, derive_builder::Builder)]
#[builder(setter(into, prefix = "with"))]
pub struct QueryRunner {
    client: KustoClient,
    database: String,
    query: String,
    kind: QueryKind,
    #[builder(default)]
    client_request_id: Option<ClientRequestId>,
    #[builder(default, setter(strip_option))]
    app: Option<App>,
    #[builder(default, setter(strip_option))]
    user: Option<User>,
    #[builder(default, setter(strip_option))]
    parameters: Option<HashMap<String, serde_json::Value>>,
    #[builder(default)]
    options: Option<RequestOptions>,
    context: Context,
}
pub struct V1QueryRunner(pub QueryRunner);

pub struct V2QueryRunner(pub QueryRunner);

impl V1QueryRunner {
    pub fn into_future(self) -> V1QueryRun {
        Box::pin(async {
            let V1QueryRunner(query_runner) = self;
            let future = query_runner.into_future().await?;
            Ok(
                std::convert::TryInto::try_into(future).expect("Unexpected conversion error from KustoResponse to KustoResponseDataSetV1 - please report this issue to the Kusto team")
            )
        })
    }
}

impl V2QueryRunner {
    pub fn into_future(self) -> V2QueryRun {
        Box::pin(async {
            let V2QueryRunner(query_runner) = self;
            let future = query_runner.into_future().await?;
            Ok(
                std::convert::TryInto::try_into(future).expect("Unexpected conversion error from KustoResponse to KustoResponseDataSetV2 - please report this issue to the Kusto team")
            )
        })
    }

    pub async fn into_stream(self) -> Result<impl Stream<Item = Result<V2QueryResult>>> {
        let V2QueryRunner(query_runner) = self;
        query_runner.into_stream().await
    }
}

impl QueryRunner {
    pub fn into_future(self) -> QueryRun {
        let this = self.clone();

        Box::pin(async move {
            let response = self.into_response().await?;

            Ok(match this.kind {
                QueryKind::Management => {
                    <KustoResponseDataSetV1 as TryFrom<HttpResponse>>::try_from(response)
                        .map_ok(KustoResponse::V1)
                        .await?
                }
                QueryKind::Query => {
                    <KustoResponseDataSetV2 as TryFrom<HttpResponse>>::try_from(response)
                        .map_ok(KustoResponse::V2)
                        .await?
                }
            })
        })
    }

    async fn into_response(self) -> Result<Response> {
        let url = match self.kind {
            QueryKind::Management => self.client.management_url(),
            QueryKind::Query => self.client.query_url(),
        };
        let mut request = prepare_request(url.parse().map_err(CoreError::from)?, Method::Post);

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
            properties: Some(RequestProperties {
                options: self.options,
                parameters: self.parameters,
            }),
        };
        let bytes = bytes::Bytes::from(serde_json::to_string(&body)?);
        request.insert_headers(&ContentLength::new(
            std::convert::TryInto::try_into(bytes.len()).map_err(InvalidArgumentError::from)?,
        ));
        request.set_body(bytes);

        let response = self
            .client
            .pipeline()
            .send(&mut self.context.clone(), &mut request)
            .await?;
        Ok(response)
    }

    pub async fn into_stream(self) -> Result<impl Stream<Item = Result<V2QueryResult>>> {
        if self.kind != QueryKind::Query {
            return Err(Error::UnsupportedOperation(
                "Progressive streaming is only supported for queries".to_string(),
            ));
        }

        let response = self.into_response().await?;
        let (_status_code, _header_map, pinned_stream) = response.deconstruct();
        let reader = pinned_stream
            .map_err(|e| std::io::Error::new(ErrorKind::Other, e))
            .into_async_read();
        Ok(async_deserializer::iter_results::<V2QueryResult, _>(
            reader,
        ).map_err(|e| (*e.into_inner().expect("Unexpected error from async_deserializer - please report this issue to the Kusto team").downcast::<azure_core::error::Error>().expect("Unexpected error from async_deserializer - please report this issue to the Kusto team")).into()  ))
    }
}

/// A Kusto query response.
#[derive(Debug, Clone)]
pub enum KustoResponse {
    /// V1 Response - represents management queries, and old V1 data queries.
    V1(KustoResponseDataSetV1),
    /// V2 Response - represents new V2 data queries.
    V2(KustoResponseDataSetV2),
}

/// The top level response from a Kusto query.
#[derive(Debug, Clone)]
pub struct KustoResponseDataSetV2 {
    /// All of the raw results in the response.
    pub results: Vec<V2QueryResult>,
}

impl std::convert::TryFrom<KustoResponse> for KustoResponseDataSetV2 {
    type Error = Error;

    fn try_from(value: KustoResponse) -> Result<Self> {
        match value {
            KustoResponse::V2(v2) => Ok(v2),
            _ => Err(Error::ConversionError("KustoResponseDataSetV2".to_string())),
        }
    }
}

impl std::convert::TryFrom<KustoResponse> for KustoResponseDataSetV1 {
    type Error = Error;

    fn try_from(value: KustoResponse) -> Result<Self> {
        match value {
            KustoResponse::V1(v1) => Ok(v1),
            _ => Err(Error::ConversionError("KustoResponseDataSetV2".to_string())),
        }
    }
}

struct KustoResponseDataSetV2TableIterator<T: Iterator<Item = V2QueryResult>> {
    tables: T,
    finished: bool,
}

impl<T: Iterator<Item = V2QueryResult>> KustoResponseDataSetV2TableIterator<T> {
    fn new(tables: T) -> Self {
        Self {
            tables,
            finished: false,
        }
    }
}

impl<T: Iterator<Item = V2QueryResult>> Iterator for KustoResponseDataSetV2TableIterator<T> {
    type Item = DataTable;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        let next_table = self.tables.find_map(|t| match t {
            V2QueryResult::DataTable(_) | V2QueryResult::TableHeader(_) => Some(t),
            _ => None,
        });

        if let Some(V2QueryResult::DataTable(t)) = next_table {
            return Some(t);
        }

        let mut table = DataTable {
            table_id: 0,
            table_name: "".to_string(),
            table_kind: TableKind::Unknown,
            columns: vec![],
            rows: vec![],
        };

        if let Some(V2QueryResult::TableHeader(header)) = next_table {
            table.table_id = header.table_id;
            table.table_kind = header.table_kind;
            table.table_name = header.table_name;
            table.columns = header.columns;
        } else {
            self.finished = true;
            return None;
        }

        let mut finished_table = false;

        for result in &mut self.tables {
            match result {
                V2QueryResult::TableFragment(fragment) => {
                    assert_eq!(fragment.table_id, table.table_id);
                    match fragment.table_fragment_type {
                        TableFragmentType::DataAppend => table.rows.extend(fragment.rows),
                        TableFragmentType::DataReplace => table.rows = fragment.rows,
                    };
                }
                V2QueryResult::TableProgress(progress) => {
                    assert_eq!(progress.table_id, table.table_id);
                }
                V2QueryResult::TableCompletion(completion) => {
                    assert_eq!(completion.table_id, table.table_id);
                    assert_eq!(
                        completion.row_count,
                        TryInto::<i32>::try_into(table.rows.len()).expect("Row count overflow")
                    );
                    finished_table = true;
                    break;
                }
                _ => unreachable!("Unexpected result type"),
            }
        }

        if finished_table {
            Some(table)
        } else {
            None
        }
    }
}

impl KustoResponseDataSetV2 {
    /// Count of the number of the raw results in the response.
    /// This, in addition to tables, includes headers and other non-table results.
    /// # Example
    /// ```rust
    /// use azure_kusto_data::models::*;
    /// use azure_kusto_data::prelude::{DataTable, KustoResponseDataSetV2};
    ///
    /// let data_set = KustoResponseDataSetV2 {
    ///    results: vec![
    ///         V2QueryResult::DataSetHeader(DataSetHeader {is_progressive: false,version: "".to_string()}),
    ///         V2QueryResult::DataTable(DataTable {
    ///         table_id: 0,
    ///         table_name: "table_1".to_string(),
    ///         table_kind: TableKind::PrimaryResult,
    ///         columns: vec![],
    ///         rows: vec![],
    ///         }),
    ///     ], };
    ///
    /// assert_eq!(data_set.raw_results_count(), 2);
    /// ```
    #[must_use]
    pub fn raw_results_count(&self) -> usize {
        self.results.len()
    }

    /// Iterates over the tables in the response.
    /// If the query is progressive, it will combine the table parts into a single table.
    ///
    /// This method does not consume the response, so it can be called multiple times.
    /// [Use into_parsed_data_tables](#method.into_parsed_data_tables) to consume the response and reduce memory usage.
    /// # Example
    /// ```rust
    /// use azure_kusto_data::models::*;
    /// use azure_kusto_data::prelude::{DataTable, KustoResponseDataSetV2};
    ///
    ///let data_set = KustoResponseDataSetV2 {
    ///results: vec![
    ///    V2QueryResult::DataSetHeader(DataSetHeader {is_progressive: false,version: "".to_string()}),
    ///    V2QueryResult::DataTable(DataTable {
    ///        table_id: 0,
    ///        table_name: "table_1".to_string(),
    ///        table_kind: TableKind::QueryCompletionInformation,
    ///        columns: vec![],
    ///        rows: vec![],
    ///    }),
    ///    V2QueryResult::TableHeader(TableHeader {
    ///        table_id: 1,
    ///        table_name: "table_2".to_string(),
    ///        table_kind: TableKind::PrimaryResult,
    ///        columns: vec![],
    ///    }),
    ///    V2QueryResult::TableCompletion(TableCompletion {
    ///        table_id: 1,
    ///        row_count: 0,
    ///    }),
    ///],
    ///};
    /// let mut results = vec![];
    /// for table in data_set.parsed_data_tables() {
    ///    results.push(format!("{} - {}", table.table_id, table.table_name));
    /// }
    ///
    /// assert_eq!(results, vec!["0 - table_1", "1 - table_2"]);
    /// ```
    pub fn parsed_data_tables(&self) -> impl Iterator<Item = DataTable> + '_ {
        KustoResponseDataSetV2TableIterator::new(self.results.iter().cloned())
    }

    /// Iterates over the tables in the response, yielding only the primary tables.
    /// If the query is progressive, it will combine the table parts into a single table.
    ///
    /// This method does not consume the response, so it can be called multiple times.
    /// [Use into_primary_results](#method.into_primary_results) to consume the response and reduce memory usage.
    /// # Example
    /// ```rust
    /// use azure_kusto_data::models::*;
    /// use azure_kusto_data::prelude::{DataTable, KustoResponseDataSetV2};
    ///
    ///let data_set = KustoResponseDataSetV2 {
    ///results: vec![
    ///    V2QueryResult::DataSetHeader(DataSetHeader {is_progressive: false,version: "".to_string()}),
    ///    V2QueryResult::DataTable(DataTable {
    ///        table_id: 0,
    ///        table_name: "table_1".to_string(),
    ///        table_kind: TableKind::QueryCompletionInformation,
    ///        columns: vec![],
    ///        rows: vec![],
    ///    }),
    ///    V2QueryResult::TableHeader(TableHeader {
    ///        table_id: 1,
    ///        table_name: "table_2".to_string(),
    ///        table_kind: TableKind::PrimaryResult,
    ///        columns: vec![],
    ///    }),
    ///    V2QueryResult::TableCompletion(TableCompletion {
    ///        table_id: 1,
    ///        row_count: 0,
    ///    }),
    ///],
    ///};
    /// let mut results = vec![];
    /// for table in data_set.primary_results() {
    ///    results.push(format!("{} - {}", table.table_id, table.table_name));
    /// }
    ///
    /// assert_eq!(results, vec!["1 - table_2"]);
    /// ```
    /// Consumes the response into an iterator over all PrimaryResult tables within the response dataset
    pub fn primary_results(&self) -> impl Iterator<Item = DataTable> + '_ {
        self.parsed_data_tables()
            .filter(|t| t.table_kind == TableKind::PrimaryResult)
    }

    /// Iterates over the tables in the response, and converts them into `arrow` `Batches`
    /// If the query is progressive, it will combine the table parts into a single table.
    ///
    /// This method does not consume the response, so it can be called multiple times.
    /// [Use into_primary_results](#method.into_primary_results) to consume the response and reduce memory usage.
    /// # Example
    /// ```rust
    /// use serde_json::Value;
    /// use azure_kusto_data::models::*;
    /// use azure_kusto_data::prelude::{DataTable, KustoResponseDataSetV2};
    ///
    ///let data_set = KustoResponseDataSetV2 {
    ///results: vec![
    ///    V2QueryResult::DataSetHeader(DataSetHeader {is_progressive: false,version: "".to_string()}),
    ///    V2QueryResult::DataTable(DataTable {
    ///        table_id: 0,
    ///        table_name: "table_1".to_string(),
    ///        table_kind: TableKind::PrimaryResult,
    ///        columns: vec![Column{column_name: "col1".to_string(), column_type: ColumnType::Long}],
    ///        rows: vec![vec![Value::from(3u64)]],
    ///    }),
    ///    V2QueryResult::TableHeader(TableHeader {
    ///        table_id: 1,
    ///        table_name: "table_2".to_string(),
    ///        table_kind: TableKind::PrimaryResult,
    ///        columns: vec![Column{column_name: "col1".to_string(), column_type: ColumnType::String}],
    ///    }),
    ///    V2QueryResult::TableFragment(TableFragment {
    ///       table_id: 1,
    ///       rows: vec![vec![Value::from("first")], vec![Value::from("second")]],
    ///       field_count: Some(1),
    ///       table_fragment_type: TableFragmentType::DataAppend,
    ///     }),
    ///    V2QueryResult::TableCompletion(TableCompletion {
    ///        table_id: 1,
    ///        row_count: 2,
    ///    }),
    ///],
    ///};
    /// let mut results = vec![];
    /// for batch in data_set.record_batches() {
    ///    results.push(batch.map(|b| b.num_rows()).unwrap_or(0));
    /// }
    ///
    /// assert_eq!(results, vec![1, 2]);
    /// ```
    /// Consumes the response into an iterator over all PrimaryResult tables within the response dataset
    #[cfg(feature = "arrow")]
    pub fn record_batches(&self) -> impl Iterator<Item = Result<RecordBatch>> + '_ {
        self.primary_results().map(convert_table)
    }

    /// Consuming version for [parse_data_tables](#method.parse_data_tables).
    pub fn into_parsed_data_tables(self) -> impl Iterator<Item = DataTable> {
        KustoResponseDataSetV2TableIterator::new(self.results.into_iter())
    }

    /// Consuming version for [primary_results](#method.primary_results).
    pub fn into_primary_results(self) -> impl Iterator<Item = DataTable> {
        self.into_parsed_data_tables()
            .filter(|t| t.table_kind == TableKind::PrimaryResult)
    }

    #[cfg(feature = "arrow")]
    /// Consuming version for [record_batches](#method.record_batches).
    pub fn into_record_batches(self) -> impl Iterator<Item = Result<RecordBatch>> {
        self.into_primary_results().map(convert_table)
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(rename_all = "PascalCase")]
/// The header of a Kusto response dataset for v1. Contains a list of tables.
pub struct KustoResponseDataSetV1 {
    /// The list of tables in the dataset.
    pub tables: Vec<TableV1>,
}

impl KustoResponseDataSetV1 {
    #[must_use]
    /// Count the number of tables in the dataset.
    /// # Example
    /// ```rust
    /// use azure_kusto_data::models::TableV1;
    /// use azure_kusto_data::prelude::KustoResponseDataSetV1;
    /// let dataset = KustoResponseDataSetV1 {
    ///    tables: vec![
    ///       TableV1 {
    ///         table_name: "table_1".to_string(),
    ///         columns: vec![],
    ///         rows: vec![],
    ///      },
    /// ]};
    ///
    /// assert_eq!(dataset.table_count(), 1);
    ///
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }
}

#[async_convert::async_trait]
impl TryFrom<HttpResponse> for KustoResponseDataSetV2 {
    type Error = Error;

    async fn try_from(response: HttpResponse) -> Result<Self> {
        let (_status_code, _header_map, pinned_stream) = response.deconstruct();
        let data = pinned_stream.collect().await?;
        let tables: Vec<V2QueryResult> = serde_json::from_slice(&data)?;
        Ok(Self { results: tables })
    }
}

#[async_convert::async_trait]
impl TryFrom<HttpResponse> for KustoResponseDataSetV1 {
    type Error = Error;

    async fn try_from(response: HttpResponse) -> Result<Self> {
        let (_status_code, _header_map, pinned_stream) = response.deconstruct();
        let data = pinned_stream.collect().await?;
        Ok(serde_json::from_slice(&data)?)
    }
}

// TODO enable once in stable
// #[cfg(feature = "into_future")]
// impl std::future::IntoFuture for ExecuteQueryBuilder {
//     type IntoFuture = ExecuteQuery;
//     type Output = <ExecuteQuery as std::future::Future>::Output;
//     fn into_future(self) -> Self::IntoFuture {
//         Self::into_future(self)
//     }
// }

pub fn prepare_request(url: Url, http_method: Method) -> Request {
    const API_VERSION: &str = "2019-02-13";

    let mut request = Request::new(url, http_method);
    request.insert_headers(&Version::from(API_VERSION));
    request.insert_headers(&Accept::from("application/json"));
    request.insert_headers(&ContentType::new("application/json; charset=utf-8"));
    request.insert_headers(&AcceptEncoding::from("gzip"));
    request.insert_headers(&ClientVersion::from(format!(
        "Kusto.Rust.Client:{}",
        env!("CARGO_PKG_VERSION"),
    )));
    request.insert_header("connection", "Keep-Alive");
    request
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn load_response_data() {
        let data = r#"{
            "Tables": [{
                "TableName": "Table_0",
                "Columns": [{
                    "ColumnName": "Text",
                    "DataType": "String"
                }],
                "Rows": [["Hello, World!"]]
            }]
        }"#;

        let parsed = serde_json::from_str::<KustoResponseDataSetV1>(data).expect("Failed to parse");

        assert_eq!(parsed.tables[0].columns[0].column_name, "Text");
        assert_eq!(parsed.tables[0].rows[0][0], "Hello, World!");
    }

    #[test]
    fn load_adminthenquery_response() {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("tests/inputs/adminthenquery.json");

        let data = std::fs::read_to_string(&path)
            .unwrap_or_else(|_| panic!("Failed to read {}", path.display()));

        let parsed = serde_json::from_str::<KustoResponseDataSetV1>(&data)
            .expect("Failed to parse response");
        assert_eq!(parsed.table_count(), 4);
    }
}
