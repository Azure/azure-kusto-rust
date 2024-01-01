#[cfg(feature = "arrow")]
use crate::arrow::convert_table;
use crate::client::{KustoClient, QueryKind};

use crate::error::{Error, Result};
use crate::query::QueryBody;
use crate::models::v2::{DataTable, TableFragmentType, TableKind};
use crate::models::v1::{Dataset as V1Dataset};
use crate::operations::v2;
use crate::prelude::ClientRequestProperties;
#[cfg(feature = "arrow")]
use arrow_array::RecordBatch;
use async_convert::TryFrom;
use azure_core::error::Error as CoreError;
use azure_core::headers::Headers;
use azure_core::prelude::*;
use azure_core::{CustomHeaders, Method, Request, Response as HttpResponse, Response};
use futures::future::BoxFuture;
use futures::{Stream, TryFutureExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::future::IntoFuture;
use std::io::ErrorKind;
use std::sync::Arc;

type QueryRun = BoxFuture<'static, Result<KustoResponse>>;
type V1QueryRun = BoxFuture<'static, Result<V1Dataset>>;
type V2QueryRun = BoxFuture<'static, Result<KustoResponseDataSetV2>>;

#[derive(Debug, Clone, derive_builder::Builder)]
#[builder(setter(into, prefix = "with"))]
pub struct QueryRunner {
    client: KustoClient,
    database: String,
    query: String,
    kind: QueryKind,
    client_request_properties: Option<ClientRequestProperties>,
    default_headers: Arc<Headers>,
}
pub struct V1QueryRunner(pub QueryRunner);

pub struct V2QueryRunner(pub QueryRunner);

impl V2QueryRunner {
    pub async fn into_stream(self) -> Result<impl Stream<Item = Result<DataSet>>> {
        let V2QueryRunner(query_runner) = self;
        query_runner.into_stream().await
    }
}

impl QueryRunner {
    async fn into_response(self) -> Result<Response> {
        let url = match self.kind {
            QueryKind::Management => self.client.management_url(),
            QueryKind::Query => self.client.query_url(),
        };
        let mut request = Request::new(url.parse().map_err(CoreError::from)?, Method::Post);

        let mut context = Context::new();
        let mut headers = self.default_headers.as_ref().clone();

        if let Some(client_request_properties) = &self.client_request_properties {
            if let Some(client_request_id) = &client_request_properties.client_request_id {
                headers.insert("x-ms-client-request-id", client_request_id);
            }

            if let Some(application) = &client_request_properties.application {
                headers.insert("x-ms-app", application);
            }
        }

        context.insert(CustomHeaders::from(headers));

        let body = QueryBody {
            db: self.database,
            csl: self.query,
            properties: self.client_request_properties,
        };

        let bytes = bytes::Bytes::from(serde_json::to_string(&body)?);
        request.set_body(bytes);

        let response = self
            .client
            .pipeline()
            .send(&mut context, &mut request)
            .await?;
        Ok(response)
    }

    pub async fn into_stream(self) -> Result<impl Stream<Item = Result<DataSet>>> {
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

        Ok(v2::parse_frames_iterative(reader).map_err(Error::from))
    }
}

impl IntoFuture for V1QueryRunner {
    type Output = Result<V1Dataset>;
    type IntoFuture = V1QueryRun;

    fn into_future(self) -> V1QueryRun {
        Box::pin(async {
            let V1QueryRunner(query_runner) = self;
            let future = query_runner.into_future().await?;
            Ok(
                std::convert::TryInto::try_into(future).expect("Unexpected conversion error from KustoResponse to V1Dataset - please report this issue to the Kusto team")
            )
        })
    }
}

impl IntoFuture for V2QueryRunner {
    type Output = Result<KustoResponseDataSetV2>;
    type IntoFuture = V2QueryRun;

    fn into_future(self) -> V2QueryRun {
        Box::pin(async {
            let V2QueryRunner(query_runner) = self;
            let future = query_runner.into_future().await?;
            Ok(
                std::convert::TryInto::try_into(future).expect("Unexpected conversion error from KustoResponse to KustoResponseDataSetV2 - please report this issue to the Kusto team")
            )
        })
    }
}

impl IntoFuture for QueryRunner {
    type Output = Result<KustoResponse>;
    type IntoFuture = QueryRun;

    fn into_future(self) -> QueryRun {
        let this = self.clone();

        Box::pin(async move {
            let response = self.into_response().await?;

            Ok(match this.kind {
                QueryKind::Management => {
                    <V1Dataset as TryFrom<HttpResponse>>::try_from(response)
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
}

/// A Kusto query response.
#[derive(Debug, Clone)]
pub enum KustoResponse {
    /// V1 Response - represents management queries, and old V1 data queries.
    V1(V1Dataset),
    /// V2 Response - represents new V2 data queries.
    V2(KustoResponseDataSetV2),
}

/// The top level response from a Kusto query.
#[derive(Debug, Clone)]
pub struct KustoResponseDataSetV2 {
    /// All of the raw results in the response.
    pub results: Vec<DataSet>,
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

impl std::convert::TryFrom<KustoResponse> for V1Dataset {
    type Error = Error;

    fn try_from(value: KustoResponse) -> Result<Self> {
        match value {
            KustoResponse::V1(v1) => Ok(v1),
            _ => Err(Error::ConversionError("KustoResponseDataSetV2".to_string())),
        }
    }
}

struct KustoResponseDataSetV2TableIterator<T: Iterator<Item = DataSet>> {
    tables: T,
    finished: bool,
}

impl<T: Iterator<Item = DataSet>> KustoResponseDataSetV2TableIterator<T> {
    fn new(tables: T) -> Self {
        Self {
            tables,
            finished: false,
        }
    }
}

impl<T: Iterator<Item = DataSet>> Iterator for KustoResponseDataSetV2TableIterator<T> {
    type Item = DataTable;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        let next_table = self.tables.find_map(|t| match t {
            DataSet::DataTable(_) | DataSet::TableHeader(_) => Some(t),
            _ => None,
        });

        if let Some(DataSet::DataTable(t)) = next_table {
            return Some(t);
        }

        let mut table = DataTable {
            table_id: 0,
            table_name: "".to_string(),
            table_kind: TableKind::Unknown,
            columns: vec![],
            rows: vec![],
        };

        if let Some(DataSet::TableHeader(header)) = next_table {
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
                DataSet::TableFragment(fragment) => {
                    assert_eq!(fragment.table_id, table.table_id);
                    match fragment.table_fragment_type {
                        TableFragmentType::DataAppend => table.rows.extend(fragment.rows),
                        TableFragmentType::DataReplace => table.rows = fragment.rows,
                    };
                }
                DataSet::TableProgress(progress) => {
                    assert_eq!(progress.table_id, table.table_id);
                }
                DataSet::TableCompletion(completion) => {
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
    ///         DataSet::DataSetHeader(DataSetHeader {is_progressive: false,version: "".to_string()}),
    ///         DataSet::DataTable(DataTable {
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
    ///    DataSet::DataSetHeader(DataSetHeader {is_progressive: false,version: "".to_string()}),
    ///    DataSet::DataTable(DataTable {
    ///        table_id: 0,
    ///        table_name: "table_1".to_string(),
    ///        table_kind: TableKind::QueryCompletionInformation,
    ///        columns: vec![],
    ///        rows: vec![],
    ///    }),
    ///    DataSet::TableHeader(TableHeader {
    ///        table_id: 1,
    ///        table_name: "table_2".to_string(),
    ///        table_kind: TableKind::PrimaryResult,
    ///        columns: vec![],
    ///    }),
    ///    DataSet::TableCompletion(TableCompletion {
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
    ///    DataSet::DataSetHeader(DataSetHeader {is_progressive: false,version: "".to_string()}),
    ///    DataSet::DataTable(DataTable {
    ///        table_id: 0,
    ///        table_name: "table_1".to_string(),
    ///        table_kind: TableKind::QueryCompletionInformation,
    ///        columns: vec![],
    ///        rows: vec![],
    ///    }),
    ///    DataSet::TableHeader(TableHeader {
    ///        table_id: 1,
    ///        table_name: "table_2".to_string(),
    ///        table_kind: TableKind::PrimaryResult,
    ///        columns: vec![],
    ///    }),
    ///    DataSet::TableCompletion(TableCompletion {
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
    ///    DataSet::DataSetHeader(DataSetHeader {is_progressive: false,version: "".to_string()}),
    ///    DataSet::DataTable(DataTable {
    ///        table_id: 0,
    ///        table_name: "table_1".to_string(),
    ///        table_kind: TableKind::PrimaryResult,
    ///        columns: vec![Column{column_name: "col1".to_string(), column_type: ColumnType::Long}],
    ///        rows: vec![Value::Array(vec![Value::from(3u64)])],
    ///    }),
    ///    DataSet::TableHeader(TableHeader {
    ///        table_id: 1,
    ///        table_name: "table_2".to_string(),
    ///        table_kind: TableKind::PrimaryResult,
    ///        columns: vec![Column{column_name: "col1".to_string(), column_type: ColumnType::String}],
    ///    }),
    ///    DataSet::TableFragment(TableFragment {
    ///       table_id: 1,
    ///       rows: vec![Value::Array(vec![Value::from("first")]), Value::Array(vec![Value::from("second")])],
    ///       field_count: Some(1),
    ///       table_fragment_type: TableFragmentType::DataAppend,
    ///     }),
    ///    DataSet::TableCompletion(TableCompletion {
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

#[async_convert::async_trait]
impl TryFrom<HttpResponse> for KustoResponseDataSetV2 {
    type Error = Error;

    async fn try_from(response: HttpResponse) -> Result<Self> {
        let (_status_code, _header_map, pinned_stream) = response.deconstruct();
        let data = pinned_stream.collect().await?;
        let tables: Vec<DataSet> = serde_json::from_slice(&data)?;
        Ok(Self { results: tables })
    }
}

#[async_convert::async_trait]
impl TryFrom<HttpResponse> for V1Dataset {
    type Error = Error;

    async fn try_from(response: HttpResponse) -> Result<Self> {
        let (_status_code, _header_map, pinned_stream) = response.deconstruct();
        let data = pinned_stream.collect().await?;
        Ok(serde_json::from_slice(&data)?)
    }
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

        let parsed = serde_json::from_str::<V1Dataset>(data).expect("Failed to parse");

        assert_eq!(parsed.tables[0].columns[0].column_name, "Text");
        assert_eq!(parsed.tables[0].rows[0][0], "Hello, World!");
    }

    #[test]
    fn load_adminthenquery_response() {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("tests/inputs/adminthenquery.json");

        let data = std::fs::read_to_string(&path)
            .unwrap_or_else(|_| panic!("Failed to read {}", path.display()));

        let parsed = serde_json::from_str::<V1Dataset>(&data)
            .expect("Failed to parse response");
        assert_eq!(parsed.table_count(), 4);
    }
}
