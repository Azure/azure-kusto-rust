#[cfg(feature = "arrow")]
use crate::arrow::convert_table;
use crate::client::{KustoClient, QueryKind};

use crate::error::{Error, InvalidArgumentError};
use crate::models::{
    DataSetCompletion, DataSetHeader, DataTable, QueryBody, RequestProperties, TableKind, TableV1,
};
use crate::request_options::RequestOptions;
#[cfg(feature = "arrow")]
use arrow::record_batch::RecordBatch;
use async_convert::TryFrom;
use azure_core::error::Error as CoreError;
use azure_core::prelude::*;
use azure_core::{collect_pinned_stream, Request, Response as HttpResponse};
use futures::future::BoxFuture;
use futures::TryFutureExt;
use http::Uri;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

type QueryRun = BoxFuture<'static, crate::error::Result<KustoResponse>>;
type V1QueryRun = BoxFuture<'static, crate::error::Result<KustoResponseDataSetV1>>;
type V2QueryRun = BoxFuture<'static, crate::error::Result<KustoResponseDataSetV2>>;

#[derive(Debug, Clone, derive_builder::Builder)]
#[builder(setter(into, strip_option, prefix = "with"))]
pub struct QueryRunner {
    client: KustoClient,
    database: String,
    query: String,
    kind: QueryKind,
    #[builder(default)]
    client_request_id: Option<ClientRequestId>,
    #[builder(default)]
    app: Option<App>,
    #[builder(default)]
    user: Option<User>,
    #[builder(default)]
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
}

impl QueryRunner {
    pub fn into_future(self) -> QueryRun {
        let this = self.clone();
        let ctx = self.context.clone();

        Box::pin(async move {
            let url = match this.kind {
                QueryKind::Management => this.client.management_url(),
                QueryKind::Query => this.client.query_url(),
            };
            let mut request =
                prepare_request(url.parse().map_err(CoreError::from)?, http::Method::POST);

            if let Some(request_id) = &this.client_request_id {
                request.insert_headers(request_id);
            };
            if let Some(app) = &this.app {
                request.insert_headers(app);
            };
            if let Some(user) = &this.user {
                request.insert_headers(user);
            };

            let body = QueryBody {
                db: this.database,
                csl: this.query,
                Properties: Some(RequestProperties {
                    options: this.options,
                    parameters: this.parameters,
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
                .send(&mut ctx.clone(), &mut request)
                .await?;

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
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "PascalCase", tag = "FrameType")]
#[allow(clippy::enum_variant_names)]
pub enum ResultTable {
    DataSetHeader(DataSetHeader),
    DataTable(DataTable),
    DataSetCompletion(DataSetCompletion),
}

#[derive(Debug, Clone)]
pub enum KustoResponse {
    V1(KustoResponseDataSetV1),
    V2(KustoResponseDataSetV2),
}

#[derive(Debug, Clone)]
pub struct KustoResponseDataSetV2 {
    pub tables: Vec<ResultTable>,
}

impl std::convert::TryFrom<KustoResponse> for KustoResponseDataSetV2 {
    type Error = Error;

    fn try_from(value: KustoResponse) -> Result<Self, Self::Error> {
        match value {
            KustoResponse::V2(v2) => Ok(v2),
            _ => Err(Error::ConversionError("KustoResponseDataSetV2".to_string())),
        }
    }
}

impl std::convert::TryFrom<KustoResponse> for KustoResponseDataSetV1 {
    type Error = Error;

    fn try_from(value: KustoResponse) -> Result<Self, Self::Error> {
        match value {
            KustoResponse::V1(v1) => Ok(v1),
            _ => Err(Error::ConversionError("KustoResponseDataSetV2".to_string())),
        }
    }
}

impl KustoResponseDataSetV2 {
    #[must_use]
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    /// Consumes the response into an iterator over all `PrimaryResult` tables within the response dataset
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
#[serde(rename_all = "PascalCase")]
pub struct KustoResponseDataSetV1 {
    pub tables: Vec<TableV1>,
}

impl KustoResponseDataSetV1 {
    #[must_use]
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }
}

#[async_convert::async_trait]
impl TryFrom<HttpResponse> for KustoResponseDataSetV2 {
    type Error = Error;

    async fn try_from(response: HttpResponse) -> Result<Self, Error> {
        let (_status_code, _header_map, pinned_stream) = response.deconstruct();
        let data = collect_pinned_stream(pinned_stream).await?;
        let tables: Vec<ResultTable> = serde_json::from_slice(&data.to_vec())?;
        Ok(Self { tables })
    }
}

#[async_convert::async_trait]
impl TryFrom<HttpResponse> for KustoResponseDataSetV1 {
    type Error = Error;

    async fn try_from(response: HttpResponse) -> Result<Self, Error> {
        let (_status_code, _header_map, pinned_stream) = response.deconstruct();
        let data = collect_pinned_stream(pinned_stream).await?;
        Ok(serde_json::from_slice(&data.to_vec())?)
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

pub fn prepare_request(uri: Uri, http_method: http::Method) -> Request {
    const API_VERSION: &str = "2019-02-13";

    let mut request = Request::new(uri, http_method);
    request.insert_headers(&Version::from(API_VERSION));
    request.insert_headers(&Accept::from("application/json"));
    request.insert_headers(&ContentType::new("application/json; charset=utf-8"));
    request.insert_headers(&AcceptEncoding::from("gzip"));
    request.insert_headers(&ClientVersion::from(format!(
        "Kusto.Rust.Client:{}",
        env!("CARGO_PKG_VERSION"),
    )));
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
                    "DataType": "String",
                    "ColumnType": "string"
                }],
                "Rows": [["Hello, World!"]]
            }]
        }"#;

        let parsed = serde_json::from_str::<KustoResponseDataSetV1>(data);
        assert!(parsed.is_ok());
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
