#[cfg(feature = "arrow")]
use crate::arrow::convert_table;
use crate::client::KustoClient;
use crate::models::{DataSetCompletion, DataSetHeader, DataTable, QueryBody, TableKind};
#[cfg(feature = "arrow")]
use arrow::record_batch::RecordBatch;
use async_convert::TryFrom;
use azure_core::prelude::*;
use azure_core::setters;
use azure_core::{collect_pinned_stream, Response as HttpResponse};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};

type ExecuteQuery = BoxFuture<'static, crate::error::Result<KustoResponseDataSetV2>>;

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

    pub fn into_future(self) -> ExecuteQuery {
        let this = self.clone();
        let ctx = self.context.clone();

        Box::pin(async move {
            let url = this.client.query_url();
            let mut request = this
                .client
                .prepare_request(url.parse()?, http::Method::POST);

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
            };
            let bytes = bytes::Bytes::from(serde_json::to_string(&body)?);
            request.insert_headers(&ContentLength::new(bytes.len() as i32));
            request.set_body(bytes.into());

            let response = self
                .client
                .pipeline()
                .send(&mut ctx.clone(), &mut request)
                .await?;

            <KustoResponseDataSetV2 as TryFrom<HttpResponse>>::try_from(response).await
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
pub struct KustoResponseDataSetV2 {
    pub tables: Vec<ResultTable>,
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

// TODO enable once in stable
// #[cfg(feature = "into_future")]
// impl std::future::IntoFuture for ExecuteQueryBuilder {
//     type IntoFuture = ExecuteQuery;
//     type Output = <ExecuteQuery as std::future::Future>::Output;
//     fn into_future(self) -> Self::IntoFuture {
//         Self::into_future(self)
//     }
// }
