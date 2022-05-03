use crate::client::KustoClient;
use crate::models::{QueryBody, TableV1};
use async_convert::TryFrom;
use azure_core::prelude::*;
use azure_core::setters;
use azure_core::{collect_pinned_stream, Response as HttpResponse};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};

type ManagementQuery = BoxFuture<'static, crate::error::Result<KustoResponseDataSetV1>>;

#[derive(Debug, Clone)]
pub struct ManagementQueryBuilder {
    client: KustoClient,
    database: String,
    query: String,
    client_request_id: Option<ClientRequestId>,
    app: Option<App>,
    user: Option<User>,
    context: Context,
}

impl ManagementQueryBuilder {
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

    pub fn into_future(self) -> ManagementQuery {
        let this = self.clone();
        let ctx = self.context.clone();

        Box::pin(async move {
            let url = this.client.management_url();
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

            <KustoResponseDataSetV1 as TryFrom<HttpResponse>>::try_from(response).await
        })
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct KustoResponseDataSetV1 {
    pub tables: Vec<TableV1>,
}

impl KustoResponseDataSetV1 {
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }
}

#[async_convert::async_trait]
impl async_convert::TryFrom<HttpResponse> for KustoResponseDataSetV1 {
    type Error = crate::error::Error;

    async fn try_from(response: HttpResponse) -> Result<Self, crate::error::Error> {
        let (_status_code, _header_map, pinned_stream) = response.deconstruct();
        let data = collect_pinned_stream(pinned_stream).await?;
        Ok(serde_json::from_slice(&data.to_vec())?)
    }
}

// TODO enable once in stable
// #[cfg(feature = "into_future")]
// impl std::future::IntoFuture for ManagementQueryBuilder {
//     type IntoFuture = ManagementQuery;
//     type Output = <ManagementQuery as std::future::Future>::Output;
//     fn into_future(self) -> Self::IntoFuture {
//         Self::into_future(self)
//     }
// }

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
        assert!(parsed.is_ok())
    }

    #[test]
    fn load_adminthenquery_response() {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("tests/inputs/adminthenquery.json");

        let data = std::fs::read_to_string(path).unwrap();

        let parsed = serde_json::from_str::<KustoResponseDataSetV1>(&data).unwrap();
        assert_eq!(parsed.table_count(), 4)
    }
}
