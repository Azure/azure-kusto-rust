use std::borrow::Cow;
use arrow::datatypes::ToByteSlice;

use azure_core::{collect_pinned_stream, Context, Pipeline, Request};
use azure_core::prelude::*;
use futures::lock::Mutex;
use hashbrown::hash_map::{EntryRef};
use hashbrown::HashMap;
use http::Method;
use http::StatusCode;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};

lazy_static! {
    static ref CLOUDINFO_CACHE: Mutex<HashMap<String, CloudInfo>> = Mutex::new(HashMap::new());
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct CloudInfo {
    pub login_mfa_required: bool,
    pub login_endpoint: Cow<'static, str>,
    pub kusto_client_app_id: Cow<'static, str>,
    pub kusto_client_redirect_uri: Cow<'static, str>,
    pub kusto_service_resource_id: Cow<'static, str>,
    pub first_party_authority_url: Cow<'static, str>,
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
struct AzureAd {
    #[serde(rename = "AzureAD")]
    azure_ad: CloudInfo,
}

impl Default for CloudInfo {
    fn default() -> Self {
        Self {
            login_mfa_required: false,
            login_endpoint: "https://login.microsoftonline.com".into(),
            kusto_client_app_id: "db662dc1-0cfe-4e1c-a843-19a68e65be58".into(),
            kusto_client_redirect_uri: "https://microsoft/kustoclient".into(),
            kusto_service_resource_id: "https://kusto.kusto.windows.net".into(),
            first_party_authority_url: "https://login.microsoftonline.com/f8cdef31-a31e-4b4a-93e4-5f571e91255a".into(),
        }
    }
}

impl CloudInfo {
    const METADATA_ENDPOINT: &'static str = "v1/rest/auth/metadata";

    async fn fetch(pipeline: &Pipeline, endpoint: &str) -> Result<CloudInfo, crate::error::Error> {
        let metadata_endpoint = format!("{}/{}", endpoint, CloudInfo::METADATA_ENDPOINT);
        let mut request = Request::new(metadata_endpoint.parse()?, Method::GET);
        request.insert_headers(&Accept::from("application/json"));
        request.insert_headers(&AcceptEncoding::from("gzip, deflate"));
        let response = pipeline.send(&mut Context::new(), &mut request).await?;
        let (status_code, _header_map, pinned_stream) = response.deconstruct();
        match status_code {
            StatusCode::OK => {
                let data = collect_pinned_stream(pinned_stream).await?;
                let result: AzureAd = serde_json::from_slice(&data)?;
                Ok(result.azure_ad)
            }
            StatusCode::NOT_FOUND => Ok(Default::default()),
            _ => Err(crate::error::Error::HttpError(status_code, String::from_utf8_lossy(collect_pinned_stream(pinned_stream).await?.to_byte_slice()).to_string())),
        }
    }

    pub async fn get(pipeline: &Pipeline, endpoint: &str) -> Result<CloudInfo, crate::error::Error> {
        Ok(
            match CLOUDINFO_CACHE.lock()
                .await
                .entry_ref(endpoint) {
                EntryRef::Occupied(o) => o.get().clone(),
                EntryRef::Vacant(e) => {
                    let result = CloudInfo::fetch(pipeline, endpoint).await?;
                    e.insert(result).clone()
                }
            }
        )
    }

    pub async fn add_to_cache(endpoint: &str, cloud_info: CloudInfo) {
        CLOUDINFO_CACHE.lock()
            .await
            .insert(endpoint.to_string(), cloud_info);
    }
}

#[cfg(test)]
mod tests {
    use azure_core::ClientOptions;

    use super::*;

    #[tokio::test]
    async fn fetch() {
        let pipeline = Pipeline::new(
            option_env!("CARGO_PKG_NAME"),
            option_env!("CARGO_PKG_VERSION"),
            ClientOptions::new(),
            Vec::new(),
            Vec::new(),
        );
        let a= CloudInfo::get(&pipeline, "https://asafdev.westeurope.dev.kusto.windows.net/").await.unwrap();
        let b= CloudInfo::get(&pipeline, "https://asafdev.westeurope.dev.kusto.windows.net/").await.unwrap();
        assert_eq!(dbg!(a), dbg!(b));
    }
}
