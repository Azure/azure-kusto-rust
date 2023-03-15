//! This module contains the logic to fetch the cloud info from the metadata endpoint.
use std::borrow::Cow;

use azure_core::error::Error as CoreError;
use azure_core::prelude::*;
use azure_core::{Context, Method, Pipeline, Request, StatusCode};
use futures::lock::Mutex;
use hashbrown::hash_map::EntryRef;
use hashbrown::HashMap;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

static CLOUDINFO_CACHE: Lazy<Mutex<HashMap<String, CloudInfo>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
#[serde(rename_all = "PascalCase")]
/// Represents the information from the metadata endpoint about a cloud.
pub struct CloudInfo {
    /// Whether the cloud requires MFA for login.
    pub login_mfa_required: bool,
    /// The login endpoint for the cloud.
    pub login_endpoint: Cow<'static, str>,
    /// The client app id for kusto for the cloud.
    pub kusto_client_app_id: Cow<'static, str>,
    /// The client redirect uri for kusto for the cloud.
    pub kusto_client_redirect_uri: Cow<'static, str>,
    /// The service resource id for kusto for the cloud.
    pub kusto_service_resource_id: Cow<'static, str>,
    /// The first party authority url for the cloud.
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
            first_party_authority_url:
                "https://login.microsoftonline.com/f8cdef31-a31e-4b4a-93e4-5f571e91255a".into(),
        }
    }
}

impl CloudInfo {
    const METADATA_ENDPOINT: &'static str = "v1/rest/auth/metadata";

    async fn fetch(pipeline: &Pipeline, endpoint: &str) -> Result<CloudInfo, crate::error::Error> {
        let metadata_endpoint = format!("{}/{}", endpoint, CloudInfo::METADATA_ENDPOINT);
        let mut request = Request::new(
            metadata_endpoint.parse().map_err(CoreError::from)?,
            Method::Get,
        );
        request.insert_headers(&Accept::from("application/json"));
        request.insert_headers(&AcceptEncoding::from("gzip, deflate"));
        let response = pipeline.send(&mut Context::new(), &mut request).await?;
        let (status_code, _header_map, pinned_stream) = response.deconstruct();
        match status_code {
            StatusCode::Ok => {
                let data = pinned_stream.collect().await?;
                let result: AzureAd = serde_json::from_slice(&data)?;
                Ok(result.azure_ad)
            }
            StatusCode::NotFound => Ok(Default::default()),
            _ => Err(crate::error::Error::HttpError(
                status_code,
                String::from_utf8_lossy((pinned_stream).collect().await?.as_ref()).to_string(),
            )),
        }
    }

    /// Fetch the metadata from the endpoint, and cache it.
    pub async fn get(
        pipeline: &Pipeline,
        endpoint: &str,
    ) -> Result<CloudInfo, crate::error::Error> {
        Ok(match CLOUDINFO_CACHE.lock().await.entry_ref(endpoint) {
            EntryRef::Occupied(o) => o.get().clone(),
            EntryRef::Vacant(e) => {
                let result = CloudInfo::fetch(pipeline, endpoint).await?;
                e.insert(result).clone()
            }
        })
    }

    /// Add a custom settings for a url, and cache them.
    pub async fn add_to_cache(endpoint: &str, cloud_info: CloudInfo) {
        CLOUDINFO_CACHE
            .lock()
            .await
            .insert(endpoint.to_string(), cloud_info);
    }

    /// Check if a url is in the cache.
    pub async fn is_in_cache(endpoint: &str) -> bool {
        CLOUDINFO_CACHE.lock().await.contains_key(endpoint)
    }

    /// Get a url from the cache.
    pub async fn get_from_cache(endpoint: &str) -> Option<CloudInfo> {
        CLOUDINFO_CACHE.lock().await.get(endpoint).cloned()
    }

    /// Remove a url from the cache.
    pub async fn remove_from_cache(endpoint: &str) {
        CLOUDINFO_CACHE.lock().await.remove(endpoint);
    }

    /// Gets the resource uri for the kusto service.
    pub fn get_resource_uri(self) -> Cow<'static, str> {
        let mut resource_uri = self.kusto_service_resource_id;
        if self.login_mfa_required {
            resource_uri = resource_uri.replace(".kusto.", ".kustomfa.").into();
        }
        resource_uri
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
            ClientOptions::default(),
            Vec::new(),
            Vec::new(),
        );
        let a = CloudInfo::get(&pipeline, "https://help.kusto.windows.net/")
            .await
            .unwrap();

        // confirm that the cache is populated
        assert!(CloudInfo::is_in_cache("https://help.kusto.windows.net/").await);

        let b = CloudInfo::get(&pipeline, "https://help.kusto.windows.net/")
            .await
            .unwrap();
        assert_eq!(dbg!(a), dbg!(b));
    }

    //test cache
    #[tokio::test]
    async fn cache() {
        CloudInfo::add_to_cache(
            "https://help.kusto.windows.net/",
            CloudInfo {
                login_mfa_required: true,
                login_endpoint: "https://login.microsoftonline.com".into(),
                kusto_client_app_id: "db662dc1-0cfe-4e1c-a843-19a68e65be58".into(),
                kusto_client_redirect_uri: "https://microsoft/kustoclient".into(),
                kusto_service_resource_id: "https://kusto.kusto.windows.net".into(),
                first_party_authority_url:
                    "https://login.microsoftonline.com/f8cdef31-a31e-4b4a-93e4-5f571e91255a".into(),
            },
        )
        .await;

        // confirm that the cache is populated
        assert!(CloudInfo::is_in_cache("https://help.kusto.windows.net/").await);

        // get from cache
        let a = CloudInfo::get_from_cache("https://help.kusto.windows.net/")
            .await
            .unwrap();
        assert_eq!(
            a,
            CloudInfo {
                login_mfa_required: true,
                login_endpoint: "https://login.microsoftonline.com".into(),
                kusto_client_app_id: "db662dc1-0cfe-4e1c-a843-19a68e65be58".into(),
                kusto_client_redirect_uri: "https://microsoft/kustoclient".into(),
                kusto_service_resource_id: "https://kusto.kusto.windows.net".into(),
                first_party_authority_url:
                    "https://login.microsoftonline.com/f8cdef31-a31e-4b4a-93e4-5f571e91255a".into(),
            }
        );
    }
}
