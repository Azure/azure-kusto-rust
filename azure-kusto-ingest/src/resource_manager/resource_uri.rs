use azure_core::ClientOptions;
use azure_storage::StorageCredentials;
use azure_storage_blobs::prelude::{ClientBuilder, ContainerClient};
use azure_storage_queues::{QueueClient, QueueServiceClientBuilder};
use url::Url;

use anyhow::Result;

/// Parsing logic of resource URIs as returned by the Kusto management endpoint
#[derive(Debug, Clone)]
pub struct ResourceUri {
    service_uri: String,
    object_name: String,
    sas_token: StorageCredentials,
}

impl ResourceUri {
    pub fn service_uri(&self) -> &str {
        self.service_uri.as_str()
    }

    pub fn object_name(&self) -> &str {
        self.object_name.as_str()
    }

    pub fn sas_token(&self) -> &StorageCredentials {
        &self.sas_token
    }
}

impl TryFrom<&str> for ResourceUri {
    type Error = anyhow::Error;

    fn try_from(uri: &str) -> Result<Self> {
        let parsed_uri = Url::parse(uri)?;

        let service_uri = match parsed_uri.host_str() {
            Some(host_str) => parsed_uri.scheme().to_string() + "://" + host_str,
            None => return Err(anyhow::anyhow!("Host is missing in the URI")),
        };
        let object_name = parsed_uri
            .path()
            .trim_start()
            .trim_start_matches('/')
            .to_string();
        let sas_token = match parsed_uri.query() {
            Some(query) => query.to_string(),
            None => {
                return Err(anyhow::anyhow!(
                    "SAS token is missing in the URI as a query parameter"
                ))
            }
        };
        let sas_token = StorageCredentials::sas_token(sas_token)?;

        Ok(Self {
            service_uri,
            object_name,
            sas_token,
        })
    }
}

/// Trait to be used to create an Azure client from a resource URI with configurability of ClientOptions
pub trait ClientFromResourceUri {
    fn create_client(resource_uri: ResourceUri, client_options: ClientOptions) -> Self;
}

impl ClientFromResourceUri for QueueClient {
    fn create_client(resource_uri: ResourceUri, client_options: ClientOptions) -> Self {
        QueueServiceClientBuilder::with_location(azure_storage::CloudLocation::Custom {
            uri: resource_uri.service_uri().to_string(),
            credentials: resource_uri.sas_token().clone(),
        })
        .client_options(client_options)
        .build()
        .queue_client(resource_uri.object_name())
    }
}

impl ClientFromResourceUri for ContainerClient {
    fn create_client(resource_uri: ResourceUri, client_options: ClientOptions) -> Self {
        ClientBuilder::with_location(azure_storage::CloudLocation::Custom {
            uri: resource_uri.service_uri().to_string(),
            credentials: resource_uri.sas_token().clone(),
        })
        .client_options(client_options)
        .container_client(resource_uri.object_name())
    }
}
