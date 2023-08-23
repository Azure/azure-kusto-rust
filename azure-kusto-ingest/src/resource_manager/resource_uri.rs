use azure_core::ClientOptions;
use azure_data_tables::{clients::TableServiceClientBuilder, prelude::TableClient};
use azure_storage::StorageCredentials;
use azure_storage_blobs::prelude::{ClientBuilder, ContainerClient};
use azure_storage_queues::{QueueClient, QueueServiceClientBuilder};
use url::Url;

use anyhow::Result;

/// Parsing logic of resource URIs as returned by the Kusto management endpoint
#[derive(Debug, Clone)]
pub struct ResourceUri {
    uri: String,
    service_uri: String,
    object_name: String,
    sas_token: StorageCredentials,
}

impl ResourceUri {
    pub fn uri(&self) -> &str {
        self.uri.as_str()
    }

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

impl TryFrom<String> for ResourceUri {
    type Error = anyhow::Error;

    fn try_from(uri: String) -> Result<Self> {
        println!("uri: {:#?}", uri);
        let parsed_uri = Url::parse(&uri)?;
        println!("parsed_uri: {:#?}", parsed_uri);

        let service_uri = match parsed_uri.host_str() {
            Some(host_str) => parsed_uri.scheme().to_string() + "://" + host_str,
            None => return Err(anyhow::anyhow!("Host is missing in the URI")),
        };
        let object_name = parsed_uri
            .path()
            .trim_start()
            .trim_start_matches("/")
            .to_string();
        let sas_token = match parsed_uri.query() {
            Some(query) => query.to_string(),
            None => return Err(anyhow::anyhow!("SAS token is missing in the URI as a query parameter")),
        };
        let sas_token = StorageCredentials::sas_token(sas_token)?;

        Ok(Self {
            uri,
            service_uri,
            object_name,
            sas_token,
        })
    }
}

pub trait ClientFromResourceUri {
    fn create_client(resource_uri: ResourceUri, client_options: ClientOptions) -> Self;
}

impl ClientFromResourceUri for QueueClient {
    fn create_client(resource_uri: ResourceUri, client_options: ClientOptions) -> Self {
        let queue_service =
            QueueServiceClientBuilder::with_location(azure_storage::CloudLocation::Custom {
                uri: resource_uri.service_uri().to_string(),
                credentials: resource_uri.sas_token().clone(),
            })
            .client_options(client_options)
            .build();

        queue_service.queue_client(resource_uri.object_name())
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

impl ClientFromResourceUri for TableClient {
    fn create_client(resource_uri: ResourceUri, client_options: ClientOptions) -> Self {
        let table_service =
            TableServiceClientBuilder::with_location(azure_storage::CloudLocation::Custom {
                uri: resource_uri.service_uri().to_string(),
                credentials: resource_uri.sas_token().clone(),
            })
            .client_options(client_options)
            .build();

        table_service.table_client(resource_uri.object_name())
    }
}
