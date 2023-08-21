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

        let service_uri = parsed_uri.scheme().to_string()
            + "://"
            + parsed_uri.host_str().expect("We should get result here");
        let object_name = parsed_uri
            .path()
            .trim_start()
            .trim_start_matches("/")
            .to_string();
        let sas_token = parsed_uri
            .query()
            .expect("Returned URI should contain SAS token as query")
            .to_string();
        let sas_token = StorageCredentials::sas_token(sas_token)?;

        Ok(Self {
            uri,
            service_uri,
            object_name,
            sas_token,
        })
    }
}

impl From<ResourceUri> for QueueClient {
    fn from(resource_uri: ResourceUri) -> Self {
        let queue_service =
            QueueServiceClientBuilder::with_location(azure_storage::CloudLocation::Custom {
                uri: resource_uri.service_uri().to_string(),
                credentials: resource_uri.sas_token().clone(),
            })
            .build();

        queue_service.queue_client(resource_uri.object_name())
    }
}

impl From<ResourceUri> for ContainerClient {
    fn from(resource_uri: ResourceUri) -> Self {
        ClientBuilder::with_location(azure_storage::CloudLocation::Custom {
            uri: resource_uri.service_uri().to_string(),
            credentials: resource_uri.sas_token().clone(),
        })
        .container_client(resource_uri.object_name())
    }
}

impl From<ResourceUri> for TableClient {
    fn from(resource_uri: ResourceUri) -> Self {
        let table_service =
            TableServiceClientBuilder::with_location(azure_storage::CloudLocation::Custom {
                uri: resource_uri.service_uri().to_string(),
                credentials: resource_uri.sas_token().clone(),
            })
            .build();

        table_service.table_client(resource_uri.object_name())
    }
}
