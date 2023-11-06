use azure_core::ClientOptions;
use azure_storage::StorageCredentials;
use azure_storage_blobs::prelude::{ClientBuilder, ContainerClient};
use azure_storage_queues::{QueueClient, QueueServiceClientBuilder};
use url::Url;

use anyhow::Result;

/// Parsing logic of resource URIs as returned by the Kusto management endpoint
#[derive(Debug, Clone)]
pub(crate) struct ResourceUri {
    pub(crate) service_uri: String,
    pub(crate) object_name: String,
    pub(crate) sas_token: StorageCredentials,
}

impl TryFrom<&str> for ResourceUri {
    type Error = anyhow::Error;

    fn try_from(uri: &str) -> Result<Self> {
        let parsed_uri = Url::parse(uri)?;

        let scheme = match parsed_uri.scheme() {
            "https" => "https".to_string(),
            other_scheme => {
                return Err(anyhow::anyhow!(
                    "URI scheme must be 'https', was '{other_scheme}'"
                ))
            }
        };

        let service_uri = scheme
            + "://"
            + parsed_uri
                .host_str()
                .expect("Url::parse should always return a host for a URI");

        let object_name = match parsed_uri.path().trim_start().trim_start_matches('/') {
            "" => return Err(anyhow::anyhow!("Object name is missing in the URI")),
            name => name.to_string(),
        };

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
pub(crate) trait ClientFromResourceUri {
    fn create_client(resource_uri: ResourceUri, client_options: ClientOptions) -> Self;
}

impl ClientFromResourceUri for QueueClient {
    fn create_client(resource_uri: ResourceUri, client_options: ClientOptions) -> Self {
        QueueServiceClientBuilder::with_location(
            azure_storage::CloudLocation::Custom {
                uri: resource_uri.service_uri,
            },
            resource_uri.sas_token,
        )
        .client_options(client_options)
        .build()
        .queue_client(resource_uri.object_name)
    }
}

impl ClientFromResourceUri for ContainerClient {
    fn create_client(resource_uri: ResourceUri, client_options: ClientOptions) -> Self {
        ClientBuilder::with_location(
            azure_storage::CloudLocation::Custom {
                uri: resource_uri.service_uri,
            },
            resource_uri.sas_token,
        )
        .client_options(client_options)
        .container_client(resource_uri.object_name)
    }
}

#[cfg(test)]
mod tests {
    use azure_storage::StorageCredentialsInner;

    use super::*;
    use std::convert::TryFrom;

    #[test]
    fn resource_uri_try_from() {
        let uri = "https://storageaccountname.blob.core.windows.com/containerobjectname?sas=token";
        let resource_uri = ResourceUri::try_from(uri).unwrap();

        assert_eq!(
            resource_uri.service_uri,
            "https://storageaccountname.blob.core.windows.com"
        );
        assert_eq!(resource_uri.object_name, "containerobjectname");

        let storage_credential_inner = std::sync::Arc::into_inner(resource_uri.sas_token.0)
            .unwrap()
            .into_inner();
        assert!(matches!(
            storage_credential_inner,
            StorageCredentialsInner::SASToken(_)
        ));

        if let StorageCredentialsInner::SASToken(sas_vec) = storage_credential_inner {
            assert_eq!(sas_vec.len(), 1);
            assert_eq!(sas_vec[0].0, "sas");
            assert_eq!(sas_vec[0].1, "token");
        }
    }

    #[test]
    fn invalid_scheme() {
        let uri = "http://storageaccountname.blob.core.windows.com/containerobjectname?sas=token";
        let resource_uri = ResourceUri::try_from(uri);

        assert!(resource_uri.is_err());
    }

    #[test]
    fn missing_host_str() {
        let uri = "https:";
        let resource_uri = ResourceUri::try_from(uri);
        println!("{:#?}", resource_uri);

        assert!(resource_uri.is_err());
    }

    #[test]
    fn missing_object_name() {
        let uri = "https://storageaccountname.blob.core.windows.com/?sas=token";
        let resource_uri = ResourceUri::try_from(uri);
        println!("{:#?}", resource_uri);

        assert!(resource_uri.is_err());
    }

    #[test]
    fn missing_sas_token() {
        let uri = "https://storageaccountname.blob.core.windows.com/containerobjectname";
        let resource_uri = ResourceUri::try_from(uri);
        println!("{:#?}", resource_uri);

        assert!(resource_uri.is_err());
    }

    #[test]
    fn queue_client_from_resource_uri() {
        let resource_uri = ResourceUri {
            service_uri: "https://mystorageaccount.queue.core.windows.net".to_string(),
            object_name: "queuename".to_string(),
            sas_token: StorageCredentials::sas_token("sas=token").unwrap(),
        };

        let client_options = ClientOptions::default();
        let queue_client = QueueClient::create_client(resource_uri, client_options);

        assert_eq!(queue_client.queue_name(), "queuename");
    }

    #[test]
    fn container_client_from_resource_uri() {
        let resource_uri = ResourceUri {
            service_uri: "https://mystorageaccount.blob.core.windows.net".to_string(),
            object_name: "containername".to_string(),
            sas_token: StorageCredentials::sas_token("sas=token").unwrap(),
        };

        let client_options = ClientOptions::default();
        let container_client = ContainerClient::create_client(resource_uri, client_options);

        assert_eq!(container_client.container_name(), "containername");
    }
}
