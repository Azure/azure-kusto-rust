use std::sync::Arc;

use crate::error::Result;
use azure_core::base64;
use azure_kusto_data::prelude::KustoClient;

use crate::client_options::QueuedIngestClientOptions;
use crate::descriptors::BlobDescriptor;
use crate::ingestion_blob_info::QueuedIngestionMessage;
use crate::ingestion_properties::IngestionProperties;
use crate::resource_manager::ResourceManager;

/// Client for ingesting data into Kusto using the queued flavour of ingestion
#[derive(Clone)]
pub struct QueuedIngestClient {
    resource_manager: Arc<ResourceManager>,
}

impl QueuedIngestClient {
    /// Creates a new client from the given [KustoClient].
    ///
    /// **WARNING**: the [KustoClient] must be created with a connection string that points to the ingestion endpoint
    pub fn new(kusto_client: KustoClient) -> Self {
        Self::new_with_client_options(kusto_client, QueuedIngestClientOptions::default())
    }

    /// Creates a new client from the given [KustoClient] and [QueuedIngestClientOptions]
    /// This allows for customisation of the [ClientOptions] used for the storage clients
    ///
    /// **WARNING**: the [KustoClient] must be created with a connection string that points to the ingestion endpoint
    pub fn new_with_client_options(
        kusto_client: KustoClient,
        options: QueuedIngestClientOptions,
    ) -> Self {
        Self {
            resource_manager: Arc::new(ResourceManager::new(kusto_client, options)),
        }
    }

    /// Ingest a file into Kusto from Azure Blob Storage
    pub async fn ingest_from_blob(
        &self,
        blob_descriptor: BlobDescriptor,
        ingestion_properties: IngestionProperties,
    ) -> Result<()> {
        let queue_client = self.resource_manager.ingestion_queue().await?;

        let auth_context = self.resource_manager.authorization_context().await?;

        let message =
            QueuedIngestionMessage::new(&blob_descriptor, &ingestion_properties, auth_context);

        let message = serde_json::to_string(&message)?;

        // Base64 encode the ingestion message
        let message = base64::encode(&message);

        let _resp = queue_client.put_message(message).await?;

        Ok(())
    }
}
