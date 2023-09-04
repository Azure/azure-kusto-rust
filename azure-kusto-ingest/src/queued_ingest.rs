use std::sync::Arc;

use anyhow::Result;
use azure_core::base64;
use azure_kusto_data::prelude::KustoClient;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;

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
    /// Creates a new client from the given [KustoClient]
    pub fn new(kusto_client: KustoClient) -> Self {
        Self::new_with_client_options(kusto_client, QueuedIngestClientOptions::default())
    }

    /// Creates a new client from the given [KustoClient] and [QueuedIngestClientOptions]
    /// This allows for customisation of the [ClientOptions] used for the storage clients
    pub fn new_with_client_options(
        kusto_client: KustoClient,
        options: QueuedIngestClientOptions,
    ) -> Self {
        // TODO: add a validation check that the client provided is against the ingestion endpoint
        // kusto_client.management_url()

        let resource_manager = Arc::new(ResourceManager::new(kusto_client, options));

        Self { resource_manager }
    }

    /// Ingest a file into Kusto from Azure Blob Storage
    pub async fn ingest_from_blob(
        &self,
        blob_descriptor: BlobDescriptor,
        ingestion_properties: IngestionProperties,
    ) -> Result<()> {
        let ingestion_queues = self.resource_manager.ingestion_queues().await?;
        // println!("queues: {:#?}", ingestion_queues);

        let auth_context = self.resource_manager.authorization_context().await?;
        // println!("auth_context: {:#?}\n", auth_context);

        let message =
            QueuedIngestionMessage::new(&blob_descriptor, &ingestion_properties, auth_context);
        // println!("message as struct: {:#?}\n", message);

        // Pick a random queue from the queue clients returned by the resource manager
        let mut rng: StdRng = SeedableRng::from_entropy();
        let queue_client = ingestion_queues
            .choose(&mut rng)
            .ok_or(anyhow::anyhow!("Failed to pick a random queue"))?;
        // println!("queue_client: {:#?}\n", queue_client);

        let message = serde_json::to_string(&message).unwrap();
        // println!("message as string: {}\n", message);

        // Base64 encode the ingestion message
        let message = base64::encode(&message);
        // println!("message as base64 encoded string: {}\n", message);

        let _resp = queue_client.put_message(message).await?;
        // println!("resp: {:#?}\n", resp);

        Ok(())
    }
}
