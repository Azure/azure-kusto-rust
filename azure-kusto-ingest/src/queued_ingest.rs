use std::sync::Arc;

use anyhow::Result;
use azure_core::base64;
use azure_kusto_data::prelude::KustoClient;

use crate::descriptors::{BlobDescriptor, FileDescriptor, StreamDescriptor};
use crate::ingestion_blob_info::QueuedIngestionMessage;
use crate::ingestion_properties::IngestionProperties;
use crate::resource_manager::ResourceManager;
use crate::result::{IngestionResult, IngestionStatus};

#[derive(Clone)]
pub struct QueuedIngestClient {
    // The KustoClient is used to get the ingestion resources, it should be a client against the ingestion cluster endpoint
    // kusto_client: KustoClient,
    resource_manager: Arc<ResourceManager>,
}

impl QueuedIngestClient {
    pub fn new(kusto_client: KustoClient) -> Self {
        let resource_manager = Arc::new(ResourceManager::new(kusto_client));

        Self { resource_manager }
    }

    pub async fn ingest_from_blob(
        self,
        blob_descriptor: BlobDescriptor,
        ingestion_properties: &IngestionProperties,
    ) -> Result<IngestionResult> {
        // The queues returned here should ideally be the storage queue client from azure-storage-queue
        // As such, it may be better for ResourceManager to return a struct that contains the storage queue client
        let ingestion_queues = self
            .resource_manager
            .secured_ready_for_aggregation_queues()
            .await?;
        // println!("queues: {:#?}", ingestion_queues);

        let auth_context = self.resource_manager.authorization_context().await?;
        // println!("auth_context: {:#?}\n", auth_context);

        let message = QueuedIngestionMessage::new(
            blob_descriptor.clone(),
            ingestion_properties,
            auth_context,
        );

        // println!("message as struct: {:#?}\n", message);

        // TODO: pick a random queue from the queue clients returned by the resource manager
        let queue_client = ingestion_queues.first().unwrap().clone();
        // println!("queue_client: {:#?}\n", queue_client);

        let message = serde_json::to_string(&message).unwrap();
        // println!("message as string: {}\n", message);
        // Base64 encode the ingestion message
        let message = base64::encode(&message);
        // println!("message as base64 encoded string: {}\n", message);

        let resp = queue_client.put_message(message).await?;

        // println!("resp: {:#?}\n", resp);

        Ok(IngestionResult::new(
            IngestionStatus::Queued,
            &ingestion_properties.database_name,
            &ingestion_properties.table_name,
            blob_descriptor.source_id,
            Some(blob_descriptor.uri()),
        ))
    }

    pub async fn ingest_from_file(
        self,
        file_descriptor: FileDescriptor,
        ingestion_properties: IngestionProperties,
    ) -> Result<IngestionResult> {
        unimplemented!()
        // This function needs to upload the blob from the file, and then call on ingest_from_blob

        // self.ingest_from_blob(blob_descriptor, &ingestion_properties)
        //     .await
    }

    pub async fn ingest_from_stream(
        self,
        stream_descriptor: StreamDescriptor,
        ingestion_properties: IngestionProperties,
    ) -> Result<IngestionResult> {
        unimplemented!()
        // This function needs to upload the blob from the stream, and then call on ingest_from_blob

        // self.ingest_from_blob(blob_descriptor, &ingestion_properties)
        //     .await
    }

    async fn upload_from_different_descriptor(
        self,
        descriptor: FileDescriptor,
        ingestion_properties: &IngestionProperties,
    ) -> Result<BlobDescriptor> {
        unimplemented!()
        // WIP
        // let blob_name = format!(
        //     "{database_name}_{table_name}_{source_id}_{stream_name}",
        //     database_name = ingestion_properties.database_name,
        //     table_name = ingestion_properties.table_name,
        //     source_id = descriptor.source_id,
        //     stream_name = descriptor.stream_name.to_str().unwrap().to_string()
        // );

        // let container_clients = self.resource_manager.temp_storage().await?;
        // // TODO: pick a random container client from the container clients returned by the resource manager
        // let container_client = container_clients.first().unwrap().clone();
        // let blob_client = container_client.blob_client(blob_name);

        // blob_client.put_block_blob(body)

        // blob_url = "";

        // Ok(BlobDescriptor::new(
        //     blob_url,
        //     ingestion_properties.source_id,
        // ))
    }
}
