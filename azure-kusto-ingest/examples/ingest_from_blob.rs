use std::env;

use anyhow::Result;
use azure_kusto_data::prelude::{ConnectionString, KustoClient, KustoClientOptions};
use azure_kusto_ingest::data_format::DataFormat;
use azure_kusto_ingest::descriptors::{BlobAuth, BlobDescriptor};
use azure_kusto_ingest::ingestion_properties::IngestionProperties;
use azure_kusto_ingest::queued_ingest::QueuedIngestClient;

/// Example of ingesting data into Kusto from Azure Blob Storage using managed identities.
/// This example enforces that the Kusto cluster has a system assigned managed identity with access to the storage account
///
/// There are some steps that need to be taken to allow for managed identities to work:
/// - Permissions as the ingestor to initiate ingestion
///     https://learn.microsoft.com/en-us/azure/data-explorer/kusto/api/netfx/kusto-ingest-client-permissions
/// - Permissions for Kusto to access storage
///     https://learn.microsoft.com/en-us/azure/data-explorer/ingest-data-managed-identity
#[tokio::main]
async fn main() -> Result<()> {
    let cluster_ingest_uri = env::var("KUSTO_INGEST_URI").expect("Must define KUSTO_INGEST_URI");
    let user_mi_object_id =
        env::var("KUSTO_USER_MI_OBJECT_ID").expect("Must define KUSTO_USER_MI_OBJECT_ID");

    // Create a Kusto client with managed identity authentication via the user assigned identity
    let kusto_client = KustoClient::new(
        ConnectionString::with_managed_identity_auth(cluster_ingest_uri, Some(user_mi_object_id)),
        KustoClientOptions::default(),
    )?;

    // Create a queued ingest client
    let queued_ingest_client = QueuedIngestClient::new(kusto_client);

    // Define ingestion properties
    let ingestion_properties = IngestionProperties {
        database_name: env::var("KUSTO_DATABASE_NAME").expect("Must define KUSTO_DATABASE_NAME"),
        table_name: env::var("KUSTO_TABLE_NAME").expect("Must define KUSTO_TABLE_NAME"),
        // Don't delete the blob on successful ingestion
        retain_blob_on_success: Some(true),
        // File format of the blob is Parquet
        data_format: DataFormat::Parquet,
        // Assume the server side default for flush_immediately
        flush_immediately: None,
    };

    // Define the blob to ingest from
    let blob_uri = env::var("BLOB_URI").expect("Must define BLOB_URI");
    // Define the size of the blob if known, this improves ingestion performance as Kusto does not need to access the blob to determine the size
    let blob_size: Option<u64> = match env::var("BLOB_SIZE") {
        Ok(blob_size) => Some(blob_size.parse().expect("BLOB_SIZE must be a valid u64")),
        Err(_) => None,
    };

    // Create the blob descriptor, also specifying that the blob should be accessed using the system assigned managed identity of the Kusto cluster
    let blob_descriptor = BlobDescriptor::new(blob_uri, blob_size, None)
        .with_blob_auth(BlobAuth::SystemAssignedManagedIdentity);

    queued_ingest_client
        .ingest_from_blob(blob_descriptor, ingestion_properties)
        .await
}
