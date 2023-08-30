use anyhow::Result;
use azure_kusto_data::prelude::{ConnectionString, KustoClient, KustoClientOptions};
use azure_kusto_ingest::data_format::DataFormat;
use azure_kusto_ingest::descriptors::{BlobAuth, BlobDescriptor};
use azure_kusto_ingest::ingestion_properties::IngestionProperties;
use azure_kusto_ingest::queued_ingest::QueuedIngestClient;

/// Example of ingesting data into Kusto from Azure Blob Storage using managed identities
///
/// There are some steps that need to be taken to allow for managed identities to work:
/// - Permissions as the ingestor to initiate ingestion
///     https://learn.microsoft.com/en-us/azure/data-explorer/kusto/api/netfx/kusto-ingest-client-permissions
/// - Permissions for Kusto to access storage
///     https://learn.microsoft.com/en-us/azure/data-explorer/ingest-data-managed-identity
#[tokio::main]
async fn main() -> Result<()> {
    let cluster_uri = "https://ingest-<cluster-name>.<region>.kusto.windows.net";
    let user_mi_object_id = "<managed-identity-object-id>";

    // Create a Kusto client with managed identity authentication via the user assigned identity
    // Note that this requires
    let kusto_client = KustoClient::new(
        ConnectionString::with_managed_identity_auth(
            cluster_uri,
            Some(user_mi_object_id.to_string()),
        ),
        KustoClientOptions::default(),
    )?;

    // Create a queued ingest client
    let queued_ingest_client = QueuedIngestClient::new(kusto_client);

    // Define ingestion properties
    let ingestion_properties = IngestionProperties {
        database_name: "<database-name>".into(),
        table_name: "<table-name>".into(),
        // Don't delete the blob on successful ingestion
        retain_blob_on_success: Some(true),
        // File format of the blob is Parquet
        data_format: DataFormat::Parquet,
        // Assume the server side default for flush_immediately
        flush_immediately: None,
    };

    // Define the blob to ingest from
    let blob_uri = "https://<storage-account>.blob.core.windows.net/<path-to-blob>";
    // Define the size of the blob if known, this improves ingestion performance as Kusto does not need to access the blob to determine the size
    let blob_size = 123;
    // Create the blob descriptor, also specifying that the blob should be accessed using the system assigned managed identity of the Kusto cluster
    let blob_descriptor = BlobDescriptor::new(blob_uri.to_string(), Some(blob_size), None)
        .with_blob_auth(BlobAuth::SystemAssignedManagedIdentity);

    queued_ingest_client
        .ingest_from_blob(blob_descriptor, ingestion_properties)
        .await
}
