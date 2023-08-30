use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::{
    data_format::DataFormat, descriptors::BlobDescriptor,
    ingestion_properties::IngestionProperties,
    resource_manager::authorization_context::KustoIdentityToken,
};

// Basing the ingestion message on
// https://learn.microsoft.com/en-us/azure/data-explorer/kusto/api/netfx/kusto-ingest-client-rest#ingestion-message-internal-structure
#[derive(Serialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct QueuedIngestionMessage {
    /// Message identifier for this upload
    id: uuid::Uuid,
    /// Path (URI) to the blob, including the SAS key granting permissions to read/write/delete it.
    /// Permissions are required so that the ingestion service can delete the blob once it has completed ingesting the data.
    blob_path: String,
    // Name of the Kusto database the data will ingest into
    database_name: String,
    // Name of the Kusto table the the data will ingest into
    table_name: String,
    /// Size of the uncompressed data in bytes.
    /// Providing this value allows the ingestion service to optimize ingestion by potentially aggregating multiple blobs.
    /// Although this property is optional, it is recommended to provide the size as otherwise the service will access the blob just to retrieve the size.
    #[serde(skip_serializing_if = "Option::is_none")]
    raw_data_size: Option<u64>,
    /// If set to `true`, the blob won't be deleted once ingestion is successfully completed. Default is `false`
    #[serde(skip_serializing_if = "Option::is_none")]
    retain_blob_on_success: Option<bool>,
    /// If set to `true`, any server side aggregation will be skipped. Default is `false`
    #[serde(skip_serializing_if = "Option::is_none")]
    flush_immediately: Option<bool>,
    source_message_creation_time: DateTime<Utc>,
    additional_properties: AdditionalProperties,
}

impl QueuedIngestionMessage {
    pub fn new(
        blob_descriptor: &BlobDescriptor,
        ingestion_properties: &IngestionProperties,
        authorization_context: KustoIdentityToken,
    ) -> Self {
        let additional_properties = AdditionalProperties {
            authorization_context,
            data_format: ingestion_properties.data_format.clone(),
        };

        Self {
            id: blob_descriptor.source_id,
            blob_path: blob_descriptor.uri(),
            raw_data_size: blob_descriptor.size,
            database_name: ingestion_properties.database_name.clone(),
            table_name: ingestion_properties.table_name.clone(),
            retain_blob_on_success: ingestion_properties.retain_blob_on_success,
            flush_immediately: ingestion_properties.flush_immediately,
            source_message_creation_time: Utc::now(),
            additional_properties,
        }
    }
}

// The additional properties struct is modelled on: https://learn.microsoft.com/en-us/azure/data-explorer/ingestion-properties
#[derive(Serialize, Clone, Debug)]
struct AdditionalProperties {
    #[serde(rename = "authorizationContext")]
    authorization_context: KustoIdentityToken,
    #[serde(rename = "format")]
    data_format: DataFormat,
}
