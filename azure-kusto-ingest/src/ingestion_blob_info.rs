use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::{
    data_format::DataFormat,
    descriptors::BlobDescriptor,
    ingestion_properties::{IngestionProperties, ReportLevel, ReportMethod, ValidationPolicy},
    resource_manager::KustoIdentityToken,
};

// Basing the ingestion message on
// https://learn.microsoft.com/en-us/azure/data-explorer/kusto/api/netfx/kusto-ingest-client-rest#ingestion-message-internal-structure
#[derive(Serialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct QueuedIngestionMessage {
    /// Message identifier (GUID)
    id: uuid::Uuid,
    /// Path (URI) to the blob, including the SAS key granting permissions to read/write/delete it.
    /// Permissions are required so that the ingestion service can delete the blob once it has completed ingesting the data.
    blob_path: String,
    /// Target database name
    database_name: String,
    /// Target table name
    table_name: String,
    /// Size of the uncompressed data in bytes. Providing this value allows the ingestion service to optimize ingestion by potentially aggregating multiple blobs. This property is optional, but if not given, the service will access the blob just to retrieve the size.
    #[serde(skip_serializing_if = "Option::is_none")]
    raw_data_size: Option<u64>,
    /// If set to `true`, the blob won't be deleted once ingestion is successfully completed. Default is `false`
    #[serde(skip_serializing_if = "Option::is_none")]
    retain_blob_on_success: Option<bool>,
    /// If set to `true`, any aggregation will be skipped. Default is `false`
    #[serde(skip_serializing_if = "Option::is_none")]
    flush_immediately: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ignore_size_limit: Option<bool>,
    // according to Go impl, the report level and method could be Option
    #[serde(skip_serializing_if = "Option::is_none")]
    report_level: Option<ReportLevel>,
    #[serde(skip_serializing_if = "Option::is_none")]
    report_method: Option<ReportMethod>,
    source_message_creation_time: DateTime<Utc>,
    additional_properties: AdditionalProperties,
}

impl QueuedIngestionMessage {
    pub fn new(
        blob_descriptor: &BlobDescriptor,
        ingestion_properties: &IngestionProperties,
        authorization_context: KustoIdentityToken,
    ) -> Self {
        // TODO: processing of additional tags, ingest_by_tags, drop_by_tags into just tags

        let additional_properties = AdditionalProperties {
            ingestion_mapping: None,
            ingestion_mapping_reference: None,
            creation_time: ingestion_properties.creation_time,
            data_format: ingestion_properties.data_format.clone(),
            ingest_if_not_exists: None,
            ignore_first_record: None,
            tags: vec![],
            validation_policy: None,
            authorization_context,
        };

        Self {
            id: blob_descriptor.source_id,
            blob_path: blob_descriptor.uri(),
            raw_data_size: blob_descriptor.size,
            database_name: ingestion_properties.database_name.clone(),
            table_name: ingestion_properties.table_name.clone(),
            retain_blob_on_success: ingestion_properties.retain_blob_on_success,
            flush_immediately: ingestion_properties.flush_immediately,
            report_level: ingestion_properties.report_level.clone(),
            report_method: ingestion_properties.report_method.clone(),
            ignore_size_limit: Some(false),
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
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "ingestionMapping")]
    ingestion_mapping: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "ingestionMappingReference")]
    ingestion_mapping_reference: Option<String>,
    // TODO: is this required?
    // #[serde(skip_serializing_if = "Option::is_none")]
    // #[serde(rename = "ingestionMappingType")]
    // ingestion_mapping_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "creationTime")]
    creation_time: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "ingestIfNotExists")]
    ingest_if_not_exists: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "ignoreFirstRecord")]
    ignore_first_record: Option<bool>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tags: Vec<String>,
    #[serde(rename = "validationPolicy")]
    #[serde(skip_serializing_if = "Option::is_none")]
    validation_policy: Option<ValidationPolicy>,
}
