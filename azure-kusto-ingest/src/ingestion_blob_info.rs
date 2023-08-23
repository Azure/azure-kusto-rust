use std::collections::HashMap;

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
    id: uuid::Uuid,
    blob_path: String,
    database_name: String,
    table_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    raw_data_size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retain_blob_on_success: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    flush_immediately: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ignore_size_limit: Option<bool>,
    // according to Go impl, the report level and method could be Option
    report_level: ReportLevel,
    report_method: ReportMethod,
    // TODO: implement this
    // #[serde(skip_serializing_if = "Option::is_none")]s
    // #[serde(skip_serializing_if = "Option::is_none")]
    // #[serde(with= "time::serde::iso8601")]
    source_message_creation_time: String,
    // The additional properties struct is modelled on:
    // https://learn.microsoft.com/en-us/azure/data-explorer/ingestion-properties
    additional_properties: AdditionalProperties,
}

impl QueuedIngestionMessage {
    pub fn new(
        blob_descriptor: &BlobDescriptor,
        ingestion_properties: &IngestionProperties,
        authorization_context: KustoIdentityToken,
    ) -> Self {
        let additional_properties = AdditionalProperties {
            ingestion_mapping: None,
            ingestion_mapping_reference: None,
            creation_time: None,
            extend_schema: None,
            folder: None,
            data_format: ingestion_properties.data_format.clone(),
            ingest_if_not_exists: None,
            ignore_first_record: None,
            policy_ingestiontime: None,
            recreate_schema: None,
            tags: vec![],
            validation_policy: None,
            zip_pattern: None,
            authorization_context,
            extra_additional_properties: HashMap::new(),
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
            // TODO: configurability of creation time
            source_message_creation_time: String::from("2023-08-16T13:30:04.639714"),
            additional_properties,
        }
    }
}

// The additional properties struct is modelled on: https://learn.microsoft.com/en-us/azure/data-explorer/ingestion-properties
#[derive(Serialize, Clone, Debug)]
pub struct AdditionalProperties {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "ingestionMapping")]
    pub ingestion_mapping: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "ingestionMappingReference")]
    pub ingestion_mapping_reference: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "creationTime")]
    pub creation_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extend_schema: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub folder: Option<String>,
    #[serde(rename = "format")]
    pub data_format: DataFormat,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "ingestIfNotExists")]
    pub ingest_if_not_exists: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "ignoreFirstRecord")]
    pub ignore_first_record: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy_ingestiontime: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recreate_schema: Option<bool>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    #[serde(rename = "validationPolicy")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validation_policy: Option<ValidationPolicy>,
    #[serde(rename = "zipPattern")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub zip_pattern: Option<String>,
    // TODO: the user shouldn't be able to set this, we should expose certain properties via IngestionProperties rather than just the AdditionalProperties struct
    #[serde(rename = "authorizationContext")]
    pub authorization_context: KustoIdentityToken,
    #[serde(flatten)]
    pub extra_additional_properties: HashMap<String, String>,
}
