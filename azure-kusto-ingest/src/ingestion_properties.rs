use crate::data_format::{DataFormat, IngestionMappingKind};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_repr::Serialize_repr;

#[derive(Clone, Debug, Default)]
pub struct IngestionProperties {
    pub database_name: String,
    pub table_name: String,
    pub retain_blob_on_success: Option<bool>,
    pub data_format: DataFormat,
    // I think we could make this neater by using some enum wizardry to enforce certain checks that are being done currently
    // I'm thinking of something like we give an ingestion mapping enum, with
    pub ingestion_mapping: Option<Vec<ColumnMapping>>,
    pub ingestion_mapping_type: Option<IngestionMappingKind>,
    pub ingestion_mapping_reference: Option<Vec<String>>,
    pub additional_tags: Vec<String>,
    pub ingest_if_not_exists: Vec<String>,
    pub ingest_by_tags: Vec<String>,
    pub drop_by_tags: Vec<String>,
    pub flush_immediately: Option<bool>,
    pub ignore_first_record: bool,
    pub report_level: Option<ReportLevel>,
    pub report_method: Option<ReportMethod>,
    pub validation_policy: Option<ValidationPolicy>,
    pub creation_time: Option<DateTime<Utc>>,
}

#[derive(Serialize, Clone, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct ValidationPolicy {
    validation_options: ValidationOptions,
    validation_implications: ValidationImplications,
}

#[derive(Serialize_repr, Clone, Debug)]
#[repr(u8)]
pub enum ValidationOptions {
    DoNotValidate = 0,
    ValidateCsvInputConstantColumns = 1,
    ValidateCsvInputColumnLevelOnly = 2,
}

#[derive(Serialize_repr, Clone, Debug)]
#[repr(u8)]
pub enum ValidationImplications {
    Fail = 0,
    BestEffort = 1,
}

#[derive(Serialize_repr, Clone, Debug)]
#[repr(u8)]
pub enum ReportLevel {
    Failures = 0,
    None = 1,
    All = 2,
}

#[derive(Serialize_repr, Clone, Debug)]
#[repr(u8)]
pub enum ReportMethod {
    Queue = 0,
    Table = 1,
}

#[derive(Serialize, Clone, Debug)]
pub enum TransformationMethod {
    PropertyBagArrayToDictionary,
    SourceLocation,
    SourceLineNumber,
    DateTimeFromUnixSeconds,
    DateTimeFromUnixMilliseconds,
    DateTimeFromUnixMicroseconds,
    DateTimeFromUnixNanoseconds,
    DropMappedFields,
    BytesAsBase64,
}

/// Use this class to create mappings for IngestionProperties.ingestionMappings and utilize mappings that were not
/// pre-created (it is recommended to create the mappings in advance and use ingestionMappingReference).
/// To read more about mappings look here: https://docs.microsoft.com/en-us/azure/kusto/management/mappings
#[derive(Serialize, Clone, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct ColumnMapping {
    column: String,
    // TODO: can this be an enum?
    data_type: String,
    properties: ColumnMappingProperties,
}

#[derive(Serialize, Clone, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct ColumnMappingProperties {
    path: Option<String>,
    transform: Option<TransformationMethod>,
    // TODO: This should get serialized to a string
    ordinal: Option<u32>,
    const_value: Option<String>,
    field: Option<String>,
}
