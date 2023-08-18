use crate::data_format::{DataFormat, IngestionMappingKind};
use serde::Serialize;
use serde_repr::Serialize_repr;

#[derive(Clone, Debug)]
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
    pub report_level: ReportLevel,
    pub report_method: ReportMethod,
    pub validation_policy: Option<ValidationPolicy>,
    // TODO: don't expose AdditionalProperties to user...
    // pub additional_properties: AdditionalProperties,
    // pub additional_properties: AdditionalProperties,
}

#[derive(Serialize, Clone, Debug)]
pub struct ValidationPolicy {
    #[serde(rename = "ValidationOptions")]
    validation_options: ValidationOptions,
    #[serde(rename = "ValidationImplications")]
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
pub struct ColumnMapping {
    #[serde(rename = "Column")]
    column: String,
    // TODO: can this be an enum?
    #[serde(rename = "DataType")]
    datatype: String,
    #[serde(rename = "Properties")]
    properties: ColumnMappingProperties,
}

#[derive(Serialize, Clone, Debug)]
pub struct ColumnMappingProperties {
    #[serde(rename = "Path")]
    path: Option<String>,
    #[serde(rename = "Transform")]
    transform: Option<TransformationMethod>,
    #[serde(rename = "Ordinal")]
    // TODO: This should get serialized to a string
    ordinal: Option<u32>,
    #[serde(rename = "ConstValue")]
    const_value: Option<String>,
    #[serde(rename = "Field")]
    field: Option<String>,
}
