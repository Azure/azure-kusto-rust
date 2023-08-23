use serde::Serialize;

#[derive(Serialize, Clone, Debug, Default)]
pub enum IngestionMappingKind {
    #[serde(rename = "Csv")]
    #[default]
    CSV,
    #[serde(rename = "Json")]
    JSON,
    Avro,
    ApacheAvro,
    Parquet,
    SStream,
    #[serde(rename = "Orc")]
    ORC,
    #[serde(rename = "W3CLogFile")]
    W3CLOGFILE,
    Unknown,
}

/// All data formats supported by Kusto
#[derive(Serialize, Clone, Debug)]
pub enum DataFormat {
    #[serde(rename = "apacheavro")]
    ApacheAvro,
    #[serde(rename = "avro")]
    Avro,
    #[serde(rename = "csv")]
    CSV,
    #[serde(rename = "json")]
    JSON,
    #[serde(rename = "multijson")]
    MultiJSON,
    #[serde(rename = "orc")]
    ORC,
    #[serde(rename = "parquet")]
    Parquet,
    #[serde(rename = "psv")]
    PSV,
    #[serde(rename = "raw")]
    RAW,
    #[serde(rename = "scsv")]
    SCSV,
    #[serde(rename = "sohsv")]
    SOHsv,
    #[serde(rename = "singlejson")]
    SingleJSON,
    #[serde(rename = "sstream")]
    SStream,
    #[serde(rename = "tsv")]
    TSV,
    #[serde(rename = "tsve")]
    TSVe,
    #[serde(rename = "txt")]
    TXT,
    #[serde(rename = "w3clogfile")]
    W3CLOGFILE,
}

impl DataFormat {
    pub fn ingestion_mapping_kind(self) -> IngestionMappingKind {
        match self {
            DataFormat::CSV => IngestionMappingKind::CSV,
            DataFormat::TSV => IngestionMappingKind::CSV,
            DataFormat::SCSV => IngestionMappingKind::CSV,
            DataFormat::SOHsv => IngestionMappingKind::CSV,
            DataFormat::PSV => IngestionMappingKind::CSV,
            DataFormat::TXT => IngestionMappingKind::CSV,
            DataFormat::TSVe => IngestionMappingKind::CSV,
            DataFormat::JSON => IngestionMappingKind::JSON,
            DataFormat::SingleJSON => IngestionMappingKind::JSON,
            DataFormat::MultiJSON => IngestionMappingKind::JSON,
            DataFormat::Avro => IngestionMappingKind::Avro,
            DataFormat::ApacheAvro => IngestionMappingKind::ApacheAvro,
            DataFormat::Parquet => IngestionMappingKind::Parquet,
            DataFormat::SStream => IngestionMappingKind::SStream,
            DataFormat::ORC => IngestionMappingKind::ORC,
            DataFormat::RAW => IngestionMappingKind::CSV,
            DataFormat::W3CLOGFILE => IngestionMappingKind::W3CLOGFILE,
        }
    }

    /// Binary formats should not be compressed
    pub fn compressible(self) -> bool {
        match self {
            DataFormat::CSV => true,
            DataFormat::TSV => true,
            DataFormat::SCSV => true,
            DataFormat::SOHsv => true,
            DataFormat::PSV => true,
            DataFormat::TXT => true,
            DataFormat::TSVe => true,
            DataFormat::JSON => true,
            DataFormat::SingleJSON => true,
            DataFormat::MultiJSON => true,
            DataFormat::Avro => true,
            DataFormat::ApacheAvro => true,
            DataFormat::Parquet => false,
            DataFormat::SStream => false,
            DataFormat::ORC => false,
            DataFormat::RAW => true,
            DataFormat::W3CLOGFILE => true,
        }
    }
}
