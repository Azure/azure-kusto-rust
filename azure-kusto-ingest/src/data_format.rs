use serde::Serialize;

/// All data formats supported by Kusto.
/// Default is [DataFormat::CSV]
#[derive(Serialize, Clone, Debug, Default, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum DataFormat {
    ApacheAvro,
    Avro,
    #[default]
    CSV,
    JSON,
    MultiJSON,
    ORC,
    Parquet,
    PSV,
    RAW,
    SCSV,
    SOHsv,
    SingleJSON,
    SStream,
    TSV,
    TSVe,
    TXT,
    W3CLOGFILE,
}

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_format_default() {
        assert_eq!(DataFormat::default(), DataFormat::CSV);
    }
}
