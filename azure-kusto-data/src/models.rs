use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct QueryBody {
    /// Name of the database in scope that is the target of the query or control command
    pub db: String,
    /// Text of the query or control command to execute
    pub csl: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum ColumnType {
    Bool,
    Boolean,
    Datetime,
    Date,
    Dynamic,
    Guid,
    Int,
    Long,
    Real,
    String,
    Timespan,
    Time,
    Decimal,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct ColumnV1 {
    pub column_name: String,
    pub column_type: Option<ColumnType>,
    pub data_type: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct TableV1 {
    pub table_name: String,
    pub columns: Vec<ColumnV1>,
    pub rows: Vec<Vec<serde_json::Value>>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct DataSetHeader {
    pub is_progressive: bool,
    pub version: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct DataTable {
    pub table_id: i32,
    pub table_name: String,
    pub table_kind: TableKind,
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<serde_json::Value>>,
}

/// Categorizes data tables according to the role they play in the data set that a Kusto query returns.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum TableKind {
    PrimaryResult,
    QueryCompletionInformation,
    QueryTraceLog,
    QueryPerfLog,
    TableOfContents,
    QueryProperties,
    QueryPlan,
    Unknown,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct Column {
    pub column_name: String,
    pub column_type: ColumnType,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct DataSetCompletion {
    pub has_errors: bool,
    pub cancelled: bool,
}
