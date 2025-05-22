//! Models to parse responses from ADX.
use crate::prelude::ClientRequestProperties;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[allow(non_snake_case)]
pub(crate) struct QueryBody {
    /// Name of the database in scope that is the target of the query or control command
    pub db: String,
    /// Text of the query or control command to execute
    pub csl: String,
    /// Additional parameters and options for fine-grained control of the request behavior
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<ClientRequestProperties>,
}

/// Represents the scalar data types of ADX. see [the docs for more information](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/scalar-data-types/)
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub enum ColumnType {
    #[serde(alias = "Boolean", alias = "bool", alias = "boolean", alias = "SByte")]
    /// Boolean type, true or false. Internally is a u8.
    Bool,
    /// Datetime, represents a specific point in time.
    #[serde(alias = "DateTime", alias = "datetime", alias = "Date", alias = "date")]
    Datetime,
    /// A complex type, that is either an array or a dictionary of other values.
    #[serde(alias = "dynamic", alias = "Object", alias = "object")]
    Dynamic,
    /// GUID type, represents a globally unique identifier.
    #[serde(
        alias = "GUID",
        alias = "guid",
        alias = "UUID",
        alias = "uuid",
        alias = "Uuid"
    )]
    Guid,
    #[serde(alias = "Int32", alias = "int32", alias = "int")]
    /// 32 bit integer type.
    Int,
    /// 64 bit integer type.
    #[serde(alias = "Int64", alias = "int64", alias = "long")]
    Long,
    /// 64 bit floating point type.
    #[serde(
        alias = "Real",
        alias = "real",
        alias = "float",
        alias = "Float",
        alias = "Double",
        alias = "double"
    )]
    Real,
    #[serde(alias = "string")]
    /// String type, represents a string of characters.
    String,
    /// Timespan type, represents a duration of time.
    #[serde(alias = "TimeSpan", alias = "timespan", alias = "Time", alias = "time")]
    Timespan,
    #[serde(alias = "decimal")]
    /// Decimal, represents a fixed-point number with a defined precision and scale.
    Decimal,
}

/// Represents a column in ADX, for a V1 (usually management) query.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct ColumnV1 {
    /// Name of the column.
    pub column_name: String,
    /// Data type of the column
    pub column_type: Option<ColumnType>,
    /// Data type of the column
    pub data_type: Option<ColumnType>,
}

/// Represents a table in ADX, for a V1 (usually management) query.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct TableV1 {
    /// Name of the table.
    pub table_name: String,
    /// Columns in the table.
    pub columns: Vec<ColumnV1>,
    /// Rows in the table. Each row is a list of values, corresponding to the columns in the table.
    pub rows: Vec<Vec<serde_json::Value>>,
}

/// The header of the V2 query response.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct DataSetHeader {
    /// Is the table progressive. If it is, tables can arrive in multiple chunks.
    /// To control this, use the [RequestOptions.results_progressive_enabled] parameter.
    pub is_progressive: bool,
    /// Version of the header. Currently it is always `v2.0`.
    pub version: String,
}

/// A result of a V2 query.
/// Could be a table, a part of a table, or metadata about the dataset.
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "PascalCase", tag = "FrameType")]
#[allow(clippy::enum_variant_names)]
pub enum V2QueryResult {
    /// The header of the dataset.
    DataSetHeader(DataSetHeader),
    /// A table in the dataset.
    DataTable(DataTable),
    /// The final result in the dataset.
    DataSetCompletion(DataSetCompletion),
    /// A header of a table (in progressive mode).
    TableHeader(TableHeader),
    /// A part of a table (in progressive mode).
    TableFragment(TableFragment),
    /// Progress report for a table (in progressive mode).
    TableProgress(TableProgress),
    /// End of a table (in progressive mode).
    TableCompletion(TableCompletion),
}

/// Query result DataTable, for a V2 Query.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct DataTable {
    /// Table id - unique identifier of the table.
    pub table_id: i32,
    /// Table name.
    pub table_name: String,
    /// Table kind - will be `PrimaryResults` for the actual query result, or other kinds for metadata.
    pub table_kind: TableKind,
    /// Columns in the table.
    pub columns: Vec<Column>,
    /// Rows in the table. Each row is a list of values, corresponding to the columns in the table.
    pub rows: Vec<serde_json::Value>,
}

/// A header of a fragment of a table (in progressive mode).
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct TableHeader {
    /// Table id - unique identifier of the table.
    pub table_id: i32,
    /// Table name.
    pub table_name: String,
    /// Table kind - will be `PrimaryResults` for the actual query result, or other kinds for metadata.
    pub table_kind: TableKind,
    /// Columns in the table.
    pub columns: Vec<Column>,
}

/// The type of the fragment of a table (in progressive mode), instructs to how to use it.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub enum TableFragmentType {
    /// Append the data to the rest of the fragments.
    DataAppend,
    /// Replace all previous data with this fragment.
    DataReplace,
}

/// Represents a fragment of a table (in progressive mode).
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct TableFragment {
    /// Table id - unique identifier of the table. Corresponds to the table_id in the TableHeader.
    pub table_id: i32,
    /// The amount of fields
    pub field_count: Option<i32>,
    /// The type of the fragment, instructs to how to use it.
    pub table_fragment_type: TableFragmentType,
    /// Rows in the table. Each row is a list of values, corresponding to the columns in the TableHeader.
    pub rows: Vec<serde_json::Value>,
}

/// Progress report for a table (in progressive mode).
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct TableProgress {
    /// Table id - unique identifier of the table. Corresponds to the table_id in the TableHeader.
    pub table_id: i32,
    /// Percentage of the progress so far.
    pub table_progress: f64,
}

/// End of a table (in progressive mode).
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct TableCompletion {
    /// Table id - unique identifier of the table. Corresponds to the table_id in the TableHeader.
    pub table_id: i32,
    /// Total row count
    pub row_count: i32,
}

/// Categorizes data tables according to the role they play in the data set that a Kusto query returns.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub enum TableKind {
    /// The table contains the actual data returned by the query.
    PrimaryResult,
    /// Information about the runtime of query.
    QueryCompletionInformation,
    /// Trace Log for the query.
    QueryTraceLog,
    /// Perf log for the query.
    QueryPerfLog,
    /// Table of contents for the other parts.
    TableOfContents,
    /// Properties of the query.
    QueryProperties,
    /// Execution plan for the query.
    QueryPlan,
    /// Unknown table kind.
    Unknown,
}
/// Represents a column in ADX, for a V2 query.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct Column {
    /// Column name.
    pub column_name: String,
    /// Data type of the column.
    pub column_type: ColumnType,
}

/// Represents an end of the query result.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct DataSetCompletion {
    /// did the query errored.
    pub has_errors: bool,
    /// Was the query cancelled.
    pub cancelled: bool,
}
