use crate::models::v2::consts::{ErrorReportingPlacement, TableFragmentType, TableKind};
use crate::models::v2::errors::OneApiError;
use crate::models::v2::{Column, Row};
use serde::{Deserialize, Serialize};

/// The header of the V2 query response.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct DataSetHeader {
    /// Is the table progressive. If it is, tables can arrive in multiple chunks.
    /// To control this, use the [RequestOptions.results_progressive_enabled] parameter.
    pub is_progressive: bool,
    /// Version of the header. Currently it is always `v2.0`.
    pub version: String,
    /// Whether or not the query fragmeneted the main table.
    pub is_fragmented: Option<bool>,
    /// Errors location.
    pub error_reporting_placement: Option<ErrorReportingPlacement>,
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
    /// Rows in the table. Each row is a list of values, corresponding to the columns in the table, or an error.
    pub rows: Vec<Row>,
}

/// A header of a fragment of a table
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

/// Represents a fragment of a table (in progressive mode).
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct TableFragment {
    /// Table id - unique identifier of the table. Corresponds to the table_id in the TableHeader.
    pub table_id: i32,
    /// The type of the fragment, instructs to how to use it.
    pub table_fragment_type: TableFragmentType,
    /// Rows in the table. Each row is a list of values, corresponding to the columns in the TableHeader, or an error.
    pub rows: Vec<Row>,
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
    /// Errors in the table - if any.
    pub one_api_errors: Option<Vec<OneApiError>>,
}

/// Represents an end of the query result.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct DataSetCompletion {
    /// did the query errored.
    pub has_errors: bool,
    /// Was the query cancelled.
    pub cancelled: bool,
    /// Errors in the query - if any.
    pub one_api_errors: Option<Vec<OneApiError>>,
}
