use crate::models::v2::errors::OneApiError;
use crate::models::ColumnType;
use serde::{Deserialize, Serialize};

mod consts;
mod errors;
mod frames;

pub use consts::*;
pub use errors::*;
pub use frames::*;

/// A result of a V2 query.
/// Could be a table, a part of a table, or metadata about the dataset.
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "PascalCase", tag = "FrameType")]
#[allow(clippy::enum_variant_names)]
pub enum Frame {
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

/// Represents a column in ADX, for a V2 query.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct Column {
    /// Column name.
    pub column_name: String,
    /// Data type of the column.
    pub column_type: ColumnType,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(untagged)]
pub enum Row {
    /// A row in a table.
    Values(Vec<serde_json::Value>),
    /// An error in a table.
    Error(OneApiError),
}

pub type DataSet = Vec<Frame>;
