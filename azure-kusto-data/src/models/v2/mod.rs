use crate::models::ColumnType;
use serde::{Deserialize, Serialize};

mod consts;
mod errors;
mod frames;
mod known_tables;

pub use consts::*;
pub use errors::*;
pub use frames::*;
pub use known_tables::*;
use crate::error::{Error, Partial};

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
    Error(OneApiErrors),
}

impl Into<Result<Vec<serde_json::Value>, Error>> for Row {
    fn into(self) -> Result<Vec<serde_json::Value>, Error> {
        match self {
            Row::Values(v) => Ok(v),
            Row::Error(e) => Err(e.errors.into()),
        }
    }
}

impl Row {
    pub fn into_result(self) -> Result<Vec<serde_json::Value>, Error> {
        self.into()
    }
}

impl DataTable {
    pub fn collect_values(&self) -> Partial<serde_json::Value> {
        let mut errors = vec![];
        let mut values = vec![];
        for row in &self.rows {
            match row.clone().into_result() {
                Ok(v) => values.push(serde_json::Value::Array(v)),
                Err(e) => match e {
                    Error::MultipleErrors(e) => errors.extend(e),
                    _ => errors.push(e),
                }
            }
        }
        match (values.len(), errors.len()) {
            (0, _) => Err((None, errors.into())),
            (_, 0) => Ok(serde_json::Value::Array(values)),
            (_, _) => Err((Some(serde_json::Value::Array(values)), errors.into())),
        }
    }

    pub fn deserialize_values<T: serde::de::DeserializeOwned>(&self) -> Partial<Vec<T>> {
        let mut errors = vec![];
        let mut values = vec![];
        for row in &self.rows {
            match row.clone().into_result() {
                Ok(v) => match serde_json::from_value::<T>(serde_json::Value::Array(v)) {
                    Ok(v) => values.push(v),
                    Err(e) => errors.push(e.into()),
                },
                Err(e) => match e {
                    Error::MultipleErrors(e) => errors.extend(e),
                    _ => errors.push(e),
                }
            }
        }

        match (values.len(), errors.len()) {
            (0, _) => Err((None, errors.into())),
            (_, 0) => Ok(values),
            (_, _) => Err((Some(values), errors.into())),
        }
    }
}

pub type DataSet = Vec<Frame>;
