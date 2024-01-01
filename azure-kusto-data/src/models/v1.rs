use serde::{Deserialize, Serialize};
use crate::models::ColumnType;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(rename_all = "PascalCase")]
/// The header of a Kusto response dataset for v1. Contains a list of tables.
pub struct Dataset {
    /// The list of tables in the dataset.
    pub tables: Vec<Table>,
}

impl Dataset {
    #[must_use]
    /// Count the number of tables in the dataset.
    /// # Example
    /// ```rust
    /// use azure_kusto_data::models::TableV1;
    /// use azure_kusto_data::prelude::KustoResponseDataSetV1;
    /// let dataset = KustoResponseDataSetV1 {
    ///    tables: vec![
    ///       TableV1 {
    ///         table_name: "table_1".to_string(),
    ///         columns: vec![],
    ///         rows: vec![],
    ///      },
    /// ]};
    ///
    /// assert_eq!(dataset.table_count(), 1);
    ///
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }
}

/// Represents a column in ADX, for a V1 (usually management) query.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct Column {
    /// Name of the column.
    pub column_name: String,
    /// Data type of the column
    pub column_type: ColumnType,
    /// Type of the column in .net.
    pub data_type: Option<String>,
}

/// Represents a table in ADX, for a V1 (usually management) query.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct Table {
    /// Name of the table.
    pub table_name: String,
    /// Columns in the table.
    pub columns: Vec<Column>,
    /// Rows in the table. Each row is a list of values, corresponding to the columns in the table.
    pub rows: Vec<Vec<serde_json::Value>>,
}
