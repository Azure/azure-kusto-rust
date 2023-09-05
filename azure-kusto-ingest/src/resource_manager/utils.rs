use anyhow::Result;
use azure_kusto_data::models::TableV1;

/// Helper to get a column index from a table
// TODO: this could be moved upstream into Kusto Data - would likely result in a change to the API of this function to return an Option<usize>
pub fn get_column_index(table: &TableV1, column_name: &str) -> Result<usize> {
    table
        .columns
        .iter()
        .position(|c| c.column_name == column_name)
        .ok_or(anyhow::anyhow!(
            "{} column is missing in the table",
            column_name
        ))
}
