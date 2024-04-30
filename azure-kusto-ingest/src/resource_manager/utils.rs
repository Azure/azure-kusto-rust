use azure_kusto_data::models::TableV1;

/// Helper to get a column index from a table
// TODO: this could be moved upstream into Kusto Data
pub fn get_column_index(table: &TableV1, column_name: &str) -> Option<usize> {
    table
        .columns
        .iter()
        .position(|c| c.column_name == column_name)
}
