pub mod v1;
pub mod v2;

#[cfg(test)]
pub(crate) mod test_helpers;

use serde::{Deserialize, Serialize};

/// Represents the scalar data types of ADX. see [the docs for more information](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/scalar-data-types/)
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Copy, Clone)]
pub enum ColumnType {
    #[serde(rename = "bool")]
    /// Boolean type, true or false. Internally is a u8.
    Bool,
    /// Datetime, represents a specific point in time.
    #[serde(rename = "datetime")]
    DateTime,
    /// A complex type, that is either an array or a dictionary of other values.
    #[serde(rename = "dynamic")]
    Dynamic,
    /// GUID type, represents a globally unique identifier.
    #[serde(rename = "guid")]
    Guid,
    #[serde(rename = "int")]
    /// 32 bit integer type.
    Int,
    /// 64 bit integer type.
    #[serde(rename = "long")]
    Long,
    /// 64 bit floating point type.
    #[serde(rename = "real")]
    Real,
    #[serde(rename = "string")]
    /// String type, represents a string of characters.
    String,
    /// Timespan type, represents a duration of time.
    #[serde(rename = "timespan")]
    Timespan,
    #[serde(alias = "decimal")]
    /// Decimal, represents a fixed-point number with a defined precision and scale.
    Decimal,
}


pub trait Column {
    fn column_name(&self) -> &str;
    fn column_type(&self) -> ColumnType;
}

impl Column for v1::Column {
    fn column_name(&self) -> &str {
        &self.column_name
    }

    fn column_type(&self) -> ColumnType {
        self.column_type
    }
}

impl Column for v2::Column {
    fn column_name(&self) -> &str {
        &self.column_name
    }

    fn column_type(&self) -> ColumnType {
        self.column_type
    }
}
