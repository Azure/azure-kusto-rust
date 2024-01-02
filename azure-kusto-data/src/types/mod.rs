//! Types used for serialization and deserialization of ADX data.

use derive_more::{Display, From, Into, FromStr};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
pub use crate::types::timespan::Timespan;

mod timespan;

macro_rules! kusto_type {
    ($name:ident, $type:ty, primitive) => {
        kusto_type!($name, $type, Copy, PartialEq, PartialOrd, Eq, Ord);
    };
    ($name:ident, $type:ty, $($additional:tt),* ) => {
        #[doc = concat!("Represents a ", stringify!($type), " for kusto, for serialization and deserialization.")]
        #[derive(Deserialize, Serialize, Clone, From, Into, Debug,  $($additional),*)]
        #[serde(transparent)]
        pub struct $name(pub Option<$type>);

        impl Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match &self.0 {
                    Some(v) => write!(f, concat!(stringify!($name), "({})"), v),
                    None => write!(f, "null"),
                }
            }
        }

        impl From<$type> for $name {
            fn from(v: $type) -> Self {
                Self(Some(v))
            }
        }

        impl TryFrom<$name> for $type {
            type Error = crate::error::Error;

            fn try_from(value: $name) -> Result<Self, Self::Error> {
                value.0.ok_or_else(|| crate::error::Error::from(crate::error::InvalidArgumentError::ValueNull(stringify!($name).to_string())))
            }
        }

        impl FromStr for $name {
            type Err = crate::error::Error;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Ok(Self(Some(s.parse::<$type>().map_err(|e| crate::error::Error::from(crate::error::InvalidArgumentError::ParseError(e.into())))?)))
            }
        }

    };
}


kusto_type!(KustoBool, bool, primitive);
kusto_type!(KustoInt, i32, primitive);
kusto_type!(KustoLong, i64, primitive);
kusto_type!(KustoReal, f64, Copy, PartialEq, PartialOrd);
kusto_type!(KustoDecimal, decimal::d128, Copy);
kusto_type!(KustoString, String, PartialEq, PartialOrd, Eq, Ord);
kusto_type!(KustoDynamic, serde_json::Value, PartialEq, Eq);
kusto_type!(KustoGuid, uuid::Uuid, primitive);
kusto_type!(KustoDateTime, OffsetDateTime, primitive);
kusto_type!(KustoTimespan, Timespan, primitive);

