//! Types used for serialization and deserialization of ADX data.

use crate::error::{Error, ParseError};
use derive_more::{Display, From, FromStr, Into};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::fmt::Debug;

mod datetime;
mod timespan;

macro_rules! kusto_type {
    ($name:ident, $type:ty, primitive) => {
        kusto_type!($name, $type, Copy, PartialOrd, Eq, Ord);
    };
    ($name:ident, $type:ty, $($additional:tt),* ) => {
        #[doc = concat!("Represents a ", stringify!($type), " for kusto, for serialization and deserialization.")]
        #[derive(Deserialize, Serialize, Default, Clone, From, Into, Debug, PartialEq, $($additional),*)]
        #[serde(transparent)]
        pub struct $name(pub Option<$type>);

        impl $name {
            #[doc = concat!("Creates a new ", stringify!($type), " for kusto.")]
            pub fn new(value: $type) -> Self {
                Self(Some(value))
            }

            #[doc = concat!("Creates a null ", stringify!($type), " for kusto.")]
            pub fn null() -> Self {
                Self(None)
            }
        }

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
                Self::new(v)
            }
        }

        impl TryFrom<$name> for $type {
            type Error = crate::error::Error;

            fn try_from(value: $name) -> Result<Self, Self::Error> {
                value.0.ok_or_else(|| crate::error::ParseError::ValueNull(stringify!($name).to_string()).into())
            }
        }

    };
}

macro_rules! kusto_from_str {
    ($name:ident, $t:ty, $err:expr) => {
        impl FromStr for $name {
            type Err = Error;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Ok(Self::new(
                    s.parse::<$t>().map_err(|e| Error::from($err(e)))?,
                ))
            }
        }
    };
}

kusto_type!(KustoBool, bool, primitive);
kusto_type!(KustoInt, i32, primitive);
kusto_type!(KustoLong, i64, primitive);
kusto_type!(KustoReal, f64, Copy, PartialOrd);
kusto_type!(KustoDecimal, Decimal, primitive);
kusto_type!(KustoString, String, PartialOrd, Eq, Ord);
kusto_type!(KustoDynamic, serde_json::Value, Eq);
kusto_type!(KustoGuid, uuid::Uuid, primitive);
pub use datetime::KustoDateTime;
pub use timespan::KustoTimespan;

kusto_from_str!(KustoBool, bool, ParseError::Bool);
kusto_from_str!(KustoInt, i32, ParseError::Int);
kusto_from_str!(KustoLong, i64, ParseError::Int);
kusto_from_str!(KustoReal, f64, ParseError::Float);
kusto_from_str!(KustoDecimal, Decimal, ParseError::Decimal);
kusto_from_str!(KustoGuid, uuid::Uuid, ParseError::Guid);

enum KustoValue {
    Bool(KustoBool),
    Int(KustoInt),
    Long(KustoLong),
    Real(KustoReal),
    Decimal(KustoDecimal),
    String(KustoString),
    Guid(KustoGuid),
    DateTime(KustoDateTime),
    TimeSpan(KustoTimespan),
    Dynamic(KustoDynamic),
}

impl FromStr for KustoString {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::new(s.to_string()))
    }
}
