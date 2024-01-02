//! Types used for serialization and deserialization of ADX data.

use std::convert::Infallible;
use derive_more::{Display, From, Into, FromStr};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use crate::error::{Error, ParseError};
pub use crate::types::timespan::Timespan;

mod timespan;

macro_rules! kusto_type {
    ($name:ident, $type:ty, primitive) => {
        kusto_type!($name, $type, Copy, PartialEq, PartialOrd, Eq, Ord);
    };
    ($name:ident, $type:ty, $($additional:tt),* ) => {
        #[doc = concat!("Represents a ", stringify!($type), " for kusto, for serialization and deserialization.")]
        #[derive(Deserialize, Serialize, Default, Clone, From, Into, Debug,  $($additional),*)]
        #[serde(transparent)]
        pub struct $name(pub Option<$type>);

        impl $name {
            pub fn new(value: $type) -> Self {
                Self(Some(value))
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
                Ok(
                    Self::new(
                        s.parse::<$t>().map_err(|e| Error::from($err(e)))?
                    )
                )
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


kusto_from_str!(KustoBool, bool, ParseError::Bool);
kusto_from_str!(KustoInt, i32, ParseError::Int);
kusto_from_str!(KustoLong, i64, ParseError::Int);
kusto_from_str!(KustoReal, f64, ParseError::Float);
kusto_from_str!(KustoDecimal, decimal::d128, ParseError::Decimal);
kusto_from_str!(KustoGuid, uuid::Uuid, ParseError::Guid);

impl FromStr for KustoDateTime {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::new(OffsetDateTime::parse(s, &Rfc3339).map_err(
            |e| Error::from(ParseError::DateTime(e)))?
        ))
    }
}

impl FromStr for KustoTimespan {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::new(s.parse::<Timespan>()?))
    }
}

impl FromStr for KustoString {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::new(s.to_string()))
    }
}
