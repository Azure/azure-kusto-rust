use crate::error::{Error, ParseError};
use derive_more::{From, Into};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

/// Datetime for kusto, for serialization and deserialization.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Into, Debug)]
pub struct KustoDateTime(pub Option<OffsetDateTime>);

impl KustoDateTime {
    /// Creates a new `KustoDatetime` from a `time::OffsetDateTime`.
    pub fn new(value: OffsetDateTime) -> Self {
        Self(Some(value))
    }

    /// Creates a null `KustoDatetime`.
    pub fn null() -> Self {
        Self(None)
    }
}

impl Display for KustoDateTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            Some(v) => write!(f, "KustoDateTime({})", v),
            None => write!(f, "null"),
        }
    }
}

impl Serialize for KustoDateTime {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match &self.0 {
            Some(v) => serializer.serialize_str(&v.format(&Rfc3339).expect("Should never fail")),
            None => serializer.serialize_none(),
        }
    }
}

impl<'de> Deserialize<'de> for KustoDateTime {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let opt = Option::<String>::deserialize(deserializer)?;
        if let Some(s) = opt {
            Ok(s.parse::<KustoDateTime>()
                .map_err(|e| serde::de::Error::custom(e.to_string()))?)
        } else {
            Ok(Self::null())
        }
    }
}

impl FromStr for KustoDateTime {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::new(
            OffsetDateTime::parse(s, &Rfc3339).map_err(|e| Error::from(ParseError::DateTime(e)))?,
        ))
    }
}

impl From<OffsetDateTime> for KustoDateTime {
    fn from(v: OffsetDateTime) -> Self {
        Self::new(v)
    }
}
