//! Types used for serialization and deserialization of ADX data.

use azure_core::error::ResultExt;
use std::fmt::{Debug};
use std::ops::Deref;
use std::str::FromStr;
use serde::{Deserialize, Serialize, Serializer};
use time::OffsetDateTime;
use derive_more::{From, Into, Display};

mod timespan;


macro_rules! kusto_type {
    ($name:ident, $type:ty, $dd:?) => {
        #[doc = concat!("Represent a ", stringify!($type), " for kusto, for serialization and deserialization.")]
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, From, Into, Debug, Display)]
        $dd
        pub struct $name(pub $type);
    }
    ($name:ident, $type:ty) => {
        #[doc = concat!("Represent a ", stringify!($type), " for kusto, for serialization and deserialization.")]
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, From, Into, Debug, Display)]
        #[serde(transparent)]
        pub struct $name(pub $type);
    };
}

kusto_type!(KustoBool, bool);
kusto_type!(KustoInt, i32);
kusto_type!(KustoLong, i64);
kusto_type!(KustoReal, f64);
kusto_type!(KustoDecimal, decimal::d128);
kusto_type!(KustoString, String);
kusto_type!(KustoGuid, uuid::Uuid);
kusto_type!(KustoDateTime, OffsetDateTime);
kusto_type!(KustoTimespan, time::Duration);
