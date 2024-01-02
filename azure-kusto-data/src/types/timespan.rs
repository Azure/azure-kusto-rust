use crate::error::Error;
use crate::error::ParseError;
use derive_more::{From, Into};
use once_cell::sync::Lazy;
use regex::{Captures, Regex};
use serde::{Deserialize, Serialize};
use std::convert::TryInto;
use std::fmt::{Debug, Display, Formatter};
use std::num::TryFromIntError;
use std::str::FromStr;
use time::Duration;

fn parse_regex_segment(captures: &Captures, name: &str) -> i64 {
    captures
        .name(name)
        .map_or(0, |m| m.as_str().parse::<i64>().expect("Failed to parse regex segment as i64 - this is a bug - please report this issue to the Kusto team"))
}

static KUSTO_DURATION_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^(?P<neg>-)?((?P<days>\d+)\.)?(?P<hours>\d+):(?P<minutes>\d+):(?P<seconds>\d+)(\.(?P<nanos>\d+))?$")
        .expect("Failed to compile KustoTimespan regex, this should never happen - please report this issue to the Kusto team")
});
/// Timespan that serializes to a string in the format `[-][d.]hh:mm:ss[.fffffff]`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, From, Into)]
pub struct KustoTimespan(pub Option<Duration>);

impl KustoTimespan {
    /// Creates a new `KustoTimespan` from a `std::time::Duration`.
    fn new(duration: Duration) -> Self {
        Self(Some(duration))
    }

    /// Creates a null `KustoTimespan`.
    pub fn null() -> Self {
        Self(None)
    }

    fn format(f: &mut Formatter, d: Duration) -> std::fmt::Result {
        let neg = if d.is_negative() {
            write!(f, "-")?;
            -1
        } else {
            1
        };
        if d.whole_days().abs() > 0 {
            write!(f, "{}.", d.whole_days().abs())?;
        }
        write!(
            f,
            "{:02}:{:02}:{:02}.{:07}",
            neg * (d.whole_hours() - d.whole_days() * 24),
            neg * (d.whole_minutes() - d.whole_hours() * 60),
            neg * (d.whole_seconds() - d.whole_minutes() * 60),
            i128::from(neg)
                * (d.whole_nanoseconds() - i128::from(d.whole_seconds()) * 1_000_000_000)
                / 100 // Ticks
        )?;
        Ok(())
    }
}

impl FromStr for KustoTimespan {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let captures = KUSTO_DURATION_REGEX
            .captures(s)
            .ok_or_else(|| ParseError::Timespan(s.to_string()))?;

        let neg = match captures.name("neg") {
            None => 1,
            Some(_) => -1,
        };

        let days = parse_regex_segment(&captures, "days");
        let hours = parse_regex_segment(&captures, "hours");
        let minutes = parse_regex_segment(&captures, "minutes");
        let seconds = parse_regex_segment(&captures, "seconds");
        let nanos = parse_regex_segment(&captures, "nanos");
        let duration = neg
            * (Duration::days(days)
                + Duration::hours(hours)
                + Duration::minutes(minutes)
                + Duration::seconds(seconds)
                + Duration::nanoseconds(nanos * 100)); // Ticks

        Ok(Self(Some(duration)))
    }
}

impl Display for KustoTimespan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(d) = self.0 {
            write!(f, "KustoTimespan(")?;
            Self::format(f, d)?;
            write!(f, ")")?;

            Ok(())
        } else {
            write!(f, "null")
        }
    }
}

impl Debug for KustoTimespan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(d) = self.0 {
            Self::format(f, d)?;
            Ok(())
        } else {
            write!(f, "null")
        }
    }
}

impl Serialize for KustoTimespan {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if let Some(d) = self.0 {
            serializer.serialize_str(&format!("{:?}", d))
        } else {
            serializer.serialize_none()
        }
    }
}

impl<'de> Deserialize<'de> for KustoTimespan {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let opt = Option::<String>::deserialize(deserializer)?;
        if let Some(s) = opt {
            Ok(s.parse::<KustoTimespan>()
                .map_err(|e| serde::de::Error::custom(e.to_string()))?)
        } else {
            Ok(Self::null())
        }
    }
}

impl TryFrom<std::time::Duration> for KustoTimespan {
    type Error = TryFromIntError;

    fn try_from(d: std::time::Duration) -> Result<Self, Self::Error> {
        Ok(Self(Some(Duration::new(
            d.as_secs().try_into()?,
            d.subsec_nanos().try_into()?,
        ))))
    }
}

impl From<Duration> for KustoTimespan {
    fn from(d: Duration) -> Self {
        Self(Some(d))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_conversion() {
        let refs: Vec<(&str, i64)> = vec![
            ("1.00:00:00.0000000", 86_400_000_000_000),
            ("01:00:00.0000000", 3_600_000_000_000),
            ("01:00:00", 3_600_000_000_000),
            ("00:05:00.0000000", 300_000_000_000),
            ("00:00:00.0000001", 100),
            ("-01:00:00", -3_600_000_000_000),
            ("-1.00:00:00.0000000", -86_400_000_000_000),
            ("00:00:00.1234567", 123_456_700),
        ];

        for (from, to) in refs {
            assert_eq!(
                KustoTimespan::from_str(from)
                    .unwrap_or_else(|_| panic!("Failed to parse duration {}", from))
                    .0
                    .unwrap()
                    .whole_nanoseconds(),
                i128::from(to)
            );
        }
    }

    #[test]
    fn format_duration() {
        let refs: Vec<&str> = vec![
            "1.00:00:00.0000001",
            "01:00:00.0000000",
            "00:05:00.0000000",
            "00:00:00.0000001",
            "-1.00:00:00.0000000",
            "00:00:00.1234567",
        ];

        for duration in refs {
            let parsed = KustoTimespan::from_str(duration)
                .unwrap_or_else(|_| panic!("Failed to parse duration {}", duration));
            assert_eq!(format!("{:?}", parsed), duration);
        }
    }
}
