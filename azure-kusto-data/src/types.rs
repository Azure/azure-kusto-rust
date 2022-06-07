use azure_core::error::{ErrorKind, ResultExt};
use lazy_static::lazy_static;
use regex::{Captures, Regex};
use serde_with::{DeserializeFromStr, SerializeDisplay};
use std::fmt::{Debug, Display, Formatter};
use std::ops::Deref;
use std::str::FromStr;
use time::{Duration, OffsetDateTime};

use crate::error::{Error, InvalidArgumentError};
use time::format_description::well_known::Rfc3339;

#[derive(PartialEq, Copy, Clone, DeserializeFromStr, SerializeDisplay)]
pub struct KustoDateTime(pub OffsetDateTime);

impl FromStr for KustoDateTime {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(OffsetDateTime::parse(s, &Rfc3339)
            .map(KustoDateTime)
            .context(ErrorKind::DataConversion, "Failed to parse KustoDateTime")?)
    }
}

impl Display for KustoDateTime {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            self.0.format(&Rfc3339).unwrap_or_else(|_| "".into())
        )?;
        Ok(())
    }
}

impl Debug for KustoDateTime {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl From<OffsetDateTime> for KustoDateTime {
    fn from(time: OffsetDateTime) -> Self {
        KustoDateTime(time)
    }
}

impl Deref for KustoDateTime {
    type Target = OffsetDateTime;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(PartialEq, Copy, Clone, DeserializeFromStr, SerializeDisplay)]
pub struct KustoDuration(pub Duration);

impl From<Duration> for KustoDuration {
    fn from(duration: Duration) -> Self {
        KustoDuration(duration)
    }
}

impl Deref for KustoDuration {
    type Target = Duration;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

fn parse_regex_segment(captures: &Captures, name: &str) -> i64 {
    captures
        .name(name)
        .map(|m| m.as_str().parse::<i64>().unwrap())
        .unwrap_or(0)
}

impl FromStr for KustoDuration {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        lazy_static! {
            static ref RE: Regex = Regex::new(r"^(?P<neg>\-)?((?P<days>\d+)\.)?(?P<hours>\d+):(?P<minutes>\d+):(?P<seconds>\d+)(\.(?P<nanos>\d+))?$").unwrap();
        }
        if let Some(captures) = RE.captures(s) {
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
            Ok(KustoDuration(duration))
        } else {
            Err(InvalidArgumentError::InvalidDuration(s.to_string()).into())
        }
    }
}

impl Display for KustoDuration {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let neg = if self.is_negative() {
            write!(f, "-")?;
            -1
        } else {
            1
        };
        if self.whole_days().abs() > 0 {
            write!(f, "{}.", self.whole_days().abs())?;
        }
        write!(
            f,
            "{:02}:{:02}:{:02}.{:07}",
            neg * (self.whole_hours() - self.whole_days() * 24),
            neg * (self.whole_minutes() - self.whole_hours() * 60),
            neg * (self.whole_seconds() - self.whole_minutes() * 60),
            neg as i128 * (self.whole_nanoseconds() - self.whole_seconds() as i128 * 1_000_000_000)
                / 100 // Ticks
        )?;

        Ok(())
    }
}

impl Debug for KustoDuration {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_conversion() {
        let refs: Vec<(&str, i64)> = vec![
            ("1.00:00:00.0000000", 86400000000000),
            ("01:00:00.0000000", 3600000000000),
            ("01:00:00", 3600000000000),
            ("00:05:00.0000000", 300000000000),
            ("00:00:00.0000001", 100),
            ("-01:00:00", -3600000000000),
            ("-1.00:00:00.0000000", -86400000000000),
            ("00:00:00.1234567", 123456700),
        ];

        for (from, to) in refs {
            assert_eq!(
                KustoDuration::from_str(from).unwrap().whole_nanoseconds(),
                to as i128
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
            let parsed = KustoDuration::from_str(duration).unwrap();
            assert_eq!(format!("{:?}", parsed), duration);
        }
    }
}
