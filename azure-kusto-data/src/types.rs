use azure_core::error::{ErrorKind, ResultExt};
use once_cell::sync::Lazy;
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
        Self(time)
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
        Self(duration)
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
        .map_or(0, |m| m.as_str().parse::<i64>().expect("Failed to parse regex segment as i64 - this is a bug - please report this issue to the Kusto team"))
}
static KUSTO_DURATION_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^(?P<neg>-)?((?P<days>\d+)\.)?(?P<hours>\d+):(?P<minutes>\d+):(?P<seconds>\d+)(\.(?P<nanos>\d+))?$")
        .expect("Failed to compile KustoDuration regex, this should never happen - please report this issue to the Kusto team")
});

impl FromStr for KustoDuration {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        KUSTO_DURATION_REGEX
            .captures(s)
            .map(|captures| {
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
                Self(duration)
            })
            .ok_or_else(|| InvalidArgumentError::InvalidDuration(s.to_string()).into())
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
            i128::from(neg)
                * (self.whole_nanoseconds() - i128::from(self.whole_seconds()) * 1_000_000_000)
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
                KustoDuration::from_str(from)
                    .unwrap_or_else(|_| panic!("Failed to parse duration {}", from))
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
            let parsed = KustoDuration::from_str(duration)
                .unwrap_or_else(|_| panic!("Failed to parse duration {}", duration));
            assert_eq!(format!("{:?}", parsed), duration);
        }
    }
}
