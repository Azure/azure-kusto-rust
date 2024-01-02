use crate::error::{Error, InvalidArgumentError};
use once_cell::sync::Lazy;
use regex::{Captures, Regex};
use std::fmt::{Debug, Display, Formatter};
use std::str::FromStr;
use derive_more::{From, Into};
use serde_with::{DeserializeFromStr, SerializeDisplay};
use time::Duration;

/// Timespan that serializes to a string in the format `[-][d.]hh:mm:ss[.fffffff]`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, SerializeDisplay, DeserializeFromStr, From, Into)]
pub struct Timespan(pub Duration);

fn parse_regex_segment(captures: &Captures, name: &str) -> i64 {
    captures
        .name(name)
        .map_or(0, |m| m.as_str().parse::<i64>().expect("Failed to parse regex segment as i64 - this is a bug - please report this issue to the Kusto team"))
}

static KUSTO_DURATION_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^(?P<neg>-)?((?P<days>\d+)\.)?(?P<hours>\d+):(?P<minutes>\d+):(?P<seconds>\d+)(\.(?P<nanos>\d+))?$")
        .expect("Failed to compile KustoTimespan regex, this should never happen - please report this issue to the Kusto team")
});

impl FromStr for Timespan {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let captures = KUSTO_DURATION_REGEX
            .captures(s)
            .ok_or_else(|| Error::from(InvalidArgumentError::InvalidDuration(s.to_string())))?;

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

        Ok(Self(duration))
    }
}

impl Display for Timespan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let neg = if self.0.is_negative() {
            write!(f, "-")?;
            -1
        } else {
            1
        };
        if self.0.whole_days().abs() > 0 {
            write!(f, "{}.", self.0.whole_days().abs())?;
        }
        write!(
            f,
            "{:02}:{:02}:{:02}.{:07}",
            neg * (self.0.whole_hours() - self.0.whole_days() * 24),
            neg * (self.0.whole_minutes() - self.0.whole_hours() * 60),
            neg * (self.0.whole_seconds() - self.0.whole_minutes() * 60),
            i128::from(neg)
                * (self.0.whole_nanoseconds() - i128::from(self.0.whole_seconds()) * 1_000_000_000)
                / 100 // Ticks
        )?;

        Ok(())
    }
}

impl Debug for Timespan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
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
                Timespan::from_str(from)
                    .unwrap_or_else(|_| panic!("Failed to parse duration {}", from))
                    .0.whole_nanoseconds(),
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
            let parsed = Timespan::from_str(duration)
                .unwrap_or_else(|_| panic!("Failed to parse duration {}", duration));
            assert_eq!(format!("{:?}", parsed), duration);
        }
    }
}
