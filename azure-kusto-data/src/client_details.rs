#![allow(missing_docs)]

use std::borrow::Cow;

use once_cell::sync::Lazy;
use regex::Regex;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ClientDetails {
    pub application: String,
    pub user: String,
    pub version: String,
}

impl ClientDetails {
    pub(crate) fn new(application: Option<String>, user: Option<String>) -> Self {
        ClientDetails {
            application: application.unwrap_or_else(|| DEFAULT_APPLICATION.to_string()),
            user: user.unwrap_or_else(|| DEFAULT_USER.to_string()),
            version: DEFAULT_VERSION.to_string(),
        }
    }
}

static UNKNOWN: &str = "unknown";
static NONE: &str = "[none]";

static ESCAPE_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new("[\\r\\n\\s{}|]+").unwrap());

static DEFAULT_USER: Lazy<String> = Lazy::new(|| {
    let domain = std::env::var("USERDOMAIN");
    let user = std::env::var("USERNAME");
    match (domain, user) {
        (Ok(domain), Ok(user)) => format!("{}\\{}", domain, user),
        (Err(_), Ok(user)) => user,
        _ => UNKNOWN.to_string(),
    }
});

static DEFAULT_APPLICATION: Lazy<String> = Lazy::new(|| {
    std::env::current_exe()
        .ok()
        .and_then(|x| x.file_name().map(|x| x.to_string_lossy().to_string()))
        .unwrap_or_else(|| UNKNOWN.to_string())
});

static DEFAULT_VERSION: Lazy<String> = Lazy::new(|| {
    format_header([
        ("Kusto.Rust.Client".into(), env!("CARGO_PKG_VERSION").into()),
        ("Os".into(), std::env::consts::OS.into()),
        ("Arch".into(), std::env::consts::ARCH.into()),
    ])
});

fn format_header<'a, T: IntoIterator<Item = (Cow<'a, str>, Cow<'a, str>)>>(args: T) -> String {
    args.into_iter()
        .map(|(k, v)| format!("{}:{}", k, escape_value(v)))
        .collect::<Vec<_>>()
        .join("|")
}

fn escape_value(s: Cow<str>) -> String {
    format!("{{{}}}", ESCAPE_REGEX.replace_all(s.as_ref(), "_"))
}

pub(crate) fn set_connector_details(details: ConnectorDetails) -> (String, String) {
    let ConnectorDetails {
        name,
        app_name,
        app_version,
        additional_fields,
        send_user,
        override_user,
        version,
    } = details;

    let mut fields: Vec<(Cow<str>, Cow<str>)> =
        vec![(format!("Kusto.{name}").into(), version.into())];

    let app_name = app_name
        .map(Cow::Borrowed)
        .unwrap_or_else(|| DEFAULT_APPLICATION.clone().into());
    let app_version = app_version
        .map(Cow::Borrowed)
        .unwrap_or_else(|| UNKNOWN.into());

    fields.push((
        format!("App.{}", escape_value(app_name)).into(),
        app_version,
    ));

    fields.extend(
        additional_fields
            .into_iter()
            .map(|(k, v)| (k.into(), v.into())),
    );

    let user = if send_user {
        override_user.unwrap_or(DEFAULT_USER.as_str())
    } else {
        NONE
    };

    (format_header(fields), user.to_string())
}

#[derive(Default, Debug, Clone, PartialEq, Eq, derive_builder::Builder)]
#[builder(setter(into, strip_option, prefix = "with"), default)]
/// Connector details for tracing.
pub struct ConnectorDetails<'a> {
    /// Connector name.
    name: &'a str,
    /// Connector version.
    version: &'a str,
    /// Whether to send user details.
    send_user: bool,
    /// Override default user.
    override_user: Option<&'a str>,
    /// Name of the containing application.
    app_name: Option<&'a str>,
    /// Version of the containing application.
    app_version: Option<&'a str>,
    /// Additional fields to add to the header.
    additional_fields: Vec<(&'a str, &'a str)>,
}

#[cfg(test)]
mod tests {
    use super::*;

    // Write extensive tests
    #[test]
    fn test_escape_value() {
        assert_eq!(escape_value("".into()), "{}");
        assert_eq!(escape_value("abc".into()), "{abc}");
        assert_eq!(escape_value("ab c".into()), "{ab_c}");
        assert_eq!(escape_value("ab_c".into()), "{ab_c}");
        assert_eq!(escape_value("ab|c".into()), "{ab_c}");
        assert_eq!(escape_value("ab{}c".into()), "{ab_c}");
    }

    #[test]
    fn test_format_header() {
        assert_eq!(format_header(vec![("a".into(), "b".into())]), "a:{b}");
        assert_eq!(
            format_header(vec![("a".into(), "b".into()), ("c".into(), "d".into())]),
            "a:{b}|c:{d}"
        );
    }

    #[test]
    fn test_client_details_new() {
        let client_details = ClientDetails::new(None, None);
        assert_eq!(
            client_details,
            ClientDetails {
                application: DEFAULT_APPLICATION.clone(),
                user: DEFAULT_USER.clone(),
                version: DEFAULT_VERSION.clone()
            }
        );

        let client_details = ClientDetails::new(Some("my_app".to_string()), None);
        assert_eq!(
            client_details,
            ClientDetails {
                application: "my_app".to_string(),
                user: DEFAULT_USER.clone(),
                version: DEFAULT_VERSION.clone()
            }
        );

        let client_details = ClientDetails::new(None, Some("my_user".to_string()));
        assert_eq!(
            client_details,
            ClientDetails {
                application: DEFAULT_APPLICATION.clone(),
                user: "my_user".to_string(),
                version: DEFAULT_VERSION.clone()
            }
        );

        let client_details =
            ClientDetails::new(Some("my_app".to_string()), Some("my_user".to_string()));
        assert_eq!(
            client_details,
            ClientDetails {
                application: "my_app".to_string(),
                user: "my_user".to_string(),
                version: DEFAULT_VERSION.clone()
            }
        );
    }

    #[test]
    fn test_set_connector_details_user() {
        let details = ConnectorDetailsBuilder::default()
            .with_name("MyConnector")
            .with_version("1.0")
            .with_send_user(true)
            .with_override_user("user1")
            .with_app_name("MyApp")
            .with_app_version("1.0.1")
            .with_additional_fields(vec![("key1", "value1"), ("key2", "value2")])
            .build()
            .unwrap();

        let (header, user) = set_connector_details(details);

        let expected_header =
            "Kusto.MyConnector:{1.0}|App.{MyApp}:{1.0.1}|key1:{value1}|key2:{value2}".to_string();

        assert_eq!(header, expected_header);

        assert_eq!(user, "user1");

        let details = ConnectorDetailsBuilder::default()
            .with_name("MyConnector")
            .with_version("1.0")
            .with_send_user(false)
            .with_app_name("MyApp")
            .with_app_version("1.0.1")
            .with_additional_fields(vec![("key1", "value1"), ("key2", "value2")])
            .build()
            .unwrap();

        let (header, user) = set_connector_details(details);

        let expected_header =
            "Kusto.MyConnector:{1.0}|App.{MyApp}:{1.0.1}|key1:{value1}|key2:{value2}".to_string();

        assert_eq!(header, expected_header);

        assert_eq!(user, "[none]");

        let details = ConnectorDetailsBuilder::default()
            .with_name("MyConnector")
            .with_version("1.0")
            .with_send_user(true)
            .with_app_name("MyApp")
            .with_app_version("1.0.1")
            .with_additional_fields(vec![("key1", "value1"), ("key2", "value2")])
            .build()
            .unwrap();

        let (header, user) = set_connector_details(details);

        let expected_header =
            "Kusto.MyConnector:{1.0}|App.{MyApp}:{1.0.1}|key1:{value1}|key2:{value2}".to_string();

        // We don't know the actual user that will be returned, but we can at least check
        // that it's not an empty string.
        assert_ne!(user, "", "user should not be an empty string, but it is");

        assert_eq!(header, expected_header);
    }

    #[test]
    fn test_set_connector_details_no_app_name() {
        let details = ConnectorDetailsBuilder::default()
            .with_name("MyConnector")
            .with_version("1.0")
            .build()
            .unwrap();

        let (header, user) = set_connector_details(details);

        assert!(header.contains("Kusto.MyConnector:{1.0}"));
        assert_eq!(user, "[none]");
    }

    #[test]
    fn test_set_connector_details_no_app_version() {
        let details = ConnectorDetailsBuilder::default()
            .with_name("MyConnector")
            .with_version("1.0")
            .with_app_name("MyApp")
            .build()
            .unwrap();

        let (header, user) = set_connector_details(details);

        assert!(header.contains("Kusto.MyConnector:{1.0}"));
        assert!(header.contains("App.{MyApp}:{unknown}"));
        assert_eq!(user, "[none]");
    }

    #[test]
    fn test_set_connector_details_no_additional_fields() {
        let details = ConnectorDetailsBuilder::default()
            .with_name("MyConnector")
            .with_version("1.0")
            .with_app_name("MyApp")
            .with_app_version("1.0.1")
            .build()
            .unwrap();

        let (header, user) = set_connector_details(details);

        assert!(header.contains("Kusto.MyConnector:{1.0}"));
        assert!(header.contains("App.{MyApp}:{1.0.1}"));
        assert_eq!(user, "[none]");
    }
}
