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
        .map(|x| x.to_string_lossy().to_string())
        .unwrap_or_else(|_| UNKNOWN.to_string())
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
