//! Set of properties that can be use in a connection string provided to KustoConnectionStringBuilder.
//! For a complete list of properties go to [the official docs](https://docs.microsoft.com/en-us/azure/kusto/api/connection-strings/kusto)

use std::fmt::{Debug, Formatter};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::credentials::{CallbackTokenCredential, ConstTokenCredential};
use azure_core::auth::TokenCredential;
use azure_identity::{
    AzureCliCredential, ClientSecretCredential, DefaultAzureCredential,
    ImdsManagedIdentityCredential, TokenCredentialOptions,
};
use hashbrown::HashMap;
use once_cell::sync::Lazy;

use crate::error::ConnectionStringError;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
enum ConnectionStringKey {
    DataSource,
    FederatedSecurity,
    UserId,
    Password,
    ApplicationClientId,
    ApplicationKey,
    ApplicationCertificate,
    ApplicationCertificateThumbprint,
    AuthorityId,
    ApplicationToken,
    UserToken,
    MsiAuth,
    MsiParams,
    AzCli,
    InteractiveLogin,
}

const CENSORED_VALUE: &str = "******";
const CONNECTION_STRING_TRUE: &str = "True";
const CONNECTION_STRING_FALSE: &str = "False";

impl ConnectionStringKey {
    const fn to_str(self) -> &'static str {
        match self {
            ConnectionStringKey::DataSource => "Data Source",
            ConnectionStringKey::FederatedSecurity => "AAD Federated Security",
            ConnectionStringKey::UserId => "AAD User ID",
            ConnectionStringKey::Password => "Password",
            ConnectionStringKey::ApplicationClientId => "Application Client Id",
            ConnectionStringKey::ApplicationKey => "Application Key",
            ConnectionStringKey::ApplicationCertificate => "ApplicationCertificate",
            ConnectionStringKey::ApplicationCertificateThumbprint => {
                "Application Certificate Thumbprint"
            }
            ConnectionStringKey::AuthorityId => "Authority Id",
            ConnectionStringKey::ApplicationToken => "ApplicationToken",
            ConnectionStringKey::UserToken => "UserToken",
            ConnectionStringKey::MsiAuth => "MSI Authentication",
            ConnectionStringKey::MsiParams => "MSI Params",
            ConnectionStringKey::AzCli => "AZ CLI",
            ConnectionStringKey::InteractiveLogin => "Interactive Login",
        }
    }
}

static ALIAS_MAP: Lazy<HashMap<&'static str, ConnectionStringKey>> = Lazy::new(|| {
    let mut m = HashMap::new();
    m.insert("data source", ConnectionStringKey::DataSource);
    m.insert("addr", ConnectionStringKey::DataSource);
    m.insert("address", ConnectionStringKey::DataSource);
    m.insert("network address", ConnectionStringKey::DataSource);
    m.insert("server", ConnectionStringKey::DataSource);

    m.insert(
        "aad federated security",
        ConnectionStringKey::FederatedSecurity,
    );
    m.insert("federated security", ConnectionStringKey::FederatedSecurity);
    m.insert("federated", ConnectionStringKey::FederatedSecurity);
    m.insert("fed", ConnectionStringKey::FederatedSecurity);
    m.insert("aadfed", ConnectionStringKey::FederatedSecurity);

    m.insert("aad user id", ConnectionStringKey::UserId);
    m.insert("user id", ConnectionStringKey::UserId);
    m.insert("uid", ConnectionStringKey::UserId);
    m.insert("user", ConnectionStringKey::UserId);

    m.insert("password", ConnectionStringKey::Password);
    m.insert("pwd", ConnectionStringKey::Password);

    m.insert(
        "application client id",
        ConnectionStringKey::ApplicationClientId,
    );
    m.insert("appclientid", ConnectionStringKey::ApplicationClientId);

    m.insert("application key", ConnectionStringKey::ApplicationKey);
    m.insert("appkey", ConnectionStringKey::ApplicationKey);

    m.insert(
        "application certificate",
        ConnectionStringKey::ApplicationCertificate,
    );

    m.insert(
        "application certificate thumbprint",
        ConnectionStringKey::ApplicationCertificateThumbprint,
    );
    m.insert(
        "appcert",
        ConnectionStringKey::ApplicationCertificateThumbprint,
    );

    m.insert("authority id", ConnectionStringKey::AuthorityId);
    m.insert("authorityid", ConnectionStringKey::AuthorityId);
    m.insert("authority", ConnectionStringKey::AuthorityId);
    m.insert("tenantid", ConnectionStringKey::AuthorityId);
    m.insert("tenant", ConnectionStringKey::AuthorityId);
    m.insert("tid", ConnectionStringKey::AuthorityId);

    m.insert("application token", ConnectionStringKey::ApplicationToken);
    m.insert("apptoken", ConnectionStringKey::ApplicationToken);

    m.insert("user token", ConnectionStringKey::UserToken);
    m.insert("usertoken", ConnectionStringKey::UserToken);

    m.insert("msi auth", ConnectionStringKey::MsiAuth);
    m.insert("msi_auth", ConnectionStringKey::MsiAuth);
    m.insert("msi", ConnectionStringKey::MsiAuth);

    m.insert("msi params", ConnectionStringKey::MsiParams);
    m.insert("msi_params", ConnectionStringKey::MsiParams);
    m.insert("msi_type", ConnectionStringKey::MsiParams);

    m.insert("az cli", ConnectionStringKey::AzCli);

    m
});

// TODO: when available
// pub const PUBLIC_APPLICATION_CERTIFICATE_NAME: &str = "Public Application Certificate";
// pub const LOGIN_HINT_NAME: &str = "Login Hint";
// pub const DOMAIN_HINT_NAME: &str = "Domain Hint";
/*

       m.insert("application certificate private key", ConnectionStringKey::ApplicationCertificatePrivateKey);
       m.insert("application certificate x5c", ConnectionStringKey::ApplicationCertificateX5C);
       m.insert("application certificate send public certificate", ConnectionStringKey::ApplicationCertificateX5C);
       m.insert("application certificate sendx5c", ConnectionStringKey::ApplicationCertificateX5C);
       m.insert("sendx5c", ConnectionStringKey::ApplicationCertificateX5C);
                   ConnectionStringKey::ApplicationCertificatePrivateKey => "Application Certificate PrivateKey",
           ConnectionStringKey::ApplicationCertificateX5C => "Application Certificate x5c",
*/

/// A connection string is a string that contains the parameters that are used to connect to an ADX cluster, as well as an authentication method.
#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionString {
    /// The URI specifying the Kusto service endpoint.
    /// For example, <https://mycluster.kusto.windows.net> or net.tcp://localhost
    pub data_source: String,
    /// Instructs the client to perform Azure Active Directory login, is true by default.
    pub federated_security: bool,

    /// The authentication method to use.
    pub auth: ConnectionStringAuth,
}

/// Authentication methods to use when connecting to an ADX cluster.
#[derive(Clone)]
pub enum ConnectionStringAuth {
    /// Default credentials - uses the environment, managed identity and azure cli to authenticate. See [`DefaultAzureCredential`](azure_identity::DefaultAzureCredential) for more details.
    Default,
    /// User credentials - uses the user id and password to authenticate.
    UserAndPassword {
        /// The user id to log in with.
        user_id: String,
        /// The password to log in with.
        password: String,
    },
    /// Token - uses a fixed token to authenticate.
    Token {
        /// A Bearer token to use for authentication.
        token: String,
    },
    /// Token callback - uses a user provided callback that accepts the resource and returns a token in order to authenticate.
    TokenCallback {
        /// A callback that accepts the resource id and returns a token in order to authenticate.
        token_callback: Arc<dyn Fn(&str) -> String + Send + Sync>,
        /// The amount of time before calling the token callback again.
        time_to_live: Option<Duration>,
    },
    /// Application - uses the application client id and key to authenticate.
    Application {
        /// The application client id to use.
        client_id: String,
        /// The application key to use.
        client_secret: String,
        /// The authority or tenant id to use.
        client_authority: String,
    },
    /// Certificate - uses the application certificate to authenticate.
    ApplicationCertificate {
        /// The application client id to use.
        client_id: String,
        /// A path to the application certificate to use.
        private_certificate_path: PathBuf,
        /// Thumbprint of the application certificate to use.
        thumbprint: String,
        /// The authority or tenant id to use.
        client_authority: String,
    },
    /// MSI - uses the MSI authentication to authenticate. If `user_id` is specified, user-based MSI is used. Otherwise, system-based MSI is used.
    ManagedIdentity {
        /// An optional user id to use. If not specified, system-based MSI is used.
        user_id: Option<String>,
    },
    /// Azure CLI - uses the Azure CLI to authenticate. Run `az login` to start the process.
    AzureCli,
    /// Device code - Gives the user a device code that they have to use in order to authenticate.
    DeviceCode {
        /// Callback to activate the device code flow. If not given, will use the default of azure identity.
        callback: Option<Arc<dyn Fn(&str) -> String + Send + Sync>>,
    },
    /// Interactive - Gives the user an interactive prompt to authenticate.
    InteractiveLogin,
    /// TokenCredential - Lets the user pass any other type of token credential.
    TokenCredential {
        /// The token credential to use.
        credential: Arc<dyn TokenCredential>,
    },
}

impl ConnectionStringAuth {
    /// Turns the authentication method into a string, to be used inside of a connection string.
    /// Some methods require complex parameters, so they cannot be converted to a string:
    ///  - `TokenCallback`
    ///  - `DeviceCode`
    ///  - `TokenCredential`
    ///
    /// The `safe` parameter, when turned on, will censor private information from the connection string.
    /// It is recommended to use it.
    ///
    /// # Returns
    /// The string representation of the authentication method.
    /// If the method cannot be represented as a string, `None` is returned.
    ///
    /// # Examples
    /// ```rust
    /// use std::sync::Arc;
    /// use azure_kusto_data::prelude::*;;
    ///
    /// let user_and_pass = ConnectionStringAuth::UserAndPassword { user_id: "user".to_string(), password: "password".to_string() };
    ///
    /// assert_eq!(user_and_pass.build(false), Some("AAD User ID=user;Password=password".to_string()));
    /// assert_eq!(user_and_pass.build(true), Some("AAD User ID=user;Password=******".to_string()));
    ///
    /// let token_callback = ConnectionStringAuth::TokenCallback { token_callback: Arc::new(|_| "token".to_string()), time_to_live: None };
    ///
    /// assert_eq!(token_callback.build(true), None);
    /// ```
    #[must_use]
    pub fn build(&self, safe: bool) -> Option<String> {
        match self {
            ConnectionStringAuth::Default => Some("".to_string()),
            ConnectionStringAuth::UserAndPassword { user_id, password } => Some(format!(
                "{}={};{}={}",
                ConnectionStringKey::UserId.to_str(),
                user_id,
                ConnectionStringKey::Password.to_str(),
                if safe { CENSORED_VALUE } else { password }
            )),
            ConnectionStringAuth::Token { token } => Some(format!(
                "{}={}",
                ConnectionStringKey::ApplicationToken.to_str(),
                if safe { CENSORED_VALUE } else { token }
            )),
            ConnectionStringAuth::Application {
                client_id,
                client_secret,
                client_authority,
            } => Some(format!(
                "{}={};{}={};{}={}",
                ConnectionStringKey::ApplicationClientId.to_str(),
                client_id,
                ConnectionStringKey::ApplicationKey.to_str(),
                if safe { CENSORED_VALUE } else { client_secret },
                ConnectionStringKey::AuthorityId.to_str(),
                client_authority
            )),
            ConnectionStringAuth::ApplicationCertificate {
                client_id,
                private_certificate_path,
                thumbprint,
                client_authority,
            } => Some(format!(
                "{}={};{}={};{}={};{}={}",
                ConnectionStringKey::ApplicationClientId.to_str(),
                client_id,
                ConnectionStringKey::ApplicationCertificate.to_str(),
                private_certificate_path.display(),
                ConnectionStringKey::ApplicationCertificateThumbprint.to_str(),
                if safe { CENSORED_VALUE } else { thumbprint },
                ConnectionStringKey::AuthorityId.to_str(),
                client_authority
            )),
            ConnectionStringAuth::ManagedIdentity { user_id } => {
                if let Some(user_id) = user_id {
                    Some(format!(
                        "{}={};{}={}",
                        ConnectionStringKey::MsiAuth.to_str(),
                        CONNECTION_STRING_TRUE,
                        ConnectionStringKey::MsiParams.to_str(),
                        user_id,
                    ))
                } else {
                    Some(format!(
                        "{}={}",
                        ConnectionStringKey::MsiAuth.to_str(),
                        CONNECTION_STRING_TRUE
                    ))
                }
            }
            ConnectionStringAuth::AzureCli => Some(format!(
                "{}={}",
                ConnectionStringKey::AzCli.to_str(),
                CONNECTION_STRING_TRUE
            )),
            ConnectionStringAuth::InteractiveLogin => Some(format!(
                "{}={}",
                ConnectionStringKey::InteractiveLogin.to_str(),
                CONNECTION_STRING_TRUE
            )),
            _ => None,
        }
    }
}

impl PartialEq for ConnectionStringAuth {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ConnectionStringAuth::Default, ConnectionStringAuth::Default) => true,
            (
                ConnectionStringAuth::UserAndPassword {
                    user_id: u1,
                    password: p1,
                },
                ConnectionStringAuth::UserAndPassword {
                    user_id: u2,
                    password: p2,
                },
            ) => u1 == u2 && p1 == p2,
            (
                ConnectionStringAuth::Token { token: t1 },
                ConnectionStringAuth::Token { token: t2 },
            ) => t1 == t2,
            (
                ConnectionStringAuth::Application {
                    client_id: c1,
                    client_secret: s1,
                    client_authority: a1,
                },
                ConnectionStringAuth::Application {
                    client_id: c2,
                    client_secret: s2,
                    client_authority: a2,
                },
            ) => c1 == c2 && s1 == s2 && a1 == a2,
            (
                ConnectionStringAuth::ApplicationCertificate {
                    client_id: c1,
                    private_certificate_path: p1,
                    thumbprint: t1,
                    client_authority: a1,
                },
                ConnectionStringAuth::ApplicationCertificate {
                    client_id: c2,
                    private_certificate_path: p2,
                    thumbprint: t2,
                    client_authority: a2,
                },
            ) => c1 == c2 && p1 == p2 && t1 == t2 && a1 == a2,
            (
                ConnectionStringAuth::ManagedIdentity { user_id: u1 },
                ConnectionStringAuth::ManagedIdentity { user_id: u2 },
            ) => u1 == u2,
            (ConnectionStringAuth::AzureCli, ConnectionStringAuth::AzureCli)
            | (ConnectionStringAuth::InteractiveLogin, ConnectionStringAuth::InteractiveLogin) => {
                true
            }
            _ => false,
        }
    }
}

impl Debug for ConnectionStringAuth {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionStringAuth::Default => write!(f, "Default"),
            ConnectionStringAuth::UserAndPassword { user_id, password } => {
                write!(f, "UserAndPassword({}, {})", user_id, password)
            }
            ConnectionStringAuth::Token { token, .. } => {
                write!(f, "Token({})", token)
            }
            ConnectionStringAuth::TokenCallback { .. } => write!(f, "TokenCallback"),
            ConnectionStringAuth::Application {
                client_id,
                client_authority,
                client_secret,
            } => write!(
                f,
                "Application({}, {}, {})",
                client_id, client_authority, client_secret
            ),
            ConnectionStringAuth::ApplicationCertificate {
                client_id,
                client_authority,
                thumbprint,
                private_certificate_path,
            } => {
                write!(
                    f,
                    "ApplicationCertificate({}, {}, {}, {})",
                    client_id,
                    client_authority,
                    thumbprint,
                    private_certificate_path.display()
                )
            }
            ConnectionStringAuth::ManagedIdentity { user_id } => {
                write!(
                    f,
                    "ManagedIdentity({})",
                    user_id.as_deref().unwrap_or("<none>")
                )
            }
            ConnectionStringAuth::AzureCli => write!(f, "AzureCli"),
            ConnectionStringAuth::DeviceCode { .. } => {
                write!(f, "DeviceCode()")
            }
            ConnectionStringAuth::InteractiveLogin => write!(f, "InteractiveLogin"),
            ConnectionStringAuth::TokenCredential { .. } => write!(f, "TokenCredential"),
        }
    }
}

impl ConnectionString {
    /// Parses a connection string in order to create a `ConnectionString` object.
    /// The connection string is a series of key-value pairs separated by semicolons.
    /// # Examples
    /// ```rust
    /// use azure_kusto_data::error::Error;
    ///
    /// use azure_kusto_data::prelude::*;
    /// # fn main() -> Result<(), Error> {
    /// let connection_string = ConnectionString::from_raw_connection_string("Data Source=localhost ; Application Client Id=f6f295b1-0ce0-41f1-bba3-735accac0c69; Appkey =1234;Authority Id= 25184ef2-1dc0-4b05-84ae-f505bf7964f4 ; aad federated security = True")?;
    ///
    /// assert_eq!(connection_string.auth, ConnectionStringAuth::Application {
    ///    client_id: "f6f295b1-0ce0-41f1-bba3-735accac0c69".to_string(),
    ///   client_authority: "25184ef2-1dc0-4b05-84ae-f505bf7964f4".to_string(),
    ///  client_secret: "1234".to_string(),
    /// });
    /// assert_eq!(connection_string.data_source, "localhost");
    /// assert_eq!(connection_string.federated_security, true);
    /// # Ok::<(), Error>(()) }
    /// ```
    pub fn from_raw_connection_string(
        connection_string: &str,
    ) -> Result<Self, ConnectionStringError> {
        let kv_str_pairs = connection_string
            .split(';')
            .filter(|s| !s.chars().all(char::is_whitespace));

        let mut result_map = HashMap::<ConnectionStringKey, &str>::new();

        for kv_pair_str in kv_str_pairs {
            let mut kv = kv_pair_str.trim().split('=');
            let k = match kv.next().filter(|k| !k.chars().all(char::is_whitespace)) {
                None => {
                    return Err(ConnectionStringError::Parsing {
                        msg: "No key found".to_string(),
                    });
                }
                Some(k) => k,
            };
            let v = match kv.next().filter(|k| !k.chars().all(char::is_whitespace)) {
                None => return Err(ConnectionStringError::MissingValue { key: k.to_string() }),
                Some(v) => v,
            };

            if let Some(&key) = ALIAS_MAP.get(k.to_ascii_lowercase().trim()) {
                result_map.insert(key, v.trim());
            } else {
                return Err(ConnectionStringError::from_unexpected_key(k));
            }
        }

        let data_source = (*result_map.get(&ConnectionStringKey::DataSource).ok_or(
            ConnectionStringError::MissingValue {
                key: "data_source".to_string(),
            },
        )?)
        .to_string();

        let federated_security = result_map
            .get(&ConnectionStringKey::FederatedSecurity)
            .map_or(Ok(false), |s| parse_boolean(s, "federated_security"))?;

        if let Some(user_id) = result_map.get(&ConnectionStringKey::UserId) {
            let password = result_map
                .get(&ConnectionStringKey::Password)
                .ok_or_else(|| ConnectionStringError::from_missing_value("password"))?;

            Ok(Self {
                data_source,
                federated_security,
                auth: ConnectionStringAuth::UserAndPassword {
                    user_id: (*user_id).to_string(),
                    password: (*password).to_string(),
                },
            })
        } else if let Some(token) = result_map.get(&ConnectionStringKey::ApplicationToken) {
            Ok(Self {
                data_source,
                federated_security,
                auth: ConnectionStringAuth::Token {
                    token: (*token).to_string(),
                },
            })
        } else if let Some(token) = result_map.get(&ConnectionStringKey::UserToken) {
            Ok(Self {
                data_source,
                federated_security,
                auth: ConnectionStringAuth::Token {
                    token: (*token).to_string(),
                },
            })
        } else if let Some(client_id) = result_map.get(&ConnectionStringKey::ApplicationClientId) {
            let client_secret = result_map
                .get(&ConnectionStringKey::ApplicationKey)
                .ok_or_else(|| ConnectionStringError::from_missing_value("application_key"))?;
            let client_authority = result_map
                .get(&ConnectionStringKey::AuthorityId)
                .ok_or_else(|| ConnectionStringError::from_missing_value("authority_id"))?;
            Ok(Self {
                data_source,
                federated_security,
                auth: ConnectionStringAuth::Application {
                    client_id: (*client_id).to_string(),
                    client_secret: (*client_secret).to_string(),
                    client_authority: (*client_authority).to_string(),
                },
            })
        } else if let Some(client_id) = result_map.get(&ConnectionStringKey::ApplicationCertificate)
        {
            let private_certificate_path = result_map
                .get(&ConnectionStringKey::ApplicationCertificate)
                .ok_or_else(|| {
                    ConnectionStringError::from_missing_value("application_certificate_thumbprint")
                })?;
            let thumbprint = result_map
                .get(&ConnectionStringKey::ApplicationCertificateThumbprint)
                .ok_or_else(|| {
                    ConnectionStringError::from_missing_value("application_certificate_thumbprint")
                })?;
            let client_authority = result_map
                .get(&ConnectionStringKey::AuthorityId)
                .ok_or_else(|| ConnectionStringError::from_missing_value("authority_id"))?;
            Ok(Self {
                data_source,
                federated_security,
                auth: ConnectionStringAuth::ApplicationCertificate {
                    client_id: (*client_id).to_string(),
                    private_certificate_path: PathBuf::from(private_certificate_path),
                    thumbprint: (*thumbprint).to_string(),
                    client_authority: (*client_authority).to_string(),
                },
            })
        } else if result_map
            .get(&ConnectionStringKey::MsiAuth)
            .map(|s| parse_boolean(s, "msi_auth"))
            .transpose()?
            == Some(true)
        {
            let msi_user_id = result_map
                .get(&ConnectionStringKey::MsiParams)
                .map(|s| (*s).to_string());
            Ok(Self {
                data_source,
                federated_security,
                auth: ConnectionStringAuth::ManagedIdentity {
                    user_id: msi_user_id,
                },
            })
        } else if result_map
            .get(&ConnectionStringKey::AzCli)
            .map(|s| parse_boolean(s, "az_cli"))
            .transpose()?
            == Some(true)
        {
            Ok(Self {
                data_source,
                federated_security,
                auth: ConnectionStringAuth::AzureCli,
            })
        } else if result_map
            .get(&ConnectionStringKey::InteractiveLogin)
            .map(|s| parse_boolean(s, "interactive_login"))
            .transpose()?
            == Some(true)
        {
            Ok(Self {
                data_source,
                federated_security,
                auth: ConnectionStringAuth::InteractiveLogin,
            })
        } else {
            Ok(Self {
                data_source,
                federated_security,
                auth: ConnectionStringAuth::Default,
            })
        }
    }

    /// Creates a connection string with the default authentication credentials.
    /// Uses the environment, managed identity and azure cli to authenticate. See [`DefaultAzureCredential`](azure_identity::DefaultAzureCredential) for more details.
    /// # Example
    /// ```rust
    /// use azure_kusto_data::prelude::{ConnectionString, ConnectionStringAuth};
    ///
    /// let conn = ConnectionString::with_default_auth("https://mycluster.kusto.windows.net");
    ///
    /// assert_eq!(conn.data_source, "https://mycluster.kusto.windows.net".to_string());
    /// assert_eq!(conn.auth, ConnectionStringAuth::Default);
    ///
    /// assert_eq!(conn.build(), Some("Data Source=https://mycluster.kusto.windows.net;AAD Federated Security=True;".to_string()))
    /// ```
    #[must_use]
    pub fn with_default_auth(data_source: impl Into<String>) -> Self {
        Self {
            data_source: data_source.into(),
            federated_security: true,
            auth: ConnectionStringAuth::Default,
        }
    }

    /// Creates a connection string with user and password authentication.
    /// # Example
    /// ```rust
    /// use azure_kusto_data::prelude::{ConnectionString, ConnectionStringAuth};
    ///
    /// let conn = ConnectionString::with_user_password_auth("https://mycluster.kusto.windows.net", "user", "password");
    ///
    /// assert_eq!(conn.data_source, "https://mycluster.kusto.windows.net".to_string());
    /// assert!(matches!(conn.auth, ConnectionStringAuth::UserAndPassword { .. }));
    ///
    /// assert_eq!(conn.build(), Some("Data Source=https://mycluster.kusto.windows.net;AAD Federated Security=True;AAD User ID=user;Password=******".to_string()))
    /// ```
    #[must_use]
    pub fn with_user_password_auth(
        data_source: impl Into<String>,
        user_id: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            data_source: data_source.into(),
            federated_security: true,
            auth: ConnectionStringAuth::UserAndPassword {
                user_id: user_id.into(),
                password: password.into(),
            },
        }
    }

    /// Creates a connection string using a fixed token to authenticate.
    /// # Example
    /// ```rust
    /// use azure_kusto_data::prelude::{ConnectionString, ConnectionStringAuth};
    ///
    /// let conn = ConnectionString::with_token_auth("https://mycluster.kusto.windows.net", "token");
    ///
    /// assert_eq!(conn.data_source, "https://mycluster.kusto.windows.net".to_string());
    /// assert!(matches!(conn.auth, ConnectionStringAuth::Token { .. }));
    ///
    /// assert_eq!(conn.build(), Some("Data Source=https://mycluster.kusto.windows.net;AAD Federated Security=True;ApplicationToken=******".to_string()))
    /// ```
    #[must_use]
    pub fn with_token_auth(data_source: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            data_source: data_source.into(),
            federated_security: true,
            auth: ConnectionStringAuth::Token {
                token: token.into(),
            },
        }
    }

    /// Creates a connection string that authenticates using a callback provided by the user.
    /// # Example
    /// ```rust
    /// use std::sync::Arc;
    /// use azure_kusto_data::prelude::{ConnectionString, ConnectionStringAuth};
    ///
    /// let conn = ConnectionString::with_token_callback_auth("https://mycluster.kusto.windows.net", Arc::new(|resource_uri| resource_uri.to_string()), None);
    ///
    /// assert_eq!(conn.data_source, "https://mycluster.kusto.windows.net".to_string());
    /// assert!(matches!(conn.auth, ConnectionStringAuth::TokenCallback { .. }));
    ///
    /// // Can't be represented as a string.
    /// assert_eq!(conn.build(), None)
    /// ```
    #[must_use]
    pub fn with_token_callback_auth(
        data_source: impl Into<String>,
        token_callback: Arc<dyn Fn(&str) -> String + Send + Sync>,
        time_to_live: Option<Duration>,
    ) -> Self {
        Self {
            data_source: data_source.into(),
            federated_security: true,
            auth: ConnectionStringAuth::TokenCallback {
                token_callback,
                time_to_live,
            },
        }
    }

    /// Creates a connection string that authenticates using application id and secret.
    /// # Example
    /// ```rust
    /// use azure_kusto_data::prelude::{ConnectionString, ConnectionStringAuth};
    ///
    /// let conn = ConnectionString::with_application_auth("https://mycluster.kusto.windows.net",
    ///     "029067d2-220e-4467-99be-b74f4751270b",
    ///     "client_secret",
    ///     "e7f86dff-7a05-4b87-8c48-ed1ea5b5b814");
    /// assert_eq!(conn.data_source, "https://mycluster.kusto.windows.net".to_string());
    ///
    /// assert!(matches!(conn.auth, ConnectionStringAuth::Application { .. }));
    /// assert_eq!(conn.build(), Some("Data Source=https://mycluster.kusto.windows.net;AAD Federated Security=True;Application Client Id=029067d2-220e-4467-99be-b74f4751270b;Application Key=******;Authority Id=e7f86dff-7a05-4b87-8c48-ed1ea5b5b814".to_string()))
    /// ```
    #[must_use]
    pub fn with_application_auth(
        data_source: impl Into<String>,
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
        client_authority: impl Into<String>,
    ) -> Self {
        Self {
            data_source: data_source.into(),
            federated_security: true,
            auth: ConnectionStringAuth::Application {
                client_id: client_id.into(),
                client_secret: client_secret.into(),
                client_authority: client_authority.into(),
            },
        }
    }

    /// Creates a connection string that authenticates using a certificate.
    /// ```rust
    /// use azure_kusto_data::prelude::{ConnectionString, ConnectionStringAuth};
    ///
    /// let conn = ConnectionString::with_application_certificate_auth("https://mycluster.kusto.windows.net",
    ///     "029067d2-220e-4467-99be-b74f4751270b",
    ///     "e7f86dff-7a05-4b87-8c48-ed1ea5b5b814",
    ///     "certificate.pem",
    ///     "thumbprint");
    /// assert_eq!(conn.data_source, "https://mycluster.kusto.windows.net".to_string());
    ///
    /// assert!(matches!(conn.auth, ConnectionStringAuth::ApplicationCertificate { .. }));
    /// assert_eq!(conn.build(), Some("Data Source=https://mycluster.kusto.windows.net;AAD Federated Security=True;Application Client Id=029067d2-220e-4467-99be-b74f4751270b;ApplicationCertificate=certificate.pem;Application Certificate Thumbprint=******;Authority Id=e7f86dff-7a05-4b87-8c48-ed1ea5b5b814".to_string()))
    /// ```
    #[must_use]
    pub fn with_application_certificate_auth(
        data_source: impl Into<String>,
        client_id: impl Into<String>,
        client_authority: impl Into<String>,
        private_certificate_path: impl Into<PathBuf>,
        thumbprint: impl Into<String>,
    ) -> Self {
        Self {
            data_source: data_source.into(),
            federated_security: true,
            auth: ConnectionStringAuth::ApplicationCertificate {
                client_id: client_id.into(),
                private_certificate_path: private_certificate_path.into(),
                thumbprint: thumbprint.into(),
                client_authority: client_authority.into(),
            },
        }
    }

    /// Creates a connection string that authenticates using managed identity.
    /// If user_id is specified, user-based MSI is used. Otherwise, system-based MSI is used.
    /// # Example
    /// ```rust
    /// use azure_kusto_data::prelude::{ConnectionString, ConnectionStringAuth};
    ///
    /// let conn = ConnectionString::with_managed_identity_auth("https://mycluster.kusto.windows.net", None);
    ///
    /// assert_eq!(conn.data_source, "https://mycluster.kusto.windows.net".to_string());
    /// assert_eq!(conn.auth, ConnectionStringAuth::ManagedIdentity { user_id: None });
    ///
    /// assert_eq!(conn.build(), Some("Data Source=https://mycluster.kusto.windows.net;AAD Federated Security=True;MSI Authentication=True".to_string()))
    /// ```
    #[must_use]
    pub fn with_managed_identity_auth(
        data_source: impl Into<String>,
        user_id: impl Into<Option<String>>,
    ) -> Self {
        Self {
            data_source: data_source.into(),
            federated_security: true,
            auth: ConnectionStringAuth::ManagedIdentity {
                user_id: user_id.into(),
            },
        }
    }

    /// Creates a connection string that authenticates using the azure cli.
    /// For more information see [the docs](https://docs.microsoft.com/en-us/cli/azure/authenticate-azure-cli)
    /// # Example
    /// ```rust
    /// use azure_kusto_data::prelude::{ConnectionString, ConnectionStringAuth};
    ///
    /// let conn = ConnectionString::with_azure_cli_auth("https://mycluster.kusto.windows.net");
    ///
    /// assert_eq!(conn.data_source, "https://mycluster.kusto.windows.net".to_string());
    /// assert_eq!(conn.auth, ConnectionStringAuth::AzureCli);
    ///
    /// assert_eq!(conn.build(), Some("Data Source=https://mycluster.kusto.windows.net;AAD Federated Security=True;AZ CLI=True".to_string()))
    /// ```
    #[must_use]
    pub fn with_azure_cli_auth(data_source: impl Into<String>) -> Self {
        Self {
            data_source: data_source.into(),
            federated_security: true,
            auth: ConnectionStringAuth::AzureCli,
        }
    }

    /// Creates a connection string that uses the flow of device code authentication.
    /// Usually, the code will be displayed on the screen, and the user will have to navigate to a web page and enter the code.
    /// # Example
    /// ```rust
    /// use std::sync::Arc;
    /// use azure_kusto_data::prelude::{ConnectionString, ConnectionStringAuth};
    ///
    /// let conn = ConnectionString::with_device_code_auth("https://mycluster.kusto.windows.net", Some(Arc::new(|code| code.to_string())));
    ///
    /// assert_eq!(conn.data_source, "https://mycluster.kusto.windows.net".to_string());
    /// assert!(matches!(conn.auth, ConnectionStringAuth::DeviceCode { .. }));
    ///
    /// // Can't be represented as a string.
    /// assert_eq!(conn.build(), None)
    /// ```
    #[must_use]
    pub fn with_device_code_auth(
        data_source: impl Into<String>,
        callback: Option<Arc<dyn Fn(&str) -> String + Send + Sync>>,
    ) -> Self {
        Self {
            data_source: data_source.into(),
            federated_security: true,
            auth: ConnectionStringAuth::DeviceCode { callback },
        }
    }

    /// Creates a connection string that authenticates using an interactive login prompt.
    /// # Example
    /// ```rust
    /// use azure_kusto_data::prelude::{ConnectionString, ConnectionStringAuth};
    ///
    /// let conn = ConnectionString::with_interactive_login_auth("https://mycluster.kusto.windows.net");
    ///
    /// assert_eq!(conn.data_source, "https://mycluster.kusto.windows.net".to_string());
    /// assert_eq!(conn.auth, ConnectionStringAuth::InteractiveLogin);
    ///
    /// assert_eq!(conn.build(), Some("Data Source=https://mycluster.kusto.windows.net;AAD Federated Security=True;Interactive Login=True".to_string()))
    /// ```
    #[must_use]
    pub fn with_interactive_login_auth(data_source: impl Into<String>) -> Self {
        Self {
            data_source: data_source.into(),
            federated_security: true,
            auth: ConnectionStringAuth::InteractiveLogin,
        }
    }

    /// Creates a connection string that uses the flow of device code authentication.
    /// Usually, the code will be displayed on the screen, and the user will have to navigate to a web page and enter the code.
    /// # Example
    /// ```rust
    /// use std::sync::Arc;
    /// use azure_identity::DefaultAzureCredential;
    /// use azure_kusto_data::prelude::{ConnectionString, ConnectionStringAuth};
    ///
    /// let conn = ConnectionString::with_token_credential("https://mycluster.kusto.windows.net", Arc::new(DefaultAzureCredential::default()));
    ///
    /// assert_eq!(conn.data_source, "https://mycluster.kusto.windows.net".to_string());
    /// assert!(matches!(conn.auth, ConnectionStringAuth::TokenCredential { .. }));
    ///
    /// // Can't be represented as a string.
    /// assert_eq!(conn.build(), None)
    /// ```
    #[must_use]
    pub fn with_token_credential(
        data_source: impl Into<String>,
        token_credential: Arc<dyn TokenCredential>,
    ) -> Self {
        Self {
            data_source: data_source.into(),
            federated_security: true,
            auth: ConnectionStringAuth::TokenCredential {
                credential: token_credential,
            },
        }
    }

    /// Builds the connection string into a string.
    /// By default, it will include the authentication, and censor secrets.
    /// If you want to use different options, use the [build_with_options](#method.build_with_options) method.
    /// # Example
    /// ```rust
    /// use azure_kusto_data::prelude::{ConnectionString, ConnectionStringAuth};
    ///
    /// let conn = ConnectionString::with_user_password_auth("https://mycluster.kusto.windows.net", "user", "password");
    ///
    /// assert_eq!(conn.build(), Some("Data Source=https://mycluster.kusto.windows.net;AAD Federated Security=True;AAD User ID=user;Password=******".to_string()));
    #[must_use]
    pub fn build(&self) -> Option<String> {
        self.build_with_options(true, false)
    }

    /// Builds the connection string into a string.
    /// You can specify if you want to include the authentication, and if you want to censor secrets.
    /// # Example
    /// ```rust
    /// use azure_kusto_data::prelude::{ConnectionString, ConnectionStringAuth};
    ///
    /// let conn = ConnectionString::with_user_password_auth("https://mycluster.kusto.windows.net", "user", "password");
    ///
    /// assert_eq!(conn.build_with_options(false, false), Some("Data Source=https://mycluster.kusto.windows.net;AAD Federated Security=True;AAD User ID=user;Password=password".to_string()));
    /// assert_eq!(conn.build_with_options(false, true), Some("Data Source=https://mycluster.kusto.windows.net;AAD Federated Security=True".to_string()));
    #[must_use]
    pub fn build_with_options(&self, safe: bool, ignore_auth: bool) -> Option<String> {
        let mut s = format!(
            "{}={};{}={}",
            ConnectionStringKey::DataSource.to_str(),
            self.data_source,
            ConnectionStringKey::FederatedSecurity.to_str(),
            if self.federated_security {
                CONNECTION_STRING_TRUE
            } else {
                CONNECTION_STRING_FALSE
            }
        );
        if !ignore_auth {
            s.push(';');
            if let Some(auth) = self.auth.build(safe) {
                s.push_str(&auth);
            } else {
                return None;
            }
        }

        Some(s)
    }

    pub(crate) fn into_data_source_and_credentials(self) -> (String, Arc<dyn TokenCredential>) {
        (
            self.data_source,
            match self.auth {
                ConnectionStringAuth::Default => Arc::new(DefaultAzureCredential::default()),
                ConnectionStringAuth::UserAndPassword { .. } => unimplemented!(),
                ConnectionStringAuth::Token { token } => Arc::new(ConstTokenCredential { token }),
                ConnectionStringAuth::TokenCallback {
                    token_callback,
                    time_to_live,
                } => Arc::new(CallbackTokenCredential {
                    token_callback,
                    time_to_live,
                }),
                ConnectionStringAuth::Application {
                    client_id,
                    client_secret,
                    client_authority,
                } => Arc::new(ClientSecretCredential::new(
                    client_authority,
                    client_id,
                    client_secret,
                    TokenCredentialOptions::default(),
                )),
                ConnectionStringAuth::ApplicationCertificate { .. } => unimplemented!(),
                ConnectionStringAuth::ManagedIdentity { user_id } => {
                    if let Some(user_id) = user_id {
                        Arc::new(ImdsManagedIdentityCredential::default().with_object_id(user_id))
                    } else {
                        Arc::new(ImdsManagedIdentityCredential::default())
                    }
                }
                ConnectionStringAuth::AzureCli => Arc::new(AzureCliCredential),
                ConnectionStringAuth::DeviceCode { .. } => unimplemented!(),
                ConnectionStringAuth::InteractiveLogin => unimplemented!(),
                ConnectionStringAuth::TokenCredential { credential } => credential.clone(),
            },
        )
    }
}

fn parse_boolean(term: &str, name: &str) -> Result<bool, ConnectionStringError> {
    match term.to_lowercase().trim() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(ConnectionStringError::from_parsing_error(format!(
            "Unexpected value for {}: {}. Please specify either 'true' or 'false'.",
            name, term
        ))),
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;

    #[test]
    fn it_returns_expected_errors() {
        assert!(matches!(
            ConnectionString::from_raw_connection_string("Data Source="),
            Err(ConnectionStringError::MissingValue { key }) if key == "Data Source"
        ));
        assert!(matches!(
            ConnectionString::from_raw_connection_string("="),
            Err(ConnectionStringError::Parsing { msg: _ })
        ));
        assert!(matches!(
            ConnectionString::from_raw_connection_string("x=123;"),
            Err(ConnectionStringError::UnexpectedKey { key }) if key == "x"
        ));
    }

    #[test]
    fn it_parses_basic_cases() {
        assert_eq!(
            ConnectionString::from_raw_connection_string("Data Source=ds"),
            Ok(ConnectionString {
                data_source: "ds".to_string(),
                federated_security: false,
                auth: ConnectionStringAuth::Default,
            })
        );
        assert_eq!(
            ConnectionString::from_raw_connection_string("addr=ds"),
            Ok(ConnectionString {
                data_source: "ds".to_string(),
                federated_security: false,
                auth: ConnectionStringAuth::Default,
            })
        );
        assert_eq!(
            ConnectionString::from_raw_connection_string(
                "Data Source=ds;Application Client Id=cid;Application Key=key;Tenant=tid",
            ),
            Ok(ConnectionString {
                data_source: "ds".to_string(),
                federated_security: false,
                auth: ConnectionStringAuth::Application {
                    client_id: "cid".to_string(),
                    client_secret: "key".to_string(),
                    client_authority: "tid".to_string(),
                },
            })
        );
        assert_eq!(
            ConnectionString::from_raw_connection_string(
                "Data Source=ds;Federated=True;AppToken=token"
            ),
            Ok(ConnectionString {
                data_source: "ds".to_string(),
                federated_security: true,
                auth: ConnectionStringAuth::Token {
                    token: "token".to_string()
                },
            })
        );
    }
}
