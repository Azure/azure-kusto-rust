//! Custom credentials for Azure Data Explorer.

use crate::connection_string::TokenCallbackFunction;
use azure_core::auth::{AccessToken, TokenCredential, TokenResponse};
use std::time::Duration;
use time::OffsetDateTime;

const SECONDS_IN_50_YEARS: u64 = 60 * 60 * 24 * 365 * 50;

/// Uses a fixed token to authenticate.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConstTokenCredential {
    pub(crate) token: String,
}
#[async_trait::async_trait]
impl TokenCredential for ConstTokenCredential {
    async fn get_token(&self, _resource: &str) -> azure_core::Result<TokenResponse> {
        Ok(TokenResponse {
            token: AccessToken::new(self.token.clone()),
            expires_on: OffsetDateTime::now_utc() + Duration::from_secs(SECONDS_IN_50_YEARS),
        })
    }
}

/// Uses a user provided callback that accepts the resource and returns a token in order to authenticate.
pub struct CallbackTokenCredential {
    pub(crate) token_callback: TokenCallbackFunction,
    pub(crate) time_to_live: Option<Duration>,
}

#[async_trait::async_trait]
impl TokenCredential for CallbackTokenCredential {
    async fn get_token(&self, resource: &str) -> azure_core::Result<TokenResponse> {
        let callback = &self.token_callback;
        Ok(TokenResponse {
            token: AccessToken::new(callback(resource)),
            expires_on: OffsetDateTime::now_utc()
                + self
                    .time_to_live
                    .unwrap_or(Duration::from_secs(SECONDS_IN_50_YEARS)),
        })
    }
}
