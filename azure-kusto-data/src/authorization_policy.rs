use crate::cloud_info::CloudInfo;
use crate::prelude::ConnectionStringAuth;
use azure_core::headers::AUTHORIZATION;
use azure_core::{
    auth::TokenCredential, ClientOptions, Context, Pipeline, Policy, PolicyResult, Request,
};
use futures::lock::Mutex;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub struct AuthorizationPolicy {
    auth: ConnectionStringAuth,
    raw_resource: String,
    credential: Mutex<Option<(Arc<dyn TokenCredential>, String)>>,
}

impl Debug for AuthorizationPolicy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthorizationPolicy")
            .field("auth", &self.auth)
            .field("raw_resource", &self.raw_resource)
            .finish()
    }
}

impl AuthorizationPolicy {
    pub(crate) fn new(auth: ConnectionStringAuth, raw_resource: String) -> Self {
        Self {
            auth,
            raw_resource,
            credential: Mutex::new(None),
        }
    }
}

#[async_trait::async_trait]
impl Policy for AuthorizationPolicy {
    async fn send(
        &self,
        ctx: &Context,
        request: &mut Request,
        next: &[Arc<dyn Policy>],
    ) -> PolicyResult {
        assert!(
            !next.is_empty(),
            "Authorization policies cannot be the last policy of a pipeline"
        );

        let (cred, resource) = {
            let mut lock = self.credential.lock().await;
            if let Some((cred, resource)) = lock.clone() {
                (cred, resource)
            } else {
                let cloud_info = CloudInfo::get(
                    &Pipeline::new(
                        option_env!("CARGO_PKG_NAME"),
                        option_env!("CARGO_PKG_VERSION"),
                        ClientOptions::default(),
                        Vec::new(),
                        Vec::new(),
                    ),
                    &self.raw_resource,
                )
                .await
                .unwrap_or_default();

                *lock = Some((
                    self.auth.clone().into_credential(),
                    cloud_info.get_resource_uri().to_string(),
                ));

                lock.clone().unwrap()
            }
        };

        let token = cred.get_token(&[".default"]).await?;

        request.insert_header(AUTHORIZATION, &format!("Bearer {}", dbg!(token.token.secret())));

        next[0].send(ctx, request, &next[1..]).await
    }
}
