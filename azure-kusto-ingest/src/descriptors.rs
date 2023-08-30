use uuid::Uuid;

/// Helper for adding authentication information to a blob path in the format expected by Kusto
#[derive(Clone)]
pub enum BlobAuth {
    /// adds `?<sas_token>` to the blob path
    SASToken(String),
    /// adds `;managed_identity=<identity>` to the blob path
    UserAssignedManagedIdentity(String),
    /// adds `;managed_identity=system` to the blob path
    SystemAssignedManagedIdentity,
}

impl std::fmt::Debug for BlobAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlobAuth::SASToken(_) => f.debug_struct("SASToken").finish(),
            BlobAuth::UserAssignedManagedIdentity(object_id) => f
                .debug_struct("UserAssignedManagedIdentity")
                .field("object_id", object_id)
                .finish(),
            BlobAuth::SystemAssignedManagedIdentity => {
                f.debug_struct("SystemAssignedManagedIdentity").finish()
            }
        }
    }
}

/// Encapsulates the information related to a blob that is required to ingest from a blob
#[derive(Debug, Clone)]
pub struct BlobDescriptor {
    uri: String,
    pub(crate) size: Option<u64>,
    pub(crate) source_id: Uuid,
    /// Authentication information for the blob; when [None], the uri is passed through as is
    blob_auth: Option<BlobAuth>,
}

impl BlobDescriptor {
    pub fn new(uri: String, size: Option<u64>, source_id: Option<Uuid>) -> Self {
        let source_id = match source_id {
            Some(source_id) => source_id,
            None => Uuid::new_v4(),
        };

        Self {
            uri,
            size,
            source_id,
            blob_auth: None,
        }
    }

    /// Mutator to modify the authentication information of the BlobDescriptor
    pub fn with_blob_auth(mut self, blob_auth: BlobAuth) -> Self {
        self.blob_auth = Some(blob_auth);
        self
    }

    /// Returns the uri with the authentication information added
    pub fn uri(&self) -> String {
        match &self.blob_auth {
            Some(BlobAuth::SASToken(sas_token)) => {
                format!("{}?{}", self.uri, sas_token.as_str())
            }
            Some(BlobAuth::UserAssignedManagedIdentity(object_id)) => {
                format!("{};managed_identity={}", self.uri, object_id)
            }
            Some(BlobAuth::SystemAssignedManagedIdentity) => {
                format!("{};managed_identity=system", self.uri)
            }
            None => self.uri.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blob_descriptor_with_no_auth_modification() {
        let uri = "https://mystorageaccount.blob.core.windows.net/mycontainer/myblob";
        let blob_descriptor = BlobDescriptor::new(uri.to_string(), None, None);

        assert_eq!(blob_descriptor.uri(), uri);
    }

    #[test]
    fn blob_descriptor_with_sas_token() {
        let uri = "https://mystorageaccount.blob.core.windows.net/mycontainer/myblob";
        let sas_token = "my_sas_token";
        let blob_descriptor = BlobDescriptor::new(uri.to_string(), None, None)
            .with_blob_auth(BlobAuth::SASToken(sas_token.to_string()));

        assert_eq!(blob_descriptor.uri(), format!("{}?{}", uri, sas_token));
    }

    #[test]
    fn blob_descriptor_with_user_assigned_managed_identity() {
        let uri = "https://mystorageaccount.blob.core.windows.net/mycontainer/myblob";
        let object_id = "my_object_id";
        let blob_descriptor = BlobDescriptor::new(uri.to_string(), None, None)
            .with_blob_auth(BlobAuth::UserAssignedManagedIdentity(object_id.to_string()));

        assert_eq!(
            blob_descriptor.uri(),
            format!("{};managed_identity={}", uri, object_id)
        );
    }

    #[test]
    fn blob_descriptor_with_system_assigned_managed_identity() {
        let uri = "https://mystorageaccount.blob.core.windows.net/mycontainer/myblob";
        let blob_descriptor = BlobDescriptor::new(uri.to_string(), None, None)
            .with_blob_auth(BlobAuth::SystemAssignedManagedIdentity);

        assert_eq!(
            blob_descriptor.uri(),
            format!("{};managed_identity=system", uri)
        );
    }

    #[test]
    fn blob_descriptor_with_size() {
        let uri = "https://mystorageaccount.blob.core.windows.net/mycontainer/myblob";
        let size = 123;
        let blob_descriptor = BlobDescriptor::new(uri.to_string(), Some(size), None);

        assert_eq!(blob_descriptor.size, Some(size));
    }

    #[test]
    fn blob_descriptor_with_source_id() {
        let uri = "https://mystorageaccount.blob.core.windows.net/mycontainer/myblob";
        let source_id = Uuid::new_v4();
        let blob_descriptor = BlobDescriptor::new(uri.to_string(), None, Some(source_id));

        assert_eq!(blob_descriptor.source_id, source_id);
    }
}
