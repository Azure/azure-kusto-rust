use url::Url;
use uuid::Uuid;

/// Helper enum for authentication information on a blob
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
    uri: Url,
    pub(crate) size: Option<u64>,
    pub(crate) source_id: Uuid,
    /// Authentication information for the blob; when [None], the uri is passed through as is
    blob_auth: Option<BlobAuth>,
}

impl BlobDescriptor {
    pub fn new(uri: Url, size: Option<u64>, source_id: Option<Uuid>) -> Self {
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
                let mut uri = self.uri.clone();
                uri.set_query(Some(sas_token.as_str()));
                uri.to_string()
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

// #[derive(Clone, Debug)]
// pub struct FileDescriptor {
//     pub path: PathBuf,
//     pub size: Option<u64>,
//     pub source_id: Uuid,
// }

// impl FileDescriptor {
//     pub fn new(path: PathBuf, size: Option<u64>, source_id: Option<Uuid>) -> Self {
//         unimplemented!()
//     }
// }

// #[derive(Clone, Debug)]
// pub struct StreamDescriptor {
//     stream: Box<dyn Read>,
//     size: Option<u64>,
//     source_id: Uuid,
//     compressed: bool,
//     stream_name: String,
// }

// impl StreamDescriptor {
//     pub fn new(
//         stream: Box<dyn Read>,
//         size: Option<u64>,
//         source_id: Option<Uuid>,
//         compressed: bool,
//         stream_name: String,
//     ) -> Self {
//         unimplemented!()
//     }
// }
