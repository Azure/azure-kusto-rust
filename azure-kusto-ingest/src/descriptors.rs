use std::{io::Read, path::PathBuf, fmt::format};

use azure_storage::StorageCredentials;
use url::Url;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub enum BlobAuth {
    SASToken(),
    // adds `;managed_identity=<identity>` to the blob path
    UserAssignedManagedIdentity(String),
    // adds `;managed_identity=system` to the blob path
    SystemAssignedManagedIdentity
}

#[derive(Clone, Debug)]
pub struct BlobDescriptor {
    uri: Url,
    pub(crate) size: Option<u64>,
    pub(crate) source_id: Uuid,
    blob_auth: Option<BlobAuth>
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

    pub fn with_blob_auth(mut self, blob_auth: BlobAuth) -> Self {
        self.blob_auth = Some(blob_auth);
        self
    }

    pub fn uri(&self) -> String {
        match &self.blob_auth {
            Some(BlobAuth::SASToken()) => {
                let mut uri = self.uri.clone();
                uri.set_query(Some("sas_token"));
                uri.to_string()
            },
            Some(BlobAuth::UserAssignedManagedIdentity(object_id)) => {
                format!("{};managed_identity={}", self.uri, object_id)
            },
            Some(BlobAuth::SystemAssignedManagedIdentity) => {
                format!("{};managed_identity=system", self.uri)
            },
            None => self.uri.to_string(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct FileDescriptor {
    pub path: PathBuf,
    pub size: Option<u64>,
    pub source_id: Uuid,
}

impl FileDescriptor {
    pub fn new(path: PathBuf, size: Option<u64>, source_id: Option<Uuid>) -> Self {
        unimplemented!()
    }
}

// #[derive(Clone, Debug)]
pub struct StreamDescriptor {
    stream: Box<dyn Read>,
    size: Option<u64>,
    source_id: Uuid,
    compressed: bool,
    stream_name: String,
}

impl StreamDescriptor {
    pub fn new(
        stream: Box<dyn Read>,
        size: Option<u64>,
        source_id: Option<Uuid>,
        compressed: bool,
        stream_name: String,
    ) -> Self {
        unimplemented!()
    }

    pub fn from_file_descriptor(file_descriptor: FileDescriptor) -> Self {
        unimplemented!()
    }
}
