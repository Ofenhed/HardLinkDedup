use super::{FileBackend, FileLinkBackend};
use async_trait::async_trait;
use std::{
  fs::Metadata,
  io::{Error, ErrorKind, Result},
  os::windows::fs::MetadataExt,
  path::Path,
};
use tokio::fs;

pub struct LinkMetadata {
  storage: u32,
  file: u64,
}

impl TryFrom<Metadata> for LinkMetadata {
  type Error = Error;

  fn try_from(metadata: Metadata) -> Result<LinkMetadata> {
    let (Some(storage), Some(file)) = (metadata.volume_serial_number(), metadata.file_index())
    else {
      return Err(Error::new(ErrorKind::NotFound, "File metadata not found"));
    };
    Ok(LinkMetadata { storage, file })
  }
}

#[async_trait]
impl FileBackend for &fs::DirEntry {
  type Metadata = LinkMetadata;

  async fn link_metadata(self) -> Result<Self::Metadata> {
    Ok(self.metadata().await?.try_into()?)
  }
}

#[async_trait]
impl FileBackend for &Path {
  type Metadata = LinkMetadata;

  async fn link_metadata(self) -> Result<Self::Metadata> {
    Ok(fs::metadata(self).await?.try_into()?)
  }
}

impl FileLinkBackend for LinkMetadata {
  type StorageUid = u32;

  type FileId = u64;

  fn get_storage_uid(&self) -> Self::StorageUid {
    self.storage
  }

  fn get_file_id(&self) -> Self::FileId {
    self.file
  }
}
