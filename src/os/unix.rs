use super::{FileBackend, FileLinkBackend};
use async_trait::async_trait;
use std::{
  fs::{File, Metadata},
  io::Result,
  os::unix::fs::MetadataExt,
  path::PathBuf,
};
use tokio::fs;

#[async_trait]
impl FileBackend for fs::File {
  type Metadata = Metadata;

  async fn link_metadata(self) -> Result<Self::Metadata> {
    Ok(self.metadata().await?)
  }
}

#[async_trait]
impl FileBackend for &DirEntry {
  type Metadata = Metadata;

  async fn link_metadata(self) -> Result<Self::Metadata> {
    Ok(self.metadata()?)
  }
}

#[async_trait]
impl FileBackend for &Path {
  type Metadata = Metadata;

  async fn link_metadata(self) -> Result<Self::Metadata> {
    Ok(fs::metadata(self).await?)
  }
}

impl FileLinkBackend for Metadata {
  type StorageUid = u64;

  type FileId = u64;

  fn get_storage_uid(&self) -> Self::StorageUid {
    self.dev()
  }

  fn get_file_id(&self) -> Self::FileId {
    self.ino()
  }
}
