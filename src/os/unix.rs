use super::FileBackend;
use async_trait::async_trait;
use std::{fs::Metadata, io::Result, os::unix::fs::MetadataExt, path::PathBuf};
use tokio::fs;

pub struct Storage {}

#[async_trait]
impl FileBackend for Storage {
  type StorageUid = u64;

  type FileUid = u64;

  type Metadata = Metadata;

  async fn metadata(path: PathBuf) -> Result<Self::Metadata> {
    Ok(fs::metadata(path).await?)
  }

  fn get_storage_uid(metadata: &Self::Metadata) -> Result<Self::StorageUid> {
    Ok(metadata.dev())
  }

  fn get_file_uid(metadata: &Self::Metadata) -> Result<Self::FileUid> {
    Ok(metadata.ino())
  }
}
