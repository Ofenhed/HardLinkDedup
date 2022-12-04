use super::FileBackend;
use async_trait::async_trait;
use std::{os::unix::fs::MetadataExt, path::PathBuf};
use tokio::fs;

pub struct Storage {}

#[async_trait]
impl FileBackend for Storage {
  type StorageUid = u64;

  type FileUid = u64;

  async fn get_storage_uid(file: PathBuf) -> std::io::Result<Self::StorageUid> {
    Ok(fs::metadata(file).await?.dev())
  }

  async fn get_file_uid(file: PathBuf) -> std::io::Result<Self::FileUid> {
    Ok(fs::metadata(file).await?.ino())
  }
}
