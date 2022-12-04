use async_trait::async_trait;
use std::{io::Result, path::PathBuf, hash::Hash};

#[cfg(unix)]
mod unix;
#[cfg(unix)]
pub use unix::Storage;
#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub use self::windows::Storage;

#[async_trait]
trait FileBackend {
  type StorageUid: Eq + Send + Hash;
  type FileUid: Eq + Send + Hash;
  type Metadata: Send;
  async fn metadata(path: PathBuf) -> Result<Self::Metadata>;
  fn get_storage_uid(file: &Self::Metadata) -> Result<Self::StorageUid>;
  fn get_file_uid(file: &Self::Metadata) -> Result<Self::FileUid>;
  fn same_storage(file1: &Self::Metadata, file2: &Self::Metadata) -> std::io::Result<bool> {
    let (file1_uid, file2_uid) = (Self::get_storage_uid(&file1), Self::get_storage_uid(&file2));
    Ok(file1_uid? == file2_uid?)
  }
  fn same_file(file1: &Self::Metadata, file2: &Self::Metadata) -> std::io::Result<bool> {
    let (file1_storage, file2_storage, file1_uid, file2_uid) = (
      Self::get_storage_uid(&file1),
      Self::get_storage_uid(&file2),
      Self::get_file_uid(&file1),
      Self::get_file_uid(&file2),
    );
    Ok((file1_storage?, file1_uid?) == (file2_storage?, file2_uid?))
  }
}
