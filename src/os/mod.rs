use async_trait::async_trait;
use std::{io::Result as IoResult, path::PathBuf};
use tokio::join;

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
  type StorageUid: Eq + Send;
  type FileUid: Eq + Send;
  async fn get_storage_uid(file: PathBuf) -> IoResult<Self::StorageUid>;
  async fn get_file_uid(file: PathBuf) -> IoResult<Self::FileUid>;
  async fn same_file(file1: PathBuf, file2: PathBuf) -> std::io::Result<bool> {
    let (file1_storage, file2_storage, file1_uid, file2_uid) = join!(
      Self::get_storage_uid(file1.clone()),
      Self::get_storage_uid(file2.clone()),
      Self::get_file_uid(file1),
      Self::get_file_uid(file2)
    );
    let (file1, file2) = ((file1_storage?, file1_uid?), (file2_storage?, file2_uid?));
    Ok(file1 == file2)
  }
}
