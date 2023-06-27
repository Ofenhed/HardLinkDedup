use super::{FileBackend, FileLinkBackend};
use async_trait::async_trait;
use std::{
  fs::File,
  io::{Error, Result},
  os::windows::io::AsRawHandle,
  path::Path,
};
use tokio::fs;
use windows::Win32::{
  Foundation::HANDLE,
  Storage::FileSystem::{GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION},
};

#[async_trait]
impl FileBackend for File {
  type Metadata = BY_HANDLE_FILE_INFORMATION;

  async fn link_metadata(self) -> Result<Self::Metadata> {
    let mut info = BY_HANDLE_FILE_INFORMATION::default();
    let info_ptr: *mut BY_HANDLE_FILE_INFORMATION = &mut info;
    let handle = HANDLE(self.as_raw_handle() as isize);
    if unsafe { GetFileInformationByHandle(handle, info_ptr).as_bool() } {
      Ok(info)
    } else {
      return Err(Error::last_os_error())?;
    }
  }
}

#[async_trait]
impl FileBackend for fs::File {
  type Metadata = BY_HANDLE_FILE_INFORMATION;

  async fn link_metadata(self) -> Result<Self::Metadata> {
    let new_file = self.into_std().await;
    Ok(new_file.link_metadata().await?)
  }
}

#[async_trait]
impl FileBackend for &Path {
  type Metadata = BY_HANDLE_FILE_INFORMATION;

  async fn link_metadata(self) -> Result<Self::Metadata> {
    let file = File::open(self)?;
    Ok(file.link_metadata().await?)
  }
}

impl FileLinkBackend for BY_HANDLE_FILE_INFORMATION {
  type StorageUid = u32;

  type FileId = u64;

  fn get_storage_uid(&self) -> Self::StorageUid {
    self.dwVolumeSerialNumber
  }

  fn get_file_id(&self) -> Self::FileId {
    (self.nFileIndexHigh as u64) << 32 | (self.nFileIndexLow as u64)
  }
}
