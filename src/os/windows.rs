use super::FileBackend;
use async_trait::async_trait;
use std::{
  fs::File,
  io::{Error, Result},
  os::windows::io::AsRawHandle,
  path::PathBuf,
};
use tokio::fs;
use windows::Win32::{
  Foundation::HANDLE,
  Storage::FileSystem::{GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION},
};

pub struct Storage {}

enum FileHandle {
  File(File),
  Path(PathBuf),
}

impl Storage {}

#[async_trait]
impl FileBackend for Storage {
  type StorageUid = u32;

  type FileUid = u64;

  type Metadata = BY_HANDLE_FILE_INFORMATION;

  async fn metadata(path: PathBuf) -> Result<Self::Metadata> {
    let file = File::open(path)?;
    let mut info = BY_HANDLE_FILE_INFORMATION::default();
    let info_ptr: *mut BY_HANDLE_FILE_INFORMATION = &mut info;
    let handle = HANDLE(unsafe { *(file.as_raw_handle() as *const isize) });
    if unsafe { GetFileInformationByHandle(handle, info_ptr).as_bool() } {
      Ok(info)
    } else {
      return Err(Error::last_os_error())?;
    }
  }

  fn get_storage_uid(info: &Self::Metadata) -> Result<Self::StorageUid> {
    Ok(info.dwVolumeSerialNumber)
  }

  fn get_file_uid(info: &Self::Metadata) -> Result<Self::FileUid> {
    Ok((info.nFileIndexHigh as Self::FileUid) << 32 | (info.nFileIndexLow as Self::FileUid))
  }
}
