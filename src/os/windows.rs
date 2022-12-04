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

impl Storage {
  fn get_file_info(file: FileHandle) -> Result<BY_HANDLE_FILE_INFORMATION> {
    let file = match file {
      FileHandle::File(f) => f,
      FileHandle::Path(path) => File::open(path)?,
    };
    let mut info = BY_HANDLE_FILE_INFORMATION::default();
    let info_ptr: *mut BY_HANDLE_FILE_INFORMATION = &mut info;
    let handle = HANDLE(unsafe { *(file.as_raw_handle() as *const isize) });
    if unsafe { GetFileInformationByHandle(handle, info_ptr).as_bool() } {
      Ok(info)
    } else {
      return Err(Error::last_os_error())?;
    }
  }
}

#[async_trait]
impl FileBackend for Storage {
  type StorageUid = u32;

  type FileUid = u64;

  async fn get_storage_uid(file: PathBuf) -> Result<Self::StorageUid> {
    let info = Storage::get_file_info(FileHandle::Path(file))?;
    Ok(info.dwVolumeSerialNumber)
  }

  async fn get_file_uid(file: PathBuf) -> Result<Self::FileUid> {
    let info = Storage::get_file_info(FileHandle::Path(file))?;
    Ok((info.nFileIndexHigh as Self::FileUid) << 32 | (info.nFileIndexLow as Self::FileUid))
  }
}
