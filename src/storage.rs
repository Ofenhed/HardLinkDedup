use std::{
  io::{Error, ErrorKind, Result},
  path::{Path, PathBuf},
};

use blake3::Hasher;
use tokio::{fs, io::AsyncReadExt, join};

use crate::{
  os::{read_link_metadata, FileId, FileLinkBackend, StorageUid},
  DedupArgs, Filesize, HashDigest,
};

#[derive(Debug)]
pub struct FileStorageData {
  pub path: PathBuf,
  pub size: Filesize,
  pub storage_uid: StorageUid,
  pub file_id: FileId,
}

impl FileStorageData {
  pub async fn new(path: impl AsRef<Path>) -> Result<Self> {
    let path = path.as_ref().to_owned();
    let (link_metadata, metadata) = join!(read_link_metadata(&path), fs::metadata(&path));
    let link_metadata = link_metadata?;
    Ok(FileStorageData {
      path,
      size: metadata?.len().try_into().unwrap(),
      storage_uid: link_metadata.get_storage_uid(),
      file_id: link_metadata.get_file_id(),
    })
  }
}

pub async fn calculate_file_hash(
  path: impl AsRef<Path>,
  expected_size: Filesize,
) -> Result<HashDigest> {
  let mut reader = fs::OpenOptions::new()
    .create(false)
    .read(true)
    .open(&path)
    .await?;
  let mut read_buf = vec![0; DedupArgs::get().buffer_size * 1024];
  let mut hash = Hasher::new();
  let mut file_length = 0;
  loop {
    let bytes_read = reader.read(&mut read_buf[..]).await?;
    file_length += bytes_read;
    if bytes_read == 0 {
      break;
    }
    hash.update(&read_buf[..bytes_read]);
  }
  if file_length != expected_size as usize {
    return Err(Error::new(
      ErrorKind::BrokenPipe,
      format!(
        "The entire file {} could not be hashed",
        path.as_ref().display()
      ),
    ))?;
  }
  Ok(hash.finalize().into())
}
