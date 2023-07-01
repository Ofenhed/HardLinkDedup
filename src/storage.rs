use std::{
  cmp::min,
  io::{Error, ErrorKind},
  path::Path,
  sync::{Arc, OnceLock},
};

use anyhow::{Context, Result};
use blake3::Hasher;
use tokio::{fs, io::AsyncReadExt, join, sync::Semaphore};

use crate::{
  os::{read_link_metadata, FileId, FileLinkBackend, StorageUid},
  DedupArgs, Filesize, HashDigest,
};

#[derive(Debug, Clone)]
pub struct FileStorageData {
  pub path: Arc<Path>,
  pub size: Filesize,
  pub storage_uid: StorageUid,
  pub file_id: FileId,
}

impl FileStorageData {
  pub async fn new(path: impl AsRef<Path>) -> Result<Self> {
    let path = path.as_ref().to_owned();
    let (link_metadata, metadata) = join!(read_link_metadata(&path), fs::metadata(&path));
    let link_metadata = link_metadata?;
    #[allow(clippy::useless_conversion)]
    Ok(FileStorageData {
      path: path.into(),
      size: metadata?.len().try_into().unwrap(),
      storage_uid: link_metadata.get_storage_uid(),
      file_id: link_metadata.get_file_id(),
    })
  }

  // pub async fn new_with_context(path: impl AsRef<Path>) -> Result<Self> {
  // Ok(Self::new(path.as_ref()).await.with_context(move || {
  // format!(
  // "Could not reat metadata for file {}",
  // path.as_ref().display()
  // )
  // })?)
  // }
}

static HASH_SEMAPHORE: OnceLock<Semaphore> = OnceLock::new();

pub fn get_file_hash_lock() -> &'static Semaphore {
  HASH_SEMAPHORE.get_or_init(|| Semaphore::new(DedupArgs::get().max_hash_threads))
}

pub async fn calculate_file_hash(
  path: impl AsRef<Path>,
  expected_size: Filesize,
) -> Result<HashDigest> {
  let lock = get_file_hash_lock().acquire().await?;
  let hash = {
    let mut hash = Box::new(Hasher::new());
    let mut file_length = 0;
    let mut reader = fs::OpenOptions::new()
      .create(false)
      .read(true)
      .open(path)
      .await?;
    let mut buffer_size = min(
      DedupArgs::get().buffer_size * 1024,
      expected_size.try_into().unwrap(),
    );
    let mut read_buf = Vec::new();
    loop {
      let (reserve_remaining, done) = buffer_size.overflowing_sub(read_buf.len());
      if done {
        break;
      }
      match read_buf.try_reserve_exact(reserve_remaining) {
        Ok(()) => break,
        Err(_) if buffer_size > 512 => {
          buffer_size >>= 1;
        }
        Err(error) => Err(error)?,
      }
    }
    unsafe {
      read_buf.set_len(read_buf.capacity());
    }
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
        "The entire file could not be hashed",
      ))?;
    }
    hash.finalize().into()
  };
  drop(lock);
  Ok(hash)
}

pub async fn calculate_file_hash_with_context(
  path: impl AsRef<Path>,
  expected_size: Filesize,
) -> Result<Option<HashDigest>> {
  let result = calculate_file_hash(path.as_ref(), expected_size)
    .await
    .with_context(move || format!("Could not hash file {}", path.as_ref().display()));
  match (result, DedupArgs::get().ignore_hash_errors) {
    (Ok(hash), _) => Ok(Some(hash)),
    (Err(err), true) => {
      let maybe_source = err.source().map(|x| format!("{}", x));
      let real_err = if let Some(ref source) = maybe_source {
        source
      } else {
        "unknown error"
      };
      eprintln!("{err} ({real_err})");
      Ok(None)
    }
    (Err(err), false) => Err(err),
  }
}
