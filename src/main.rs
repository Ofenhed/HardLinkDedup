use blake3::{Hasher, OUT_LEN as HASH_LEN};
use clap::{ArgAction, Parser};
use once_cell::sync::Lazy;
use regex::Regex;
use std::{
  cmp::max,
  collections::{hash_map::Entry, HashMap, HashSet},
  ffi::OsString,
  io::{Error, ErrorKind, Result},
  path::{Path, PathBuf},
};
use tokio::{fs, io::AsyncReadExt, join, spawn, sync::mpsc};

mod os;
use os::{FileBackend, FileId, FileLinkBackend, StorageUid};

type HashDigest = [u8; HASH_LEN];
type Filesize = u64;

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
struct DedupArgs {
  /// Regex pattern files must match to be included in the dedup.
  #[arg(short, long)]
  pattern: Option<Regex>,

  /// Don't actually do anything, just print what would have been done.
  #[arg(short, long, action = ArgAction::SetTrue)]
  dry_run: bool,

  /// File buffer size per file in KiB.
  #[arg(short, long, default_value = "16")]
  buffer_size: usize,

  /// The extension to apply to the hard link before it's renamed to the original filename.
  #[arg(short, long, default_value = "hard_link")]
  temporary_extension: OsString,

  /// By default, all hardlinked files will be set readonly (to avoid confusing file interactions).
  /// This flags makes it so that this program doesn't affect file permissions beyond the effect of
  /// hard linking the files.
  #[arg(short, long, action = ArgAction::SetTrue)]
  not_readonly: bool,

  /// Fully compare files before overwriting. This is a lot more expensive than checking hashes,
  /// which is the default behaviour.
  #[arg(short = 'f', long, action = ArgAction::SetTrue)]
  paranoid: bool,

  /// Paths where files will be deduplicated.
  #[arg(required = true, value_hint = clap::ValueHint::DirPath)]
  path: Vec<PathBuf>,
}

#[derive(Debug)]
struct SizedFile {
  path: PathBuf,
  size: Filesize,
}

#[derive(Debug)]
struct FileStorageData {
  path: PathBuf,
  size: Filesize,
  storage_uid: StorageUid,
  file_id: FileId,
}

impl FileStorageData {
  fn unique_id(&self) -> (StorageUid, FileId) {
    (self.storage_uid, self.file_id)
  }
}

#[derive(Debug)]
enum NewData {
  File(SizedFile),
  OpenFile(FileStorageData, fs::File),
  HashedFile(FileStorageData, HashDigest),
  Dir(PathBuf),
}

#[derive(Debug)]
struct NewDataHolder {
  data: NewData,
  sender: mpsc::Sender<NewDataHolder>,
}

static ARGS: Lazy<DedupArgs> = Lazy::new(|| DedupArgs::parse());

impl FileStorageData {
  async fn open(
    path: impl AsRef<Path>,
    file_metadata_found: mpsc::Sender<NewDataHolder>,
  ) -> Result<()> {
    let reader = fs::File::open(&path).await?;
    let link_metadata = match reader.link_metadata().await {
      Ok(x) => Ok(x),
      Err(e) => {
        println!("Error: {}", e);
        Err(e)
      }
    }?;
    let file_info = FileStorageData {
      path: path.as_ref().to_path_buf(),
      size: reader.metadata().await?.len().try_into().unwrap(),
      storage_uid: link_metadata.get_storage_uid(),
      file_id: link_metadata.get_file_id(),
    };
    file_metadata_found
      .send(NewDataHolder {
        data: NewData::OpenFile(file_info, reader),
        sender: file_metadata_found.clone(),
      })
      .await
      .unwrap();
    Ok(())
  }

  async fn calculate_file_hash(
    self,
    mut reader: fs::File,
    file_hash_found: mpsc::Sender<NewDataHolder>,
  ) -> Result<()> {
    let mut read_buf = vec![0; Lazy::force(&ARGS).buffer_size * 1024];
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
    if file_length != self.size as usize {
      return Err(Error::new(
        ErrorKind::BrokenPipe,
        format!(
          "The entire file {} could not be hashed",
          self.path.display()
        ),
      ))?;
    }
    file_hash_found
      .send(NewDataHolder {
        data: NewData::HashedFile(self, hash.finalize().into()),
        sender: file_hash_found.clone(),
      })
      .await
      .unwrap();
    Ok(())
  }
}

async fn scan_dir(dir: PathBuf, entry_found_tx: mpsc::Sender<NewDataHolder>) -> Result<()> {
  let mut reader = fs::read_dir(dir).await?;
  while let Some(entry) = reader.next_entry().await? {
    let metadata = entry.metadata().await?;
    if metadata.is_dir() {
      entry_found_tx
        .send(NewDataHolder {
          data: NewData::Dir(entry.path()),
          sender: entry_found_tx.clone(),
        })
        .await
        .unwrap();
    } else if metadata.is_file() {
      if let Some(ref pattern) = Lazy::force(&ARGS).pattern {
        if let Some(file_name) = entry.path().file_name().map(|name| name.to_string_lossy()) {
          if let Some(found) = pattern.find(&file_name) {
            if found.start() != 0 || found.end() != file_name.len() {
              continue;
            }
          } else {
            continue;
          }
        }
      }
      let size = metadata.len();
      let path = entry.path();
      if size > 0 {
        entry_found_tx
          .send(NewDataHolder {
            data: NewData::File(SizedFile { path, size }),
            sender: entry_found_tx.clone(),
          })
          .await
          .unwrap();
      }
    }
  }
  Ok(())
}

async fn compare_files(file1: impl AsRef<Path>, file2: impl AsRef<Path>) -> Result<bool> {
  let (file1, file2) = join!(fs::File::open(&file1), fs::File::open(&file2));
  let (mut file1, mut file2) = (file1?, file2?);
  let buffer_size = Lazy::force(&ARGS).buffer_size;
  let (mut buf_file1, mut buf_file2) = (vec![0; buffer_size * 1024], vec![0; buffer_size * 1024]);
  let (buf_file1, buf_file2) = (&mut buf_file1, &mut buf_file2);
  loop {
    let (file1_read, file2_read) = join!(
      file1.read(&mut buf_file1[..]),
      file2.read(&mut buf_file2[..])
    );
    match (file1_read?, file2_read?) {
      (0, 0) => {
        return Ok(true);
      }
      (mut file1_read, mut file2_read) => {
        while file1_read < file2_read {
          let new_read = file1.read(&mut buf_file1[file1_read..file2_read]).await?;
          if new_read == 0 {
            return Ok(false);
          }
          file1_read += new_read;
        }
        while file2_read < file1_read {
          let new_read = file2.read(&mut buf_file2[file2_read..file1_read]).await?;
          if new_read == 0 {
            return Ok(false);
          }
          file2_read += new_read;
        }
        if buf_file1[..file1_read] != buf_file2[..file2_read] {
          return Ok(false);
        }
      }
    }
  }
}

async fn merge_with_hard_link(file1: impl AsRef<Path>, file2: impl AsRef<Path>) -> Result<()> {
  let args = Lazy::force(&ARGS);
  let new_file = if let Some(new_file_name) = file2.as_ref().file_name() {
    let mut new_file_name = new_file_name.to_owned();
    new_file_name.push(".");
    new_file_name.push(&args.temporary_extension);
    file2.as_ref().with_file_name(new_file_name)
  } else {
    unreachable!()
  };

  if args.dry_run {
    println!(
      "Creating file {} linked with {}",
      new_file.display(),
      file1.as_ref().display()
    );
  } else {
    fs::hard_link(&file1, &new_file).await?;
  }
  if args.dry_run {
    println!(
      "Renaming file {} to {}",
      new_file.display(),
      file2.as_ref().display()
    );
  } else {
    if let Err(e) = fs::rename(&new_file, &file2).await {
      fs::remove_file(new_file).await?;
      return Err(e)?;
    }
  }
  if !args.not_readonly {
    if args.dry_run {
      if !fs::metadata(&file1).await?.permissions().readonly() {
        println!("Applying readonly to {} ", file1.as_ref().display());
      }
    } else {
      let mut permissions = fs::metadata(&file1).await?.permissions();
      if !permissions.readonly() {
        permissions.set_readonly(true);
        fs::set_permissions(&file1, permissions).await?;
      }
    }
  }
  Ok(())
}

enum FilesizeStatus {
  FoundOne(PathBuf),
  FoundMultiple,
}

#[tokio::main]
async fn main() {
  let args = Lazy::force(&ARGS);
  let (file_found_tx, mut file_found_rx) =
    mpsc::channel::<NewDataHolder>(max(100, args.path.len()));
  let (new_file_data_tx, mut new_file_data_rx) = mpsc::channel::<(FileStorageData, HashDigest)>(10);

  spawn(async move {
    for path in &args.path {
      spawn(scan_dir(path.to_owned(), file_found_tx.clone()));
    }
    drop(file_found_tx);
    let mut found_files = HashMap::<Filesize, FilesizeStatus>::new();
    let mut hash_requested = HashSet::<(StorageUid, FileId)>::new();
    while let Some(new_data) = file_found_rx.recv().await {
      match new_data {
        NewDataHolder {
          data: NewData::File(new_file),
          sender,
        } => match found_files.entry(new_file.size) {
          Entry::Occupied(mut entry) => match entry.get() {
            FilesizeStatus::FoundMultiple => {
              spawn(FileStorageData::open(new_file.path, sender));
            }
            FilesizeStatus::FoundOne(_) => {
              let other_file = if let FilesizeStatus::FoundOne(other_file) =
                entry.insert(FilesizeStatus::FoundMultiple)
              {
                other_file
              } else {
                unreachable!()
              };
              spawn(FileStorageData::open(new_file.path, sender.clone()));
              spawn(FileStorageData::open(other_file, sender));
            }
          },
          Entry::Vacant(entry) => {
            entry.insert(FilesizeStatus::FoundOne(new_file.path));
          }
        },
        NewDataHolder {
          data: NewData::OpenFile(file_info, file_reader),
          sender,
        } => {
          let key = file_info.unique_id();
          if hash_requested.insert(key) {
            spawn(file_info.calculate_file_hash(file_reader, sender));
          }
        }
        NewDataHolder {
          data: NewData::HashedFile(file_info, hash_digest),
          ..
        } => new_file_data_tx
          .send((file_info, hash_digest))
          .await
          .unwrap(),
        NewDataHolder {
          data: NewData::Dir(dir),
          sender,
        } => {
          spawn(scan_dir(dir, sender));
        }
      }
    }
  });

  let mut unique_files = HashMap::<(StorageUid, Filesize, HashDigest), (PathBuf, FileId)>::new();
  let mut wasted_space: Filesize = 0;

  while let Some((file_info, hash_digest)) = new_file_data_rx.recv().await {
    let identifier = (file_info.storage_uid, file_info.size, hash_digest);
    match unique_files.get_mut(&identifier) {
      Some((ref similar_file_path, ref similar_file_id)) => {
        if similar_file_id == &file_info.file_id {
          continue;
        }
        if args.paranoid {
          match compare_files(&similar_file_path, &file_info.path).await {
            Ok(false) => unreachable!(
              "Same hash for differing files {} and {}",
              similar_file_path.display(),
              file_info.path.display()
            ),
            Ok(true) => (),
            Err(e) => {
              eprintln!(
                "Failed to compare {} with {}: {}",
                similar_file_path.display(),
                file_info.path.display(),
                e
              );
            }
          }
        }
        match merge_with_hard_link(&similar_file_path, &file_info.path).await {
          Ok(()) => {
            wasted_space += file_info.size;
          }
          Err(e) => eprintln!(
            "Failed to merge {} with {}: {}",
            similar_file_path.display(),
            file_info.path.display(),
            e
          ),
        }
      }
      None => {
        unique_files.insert(identifier, (file_info.path, file_info.file_id));
      }
    };
  }

  println!(
    "A total of {} MiB {} saved",
    wasted_space / (1024 * 1024),
    if args.dry_run { "can be" } else { "was" }
  );
}
