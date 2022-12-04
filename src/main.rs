use clap::{ArgAction, Parser};
use once_cell::sync::Lazy;
use regex::Regex;
use std::{
  cmp::max,
  collections::HashMap,
  ffi::OsString,
  io::{Error, ErrorKind, Result},
  path::{Path, PathBuf},
};
use tokio::{fs, io::AsyncReadExt, spawn, sync::mpsc};
use xxhash_rust::xxh3::Xxh3;

mod os;
use os::{FileBackend, FileId, FileLinkBackend, StorageUid};

type HashDigest = u128;
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

  /// The extension to apply to the hard link before it's renamed to the original filename.
  #[arg(short, long, default_value = "hard_link")]
  temporary_extension: OsString,

  /// By default, all hardlinked files will be set readonly (to avoid confusing file interactions).
  /// This flags makes it so that this program doesn't affect file permissions beyond the effect of
  /// hard linking the files.
  #[arg(short, long, action = ArgAction::SetTrue)]
  not_readonly: bool,

  /// Paths where files will be deduplicated.
  #[arg(required = true, value_hint = clap::ValueHint::DirPath)]
  path: Vec<PathBuf>,
}

#[derive(Debug)]
struct FileWithMetadata {
  path: PathBuf,
  size: Filesize,
}

#[derive(Debug)]
enum HashValue {
  Incomplete(fs::File),
  Hash(HashDigest),
}

#[derive(Debug)]
struct FileContentInfo {
  path: PathBuf,
  size: Filesize,
  hash: HashValue,
  storage_uid: StorageUid,
  file_id: FileId,
}

#[derive(Debug)]
enum NewData {
  File(FileWithMetadata),
  FileContentInfo(FileContentInfo),
  Dir(PathBuf),
}

#[derive(Debug)]
struct NewDataHolder {
  data: NewData,
  sender: mpsc::Sender<NewDataHolder>,
}

static ARGS: Lazy<DedupArgs> = Lazy::new(|| DedupArgs::parse());

impl FileContentInfo {
  async fn create(
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
    let file_info = FileContentInfo {
      path: path.as_ref().to_path_buf(),
      size: reader.metadata().await?.len().try_into().unwrap(),
      hash: HashValue::Incomplete(reader),
      storage_uid: link_metadata.get_storage_uid(),
      file_id: link_metadata.get_file_id(),
    };
    file_metadata_found
      .send(NewDataHolder {
        data: NewData::FileContentInfo(file_info),
        sender: file_metadata_found.clone(),
      })
      .await
      .unwrap();
    Ok(())
  }

  async fn calculate_file_hash(self, file_hash_found: mpsc::Sender<NewDataHolder>) -> Result<()> {
    if let HashValue::Incomplete(mut reader) = self.hash {
      let mut read_buf = vec![0; 16 * 1024];
      let mut hash = Xxh3::new();
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
          data: NewData::FileContentInfo(Self {
            hash: HashValue::Hash(hash.digest128()),
            ..self
          }),
          sender: file_hash_found.clone(),
        })
        .await
        .unwrap();
    }
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
            data: NewData::File(FileWithMetadata { path, size }),
            sender: entry_found_tx.clone(),
          })
          .await
          .unwrap();
      }
    }
  }
  Ok(())
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
  let (new_file_data_tx, mut new_file_data_rx) = mpsc::channel::<FileContentInfo>(10);

  spawn(async move {
    for path in &args.path {
      spawn(scan_dir(path.to_owned(), file_found_tx.clone()));
    }
    drop(file_found_tx);
    let mut found_files = HashMap::<Filesize, FilesizeStatus>::new();
    let mut file_id_hashes = HashMap::<(StorageUid, FileId), HashDigest>::new();
    while let Some(new_data) = file_found_rx.recv().await {
      match new_data {
        NewDataHolder {
          data: NewData::File(new_file),
          sender,
        } => match found_files.insert(new_file.size, FilesizeStatus::FoundOne(new_file.path)) {
          None => (),
          Some(FilesizeStatus::FoundOne(other_file)) => {
            spawn(FileContentInfo::create(other_file, sender.clone()));
            let new_file = if let FilesizeStatus::FoundOne(new_file) = found_files
              .insert(new_file.size, FilesizeStatus::FoundMultiple)
              .unwrap()
            {
              new_file
            } else {
              unreachable!()
            };
            spawn(FileContentInfo::create(new_file, sender.clone()));
          }
          Some(FilesizeStatus::FoundMultiple) => {
            let new_file = if let FilesizeStatus::FoundOne(new_file) = found_files
              .insert(new_file.size, FilesizeStatus::FoundMultiple)
              .unwrap()
            {
              new_file
            } else {
              unreachable!()
            };
            spawn(FileContentInfo::create(new_file, sender.clone()));
          }
        },
        NewDataHolder {
          data: NewData::FileContentInfo(file_info),
          sender,
        } => {
          let hash_identifier = (file_info.storage_uid, file_info.file_id);
          match file_info.hash {
            HashValue::Incomplete(..) => {
              if file_id_hashes.get(&hash_identifier).is_none() {
                spawn(file_info.calculate_file_hash(sender));
              }
            }
            HashValue::Hash(hash_digest) => {
              file_id_hashes.insert(hash_identifier, hash_digest);
              new_file_data_tx.send(file_info).await.unwrap()
            }
          }
        }
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

  while let Some(file_info) = new_file_data_rx.recv().await {
    let hash_digest = if let HashValue::Hash(hash_digest) = file_info.hash {
      hash_digest
    } else {
      unreachable!()
    };
    let identifier = (file_info.storage_uid, file_info.size, hash_digest);
    match unique_files.get_mut(&identifier) {
      Some((ref similar_file_path, ref similar_file_id)) => {
        if similar_file_id == &file_info.file_id {
          continue;
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
