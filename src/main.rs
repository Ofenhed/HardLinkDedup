use clap::{ArgAction, Parser};
use once_cell::sync::Lazy;
use regex::Regex;
use std::{
  cmp::max,
  collections::HashMap,
  ffi::OsString,
  io::Result,
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

type UniqueFileId = (StorageUid, Filesize, HashDigest);

#[derive(Debug)]
struct FileContentInfo {
  path: PathBuf,
  size: Filesize,
  hash: HashDigest,
  storage_uid: StorageUid,
  file_id: FileId,
}

impl FileContentInfo {
  fn get_identifier(&self) -> UniqueFileId {
    (self.storage_uid, self.size, self.hash)
  }
}

#[derive(Debug)]
enum NewData {
  File(FileWithMetadata),
  Dir(ScanDirJob),
}

static ARGS: Lazy<DedupArgs> = Lazy::new(|| DedupArgs::parse());

#[derive(Debug)]
struct ScanDirJob {
  dir: PathBuf,
  file_found_tx: mpsc::Sender<NewData>,
}

async fn scan_dir(what: ScanDirJob) -> Result<()> {
  let ScanDirJob {
    ref dir,
    ref file_found_tx,
  } = what;
  let mut reader = fs::read_dir(dir).await?;
  while let Some(entry) = reader.next_entry().await? {
    let metadata = entry.metadata().await?;
    if metadata.is_dir() {
      let job = ScanDirJob {
        dir: entry.path(),
        file_found_tx: file_found_tx.clone(),
      };
      file_found_tx.send(NewData::Dir(job)).await.unwrap();
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
        file_found_tx
          .send(NewData::File(FileWithMetadata { path, size }))
          .await
          .unwrap();
      }
    }
  }
  Ok(())
}

async fn calculate_file_hash(
  file: impl AsRef<Path>,
  hash_calculated_tx: mpsc::Sender<FileContentInfo>,
) -> Result<()> {
  let mut read_buf = vec![0; 16 * 1024];
  let mut reader = fs::File::open(&file).await?;
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
  let link_metadata = match reader.link_metadata().await {
    Ok(x) => Ok(x),
    Err(e) => {
      println!("Error: {}", e);
      Err(e)
    }
  }?;
  let file_info = FileContentInfo {
    path: file.as_ref().to_path_buf(),
    size: file_length.try_into().unwrap(),
    hash: hash.digest128(),
    storage_uid: link_metadata.get_storage_uid(),
    file_id: link_metadata.get_file_id(),
  };
  hash_calculated_tx.send(file_info).await.unwrap();
  Ok(())
}

async fn merge_with_hard_link(file1: impl AsRef<Path>, file2: impl AsRef<Path>) -> Result<()> {
  let args = Lazy::force(&ARGS);
  let new_file = file2.as_ref().with_extension(&args.temporary_extension);
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
    fs::rename(new_file, &file2).await?;
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
  let (file_found_tx, mut file_found_rx) = mpsc::channel::<NewData>(max(100, args.path.len()));
  let (new_file_hash_tx, mut new_file_hash_rx) = mpsc::channel::<FileContentInfo>(10);

  spawn(async move {
    for path in &args.path {
      let job = ScanDirJob {
        dir: path.to_owned(),
        file_found_tx: file_found_tx.clone(),
      };
      spawn(scan_dir(job));
    }
    drop(file_found_tx);
    let mut found_files = HashMap::<Filesize, FilesizeStatus>::new();
    while let Some(new_data) = file_found_rx.recv().await {
      match new_data {
        NewData::File(new_file) => {
          match found_files.insert(new_file.size, FilesizeStatus::FoundOne(new_file.path)) {
            None => (),
            Some(FilesizeStatus::FoundOne(other_file)) => {
              spawn(calculate_file_hash(other_file, new_file_hash_tx.clone()));
              let new_file = if let FilesizeStatus::FoundOne(new_file) = found_files
                .insert(new_file.size, FilesizeStatus::FoundMultiple)
                .unwrap()
              {
                new_file
              } else {
                unreachable!()
              };
              spawn(calculate_file_hash(new_file, new_file_hash_tx.clone()));
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
              spawn(calculate_file_hash(new_file, new_file_hash_tx.clone()));
            }
          }
        }
        NewData::Dir(job) => {
          spawn(scan_dir(job));
        }
      }
    }
  });

  let mut unique_files = HashMap::<UniqueFileId, (PathBuf, FileId)>::new();
  let mut wasted_space: Filesize = 0;

  while let Some(file_info) = new_file_hash_rx.recv().await {
    let identifier = file_info.get_identifier();
    match unique_files.get_mut(&identifier) {
      Some((ref similar_file_path, ref similar_file_id)) => {
        if similar_file_id == &file_info.file_id {
          continue;
        }
        wasted_space += file_info.size;
        match merge_with_hard_link(&similar_file_path, &file_info.path).await {
          Ok(()) => {}
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
    }
  }

  println!(
    "A total of {} MiB can be saved",
    wasted_space / (1024 * 1024)
  );
}
