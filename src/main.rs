use anyhow::{Context, Result};
use blake3::OUT_LEN as HASH_LEN;
use clap::{ArgAction, Parser};
use regex::Regex;
use std::{
  collections::{hash_map::Entry, HashMap, HashSet},
  ffi::OsString,
  path::{Path, PathBuf},
  sync::{Arc, OnceLock},
};
use tokio::{fs, task::JoinSet};

mod os;
mod storage;
use os::{FileId, StorageUid};
use storage::{calculate_file_hash_with_context, FileStorageData};

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
  #[arg(short, long, default_value = "2048")]
  buffer_size: usize,

  /// The extension to apply to the hard link before it's renamed to the original filename.
  #[arg(short, long, default_value = "hard_link")]
  temporary_extension: OsString,

  /// By default, all hardlinked files will be set readonly (to avoid confusing file interactions).
  /// This flags makes it so that this program doesn't affect file permissions beyond the effect of
  /// hard linking the files.
  #[arg(short, long, action = ArgAction::SetTrue)]
  not_readonly: bool,

  /// Keep going even if not all files can be read.
  #[arg(short, long, action = ArgAction::SetTrue)]
  ignore_scan_errors: bool,

  /// Paths where files will be deduplicated.
  #[arg(required = true, value_hint = clap::ValueHint::DirPath)]
  path: Vec<PathBuf>,
}

//#[derive(Debug)]
// struct SizedFile {
//  path: PathBuf,
//  size: Filesize,
//}

#[derive(Debug, Clone)]
enum ScanDirResult {
  Dir(PathBuf),
  File(FileStorageData),
}

static ARGS: OnceLock<DedupArgs> = OnceLock::new();

impl DedupArgs {
  pub fn get() -> &'static Self {
    ARGS.get_or_init(|| DedupArgs::parse())
  }
}

async fn scan_dir(dir: impl AsRef<Path>) -> Result<Arc<[ScanDirResult]>> {
  let mut reader = Box::new(fs::read_dir(dir).await?);
  let mut result = vec![];
  while let Some(entry) = reader.next_entry().await? {
    let path = entry.path();
    let metadata = fs::metadata(&path).await?;
    if metadata.is_dir() {
      result.push(ScanDirResult::Dir(entry.path()));
    } else if metadata.is_file() {
      if let Some(ref pattern) = DedupArgs::get().pattern {
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
      let file = FileStorageData::new(path).await?;
      if file.size > 0 {
        result.push(ScanDirResult::File(file));
      }
    }
  }
  Ok(result.into())
}

async fn scan_dir_with_context(dir: impl AsRef<Path>) -> Result<Arc<[ScanDirResult]>> {
  let result = scan_dir(dir.as_ref())
    .await
    .with_context(move || format!("Could not scan dir {}", dir.as_ref().display()));
  match (result, DedupArgs::get().ignore_scan_errors) {
    (result, false) => result,
    (Ok(result), true) => Ok(result),
    (Err(e), true) => {
      println!("{e}");
      Ok(Arc::new([]))
    }
  }
}

async fn merge_with_hard_link(
  original: impl AsRef<Path>,
  redundant: impl AsRef<Path>,
) -> Result<()> {
  let args = DedupArgs::get();
  let new_file = if let Some(new_file_name) = redundant.as_ref().file_name() {
    let mut new_file_name = new_file_name.to_owned();
    new_file_name.push(".");
    new_file_name.push(&args.temporary_extension);
    redundant.as_ref().with_file_name(new_file_name)
  } else {
    unreachable!()
  };

  if args.dry_run {
    println!(
      "Linking file {} to {}",
      redundant.as_ref().display(),
      original.as_ref().display()
    );
  } else {
    fs::hard_link(&original, &new_file).await?;
  }
  if !args.dry_run {
    if let Err(e) = fs::rename(&new_file, &redundant).await {
      fs::remove_file(new_file).await?;
      return Err(e)?;
    }
  }
  if !args.not_readonly {
    if args.dry_run {
      if !fs::metadata(&original).await?.permissions().readonly() {
        println!("Applying readonly to {} ", original.as_ref().display());
      }
    } else {
      let mut permissions = fs::metadata(&original).await?.permissions();
      if !permissions.readonly() {
        permissions.set_readonly(true);
        fs::set_permissions(&original, permissions).await?;
      }
    }
  }
  Ok(())
}

async fn merge_with_hard_link_with_context(
  original: impl AsRef<Path>,
  redundant: impl AsRef<Path>,
) -> Result<()> {
  Ok(
    merge_with_hard_link(original.as_ref(), redundant.as_ref())
      .await
      .with_context(move || {
        format!(
          "Could not merge hard link {} to {}",
          redundant.as_ref().display(),
          original.as_ref().display()
        )
      })?,
  )
}

#[tokio::main]
async fn main() -> Result<()> {
  let args = DedupArgs::get();

  enum WorkerResult {
    ScanResult(Arc<[ScanDirResult]>),
    NewHashReceived(StorageUid, FileId, (Filesize, HashDigest)),
  }
  let mut worker = JoinSet::<Result<WorkerResult>>::new();

  for path in &args.path {
    worker.spawn(async {
      Ok(WorkerResult::ScanResult(
        scan_dir_with_context(path.to_owned()).await?,
      ))
    });
  }

  #[derive(Debug, Default)]
  struct StorageContent {
    file_sizes: HashMap<Filesize, HashSet<FileId>>,
    hashes: HashMap<(Filesize, HashDigest), HashSet<FileId>>,
    files: HashMap<FileId, (Arc<Path>, Vec<Arc<Path>>)>,
  }
  let mut known_files = HashMap::<StorageUid, StorageContent>::new();
  while let Some(found_files) = worker.join_next().await {
    match found_files?? {
      WorkerResult::ScanResult(files) => {
        for file in files.into_iter().map(ToOwned::to_owned) {
          match file {
            ScanDirResult::Dir(path) => {
              worker.spawn(async move {
                Ok(WorkerResult::ScanResult(scan_dir_with_context(path).await?))
              });
            }
            ScanDirResult::File(storage_data) => {
              let storage = known_files.entry(storage_data.storage_uid).or_default();
              match storage.files.entry(storage_data.file_id) {
                Entry::Occupied(mut entry) => {
                  entry.get_mut().1.push(storage_data.path.clone());
                }
                Entry::Vacant(entry) => {
                  entry.insert((storage_data.path.to_owned(), vec![]));
                  match storage.file_sizes.entry(storage_data.size) {
                    Entry::Occupied(mut entry) => {
                      let other_files = entry.get_mut();
                      let mut files_iter = other_files.iter();
                      let first_file =
                        if let (Some(file), None) = (files_iter.next(), files_iter.next()) {
                          Some(file.to_owned())
                        } else {
                          None
                        };
                      other_files.insert(storage_data.file_id);
                      if let Some(first_file_id) = first_file {
                        let first_file_path = storage
                          .files
                          .get(&first_file_id)
                          .expect("This file id has to exist")
                          .0
                          .clone();
                        let storage_uid = storage_data.storage_uid;
                        let file_size = storage_data.size;
                        worker.spawn(async move {
                          Ok(WorkerResult::NewHashReceived(
                            storage_uid,
                            first_file_id,
                            (
                              file_size,
                              calculate_file_hash_with_context(first_file_path, file_size).await?,
                            ),
                          ))
                        });
                      }
                      worker.spawn(async move {
                        Ok(WorkerResult::NewHashReceived(
                          storage_data.storage_uid,
                          storage_data.file_id,
                          (
                            storage_data.size,
                            calculate_file_hash_with_context(
                              storage_data.path.clone(),
                              storage_data.size,
                            )
                            .await?,
                          ),
                        ))
                      });
                    }
                    entry @ Entry::Vacant(..) => {
                      entry.or_default().insert(storage_data.file_id);
                    }
                  }
                }
              }
            }
          }
        }
      }
      WorkerResult::NewHashReceived(storage_uid, file_id, digest) => {
        let Entry::Occupied(mut storage) = known_files.entry(storage_uid) else { unreachable!("Always set by this point") };
        storage
          .get_mut()
          .hashes
          .entry(digest)
          .or_default()
          .insert(file_id);
      }
    }
  }

  let mut wasted_space: Filesize = 0;
  for storage in known_files.values() {
    for ((file_size, _hash), duplicates) in storage.hashes.iter().filter(|(_, x)| x.len() > 1) {
      let mut files = duplicates
        .iter()
        .flat_map(|x| {
          let (first, rest) = storage.files.get(x).expect("File id will exist here");
          let mut result = rest.to_owned();
          result.push(first.to_owned());
          result
        })
        .collect::<Vec<_>>();
      if let Some(original_file) = files.pop() {
        for duplicate in files {
          merge_with_hard_link_with_context(&original_file, duplicate).await?;
          wasted_space += file_size;
        }
      }
    }
  }

  println!(
    "A total of {} MiB {} saved",
    wasted_space / (1024 * 1024),
    if args.dry_run { "can be" } else { "was" }
  );
  Ok(())
}
