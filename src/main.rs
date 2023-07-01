#![cfg_attr(all(windows, not(feature = "stable")), feature(windows_by_handle))]
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

  /// Ignore files smaller than this (in KiB).
  #[arg(short, long, default_value = "1024")]
  min_file_size: Filesize,

  /// File buffer size per file (in KiB).
  #[arg(short, long, default_value = "2048")]
  buffer_size: usize,

  /// Max threads allowed to hash files at the same time. This in combination with limiting the
  /// buffer size can be used to limit memory usage.
  #[arg(short, long, default_value = "10")]
  max_hash_threads: usize,

  /// The extension to apply to the hard link before it's renamed to the original filename.
  #[arg(short, long, default_value = "hard_link")]
  temporary_extension: OsString,

  /// By default, all hardlinked files will be set readonly (to avoid confusing file interactions).
  /// This flags makes it so that this program doesn't affect file permissions beyond the effect of
  /// hard linking the files.
  #[arg(short, long, action = ArgAction::SetTrue)]
  not_readonly: bool,

  /// Keep going even if not all file's metadata can be read.
  #[arg(long, action = ArgAction::SetTrue)]
  ignore_scan_errors: bool,

  /// Keep going even if not all files can be read.
  #[arg(long, action = ArgAction::SetTrue)]
  ignore_hash_errors: bool,

  /// Print debug information about file IDs.
  #[arg(long, action = ArgAction::SetTrue)]
  debug: bool,

  /// Paths where files will be deduplicated.
  #[arg(required = true, value_hint = clap::ValueHint::DirPath)]
  path: Vec<PathBuf>,
}

#[derive(Debug, Clone)]
enum ScanDirResult {
  Dir(Arc<Path>),
  File(FileStorageData),
}

static ARGS: OnceLock<DedupArgs> = OnceLock::new();

impl DedupArgs {
  pub fn get() -> &'static Self {
    ARGS.get_or_init(DedupArgs::parse)
  }
}

async fn scan_dir(dir: impl AsRef<Path>) -> Result<Arc<[ScanDirResult]>> {
  let mut reader = Box::new(fs::read_dir(dir).await?);
  let mut result = vec![];
  let args = DedupArgs::get();
  while let Some(entry) = reader.next_entry().await? {
    let path = entry.path();
    let metadata = fs::symlink_metadata(&path).await?;
    if metadata.is_symlink() {
      continue;
    } else if metadata.is_dir() {
      result.push(ScanDirResult::Dir(entry.path().into()));
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
      if file.size != 0 && file.size >= args.min_file_size * 1024 {
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
      eprintln!("{e}");
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

  let sign = if args.dry_run { '↫' } else { '⇐' };
  println!(
    "{original} {sign} {redundant}",
    original = original.as_ref().display(),
    redundant = redundant.as_ref().display()
  );
  if !args.dry_run {
    fs::hard_link(&original, &new_file).await?;
  }
  if !args.dry_run {
    let mut redundant_permissions = fs::metadata(&redundant).await?.permissions();
    if redundant_permissions.readonly() {
      redundant_permissions.set_readonly(false);
      fs::set_permissions(&redundant, redundant_permissions).await?;
    }
    if let Err(e) = fs::rename(&new_file, &redundant).await {
      fs::remove_file(new_file).await?;
      return Err(e)?;
    }
  }
  if !args.not_readonly {
    let metadata_original = fs::metadata(&original).await?;
    if args.dry_run {
      if !metadata_original.permissions().readonly() {
        println!("Applying readonly to {} ", original.as_ref().display());
      }
    } else {
      let mut permissions = metadata_original.permissions();
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
  merge_with_hard_link(original.as_ref(), redundant.as_ref())
    .await
    .with_context(move || {
      format!(
        "Could not merge hard link {} to {}",
        redundant.as_ref().display(),
        original.as_ref().display()
      )
    })
}

#[derive(Debug)]
enum FileEntry {
  OriginalFile(Arc<Path>),
  Files(Arc<Path>, HashSet<Arc<Path>>),
  LinkTo(FileId),
}

#[derive(Debug, Default)]
struct StorageContent {
  file_sizes: HashMap<Filesize, Option<FileId>>,
  hashes: HashMap<(Filesize, HashDigest), FileId>,
  files: HashMap<FileId, FileEntry>,
}

#[tokio::main]
async fn main() -> Result<()> {
  let args = DedupArgs::get();

  enum WorkerResult {
    ScanResult(Arc<[ScanDirResult]>),
    NewHashReceived(StorageUid, FileId, (Filesize, Option<HashDigest>)),
  }
  let mut worker = JoinSet::<Result<WorkerResult>>::new();

  for path in &args.path {
    worker.spawn(async {
      Ok(WorkerResult::ScanResult(
        scan_dir_with_context(path.to_owned()).await?,
      ))
    });
  }

  let mut known_files = HashMap::<StorageUid, StorageContent>::new();
  let mut wasted_space = 0;
  while let Some(found_files) = worker.join_next().await {
    match found_files?? {
      WorkerResult::ScanResult(files) => {
        for file in files.iter().map(ToOwned::to_owned) {
          match file {
            ScanDirResult::Dir(path) => {
              worker.spawn(async move {
                Ok(WorkerResult::ScanResult(scan_dir_with_context(path).await?))
              });
            }
            ScanDirResult::File(storage_data) => {
              let storage = known_files.entry(storage_data.storage_uid).or_default();
              match storage.files.entry(storage_data.file_id) {
                Entry::Occupied(current_file_entry) => {
                  let mut id = storage_data.file_id;
                  let mut current_entry = current_file_entry;
                  loop {
                    let make_link = id != storage_data.file_id;
                    match current_entry.get_mut() {
                      FileEntry::LinkTo(ref file_id) if file_id == &storage_data.file_id => {
                        unreachable!("File links will never loop")
                      }
                      FileEntry::LinkTo(ref file_id) => {
                        id = *file_id;
                      }
                      FileEntry::OriginalFile(ref target_file) => {
                        if make_link {
                          merge_with_hard_link_with_context(target_file, &storage_data.path)
                            .await?;
                        }
                        break;
                      }
                      FileEntry::Files(_, ref mut links) if !make_link => {
                        links.insert(storage_data.path);
                        break;
                      }
                      FileEntry::Files(..) => {
                        unreachable!("Tried to create link to non-original file");
                      }
                    }
                    let Entry::Occupied(new_entry) = storage.files.entry(id) else {
                          unreachable!("Files will never point to invalid file id's")
                      };
                    current_entry = new_entry;
                  }
                }
                Entry::Vacant(entry) => {
                  entry.insert(FileEntry::Files(
                    storage_data.path.to_owned(),
                    Default::default(),
                  ));
                  match storage.file_sizes.entry(storage_data.size) {
                    Entry::Occupied(mut entry) => {
                      match entry.get_mut() {
                        old_value @ Some(_) => {
                          let first_file_id = old_value.unwrap();
                          if let FileEntry::Files(first_file_path, _) = storage
                            .files
                            .get(&first_file_id)
                            .expect("This file id has to exist")
                          {
                            let storage_uid = storage_data.storage_uid;
                            let file_size = storage_data.size;
                            let first_file_path = first_file_path.clone();
                            worker.spawn(async move {
                              Ok(WorkerResult::NewHashReceived(
                                storage_uid,
                                first_file_id,
                                (
                                  file_size,
                                  calculate_file_hash_with_context(first_file_path, file_size)
                                    .await?,
                                ),
                              ))
                            });
                          }
                          *old_value = None;
                        }
                        None => (),
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
                    Entry::Vacant(entry) => {
                      entry.insert(Some(storage_data.file_id));
                    }
                  }
                }
              }
            }
          }
        }
      }
      WorkerResult::NewHashReceived(storage_uid, file_id, (file_size, Some(digest))) => {
        let storage = known_files
          .get_mut(&storage_uid)
          .expect("Always set by this point");
        match storage.hashes.entry((file_size, digest)) {
          Entry::Vacant(entry) => {
            entry.insert(file_id);
            let Some(FileEntry::Files(original, _)) = storage.files.remove(&file_id) else {
                unreachable!("Got vacant hash of invalid file id");
            };
            storage
              .files
              .insert(file_id, FileEntry::OriginalFile(original));
          }
          Entry::Occupied(hash_entry) => {
            let original_id = hash_entry.get();
            let FileEntry::Files(new_file, mut new_links) = storage.files.insert(file_id, FileEntry::LinkTo(*original_id)).expect("Only known file IDs are hashed") else {
                      unreachable!("Only files are hashed, and only once")
                  };
            let FileEntry::OriginalFile(ref original_file) = storage.files.get_mut(original_id).expect("Only known file IDs are stored as hash targets") else {
                      unreachable!("Hash targets are never converted to links")
                  };
            wasted_space += file_size;
            new_links.insert(new_file);
            for new_file in new_links.into_iter() {
              merge_with_hard_link_with_context(original_file, &new_file).await?;
            }
          }
        }
      }
      WorkerResult::NewHashReceived(_, _, (_, None)) => (),
    }
  }

  if args.debug {
    let debug = known_files
      .into_values()
      .flat_map(|x| x.files)
      .collect::<HashMap<_, _>>();
    println!("{debug:#?}");
  }

  println!(
    "A total of {} MiB {} saved",
    wasted_space / (1024 * 1024),
    if args.dry_run { "can be" } else { "was" }
  );
  Ok(())
}
