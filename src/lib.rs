#![recursion_limit = "1024"]
#![feature(test)]
extern crate byteorder;
extern crate bytes;
extern crate crc;
extern crate num;
extern crate protobuf;
extern crate rand;
extern crate test;

#[macro_use]
extern crate log;
extern crate lazy_static;
extern crate regex;
extern crate simplelog;
#[macro_use]
extern crate error_chain;

mod bitrust_pb;
mod common;
mod config;
mod errors;
mod keydir;
mod lockfile;
mod locking;
mod storage;
pub mod util;

use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use protobuf::Message;

pub use common::{BitrustOperation, FileID, BITRUST_TOMBSTONE_STR};
pub use config::*;
pub use errors::*;

use keydir::{KeyDir, KeyDirEntry};
use storage::{RecordAppender, RecordReader};

macro_rules! debug_timeit {
  ( $name:expr => $b:block ) => {{
    let start = Instant::now();
    let ret = $b;
    let end = Instant::now();
    let dur = end - start;
    let ns = dur.as_secs() * 1_000_000_000 + dur.subsec_nanos() as u64;
    debug!("{} took {}ns", $name, ns);
    ret
  }};
}

// struct ActiveFile is just a collection of things related to the active file
// -- the current log of writes. We hold two file handles to this file, one for
// writing, which happens only by appending, and one handle for reading, which
// we can seek freely.
#[derive(Debug)]
struct ActiveFile {
  write_handle: File,
  read_handle: BufReader<File>,
  pub name: PathBuf,
  pub id: FileID,
}

impl ActiveFile {
  fn new(path: PathBuf) -> io::Result<ActiveFile> {
    let mut write_handle = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .open(&path)?;
    write_handle.seek(SeekFrom::End(0))?;
    let active_file = ActiveFile {
      write_handle: write_handle,
      read_handle: BufReader::new(
        OpenOptions::new().read(true).create(false).open(&path)?,
      ),
      id: util::file_id_from_path(&path),
      name: path,
    };
    debug!("Initialized active file");
    Ok(active_file)
  }
}

impl RecordReader for ActiveFile {
  type Message = bitrust_pb::BitRustDataRecord;
  type Reader = BufReader<File>;
  fn reader<'a>(&'a mut self) -> Result<&'a mut BufReader<File>> {
    Ok(&mut self.read_handle)
  }
}

impl RecordAppender for ActiveFile {
  type Message = bitrust_pb::BitRustDataRecord;
  type Writer = File;
  fn writer<'a>(&'a mut self) -> Result<&'a mut File> {
    Ok(&mut self.write_handle)
  }
}

#[derive(Debug)]
struct InactiveFile {
  read_handle: BufReader<File>,
  pub name: PathBuf,
  pub id: FileID,
}

impl InactiveFile {
  fn new(path: PathBuf) -> io::Result<InactiveFile> {
    let id = util::file_id_from_path(&path);
    Ok(InactiveFile {
      read_handle: BufReader::new(
        OpenOptions::new().read(true).create(false).open(&path)?,
      ),
      name: path,
      id: id,
    })
  }
}

impl RecordReader for InactiveFile {
  type Message = bitrust_pb::BitRustDataRecord;
  type Reader = BufReader<File>;
  fn reader<'a>(&'a mut self) -> Result<&'a mut BufReader<File>> {
    Ok(&mut self.read_handle)
  }
}

impl Into<InactiveFile> for ActiveFile {
  fn into(self) -> InactiveFile {
    InactiveFile {
      read_handle: self.read_handle,
      name: self.name,
      id: self.id,
    }
  }
}

#[derive(Debug)]
pub struct BitRustState<ClockT> {
  keydir: KeyDir,
  active_file: ActiveFile,

  inactive_files: HashMap<FileID, InactiveFile>,

  lockfile: lockfile::LockFile,

  // mutex for writers. There is an additional R/W lock at the keydir level
  // (see keydir::KeyDir) which takes care of concurrent updates of the
  // keydir. This lock is to synchronize writes since they involve writing
  // to data files (as opposed to reads, which read immutable records from
  // those files).
  mutex: Arc<Mutex<()>>,

  config: Config,

  // Apart from the initialization code, this is used by put() to overflow
  // data files (i.e., to start a new active file when the current one
  // becomes too big), and by merge(), to get a name for the merge output
  // files, which will replace all older inactive data files after the merge.
  //
  // When a write and a merge are happening concurrently, access to this
  // needs to be synchronized using `mutex` above.
  data_file_id_gen: util::FileIDGen,

  clock: Arc<ClockT>,
}

impl<ClockT> BitRustState<ClockT>
where
  ClockT: util::LogicalClock,
{
  pub fn new(
    config: Config,
    mut clock: ClockT,
  ) -> Result<BitRustState<ClockT>> {
    info!("Making a bitrust");
    fs::create_dir_all(config.datadir()).chain_err(|| {
      format!(
        "Failed to ensure that datadir is created at {:?}\
         (permission issues?)",
        &config.datadir()
      )
    })?;

    let data_dir = config.datadir().to_path_buf();

    // If the following fails, all it means is that we have a file at our
    // lockfile location that we did not create. We don't care at this
    // point why it exists, and just bail out.
    //
    // This lock will be released (and the lockfile removed) when the
    // returned object goes out of scope. Since we move it into the
    // returned BitRustState, this means the lock lives as long as the returned
    // BitRustState lives.
    let lockfile = locking::acquire(&data_dir, BitrustOperation::Write)
      .chain_err(|| {
        format!("Failed to obtain write lock in {:?}", &data_dir)
      })?;

    debug!("Obtained data directory lock");

    // Get the names of the data and hint files. Because these are named
    // like ${file_id}.data and ${file_id}.hint respectively, we can group
    // together the data and hint files for a given file_id together. A
    // hint file is optional, but cannot exist without a corresponding data
    // file (because hint files just contain pointers into the respective
    // data files)
    let dd_contents =
      util::read_data_dir_contents(&data_dir).chain_err(|| {
        format!(
          "Failed to enumerate contents of the data dir\
           {:?} when starting up bitrust",
          &data_dir
        )
      })?;

    let (latest_timestamp, keydir) = if dd_contents.len() == 0 {
      // We are starting from scratch, write the name of the active file
      // as 0.data.
      let ptr_path = active_file_pointer_path(&data_dir);
      debug!(
        "Starting in an empty data directory, writing {:?}",
        &ptr_path
      );
      util::write_to_file(
        &ptr_path,
        data_dir
          .join("0.data")
          .to_str()
          .expect("Garbled initial data file name?"),
      )
      .chain_err(|| {
        format!(
          "Failed to write active file name to the\
           pointer file {:?}",
          &ptr_path
        )
      })?;

      (0u64, KeyDir::new())
    } else {
      debug!("Now building keydir with these files: {:?}", &dd_contents);
      // We should probably build the keydir and the `active_file` and
      // `inactive_file` fields together, but it is just simpler to first
      // build the keydir and then do another pass to build the other fields.
      build_keydir(&dd_contents)?
    };
    debug!("Done building keydir: {:?}", &keydir.entries);

    let active_file_name = active_file_path(&data_dir)?;
    debug!("Using active file {:?}", &active_file_name);
    let active_file =
      ActiveFile::new(active_file_name.clone()).chain_err(|| {
        format!("Could not open active file {:?}", &active_file_name)
      })?;
    let mut inactive_files = HashMap::new();
    for (file_id, dd_entry) in dd_contents {
      if file_id != active_file.id {
        let inactive_file = InactiveFile::new(
          dd_entry.data_file_path().clone(),
        )
        .chain_err(|| {
          format!(
            "Failed to open inactive file at {:?} for\
               reading into keydir",
            dd_entry.data_file_path()
          )
        })?;
        inactive_files.insert(file_id, inactive_file);
      }
    }
    let active_file_id = active_file.id;
    clock.set_next(latest_timestamp + 1);
    let bitrust = BitRustState {
      keydir,
      config: config,
      inactive_files: inactive_files,
      active_file: active_file,
      lockfile,
      mutex: Arc::new(Mutex::new(())),
      data_file_id_gen: util::FileIDGen::new(active_file_id + 1),
      clock: Arc::new(clock),
    };
    debug!("Returning from BitRustState::new");
    Ok(bitrust)
  }

  /// Returns a vector of `(file_id, path_to_file)` tuples, sorted ascending
  /// by `file_id`.
  fn get_files_for_merging(&self) -> Vec<(FileID, PathBuf)> {
    let datadir = self.config.datadir();
    let mut files_to_merge: Vec<(FileID, PathBuf)> = self
      .inactive_files
      .keys()
      .cloned()
      .map(|id| (id, data_file_name_from_id(id, &datadir)))
      .collect();

    // Sort the above tuples by file id
    files_to_merge.sort_by(|a, b| a.0.cmp(&b.0));
    files_to_merge
  }

  // XXX: Partial merges, i.e., when we operate only on a subset of the
  // datafiles, is not implemented yet. It is important because if the
  // merge process gets a random error from the OS when opening/reading
  // one of the data files, the merge can still continue with the other
  // files, degrading into a partial merge. It differs from a total merge
  // mostly in the handling of tombstone values. In a partial merge, when
  // a key is seen with a tombstone, one can not just drop it, as there
  // might be a newer file that contains a non-tombstone value for the
  // key.
  pub fn merge(&mut self) -> Result<()> {
    // Try to get a merge-lock on the active directory.
    // Go through the files in ascending order of ids
    let _merge_lockfile =
      locking::acquire(&self.config.datadir(), BitrustOperation::Merge)
        .chain_err(|| {
          format!(
            "Failed to acquire merge lock in {:?}",
            &self.config.datadir()
          )
        })?;

    debug!("Acquired merge lockfile");

    // Open all the mergefiles and fail early if we cannot open any.
    // This is where a partial merge would have proceeded with the files we
    // could open (see the note above).

    let merge_files: Vec<InactiveFile> = {
      let files_to_merge = self.get_files_for_merging();
      debug!("Going to merge these files: {:?}", &files_to_merge);

      let mut merge_files = Vec::new();
      for (id, file_path) in files_to_merge.into_iter() {
        // TODO: partial merges even if we fail to open some files.
        let merge_file =
          InactiveFile::new(file_path.clone()).chain_err(|| {
            format!(
              "Failed to open inactive file id {} ({:?}) for merging",
              id, &file_path
            )
          })?;
        merge_files.push(merge_file);
      }
      merge_files
    };

    let datadir = self.config.datadir();

    let mut merge_output_file = {
      let mutex = self.mutex.clone();
      let merge_output_file_id = {
        let _lock = mutex.lock().unwrap();
        self.data_file_id_gen.take_next()
      };
      let merge_output_file_name =
        data_file_name_from_id(merge_output_file_id, &datadir);

      let merge_active_file =
        ActiveFile::new(merge_output_file_name.clone())
          .chain_err(|| "Failed to open a new merge file for writing")?;

      // We will also open an InactiveFile and make it known to the bitrust
      // state, since we will incrementally "port" keys from their current
      // files to the new merge file.
      let merge_inactive_file = InactiveFile::new(merge_output_file_name)
        .chain_err(|| "Failed to open merge file for reading")?;

      self
        .inactive_files
        .insert(merge_inactive_file.id, merge_inactive_file);

      merge_active_file
    };

    let mut all_merge_file_ids = HashSet::<FileID>::new();

    all_merge_file_ids.insert(merge_output_file.id);

    // Read records sequentially from the file.
    // If record.key does not exist in our keydir, move on.
    // If record.key exists, but the filename in the record does not match the
    // file we are merging
    for data_file in merge_files.into_iter() {
      let data_file_id = data_file.id;
      merge_one_file(
        &mut self.keydir,
        data_file,
        self.mutex.clone(),
        &mut self.clock,
        &all_merge_file_ids,
        &mut merge_output_file,
      )?;
      let data_file_path = data_file_name_from_id(data_file_id, &datadir);
      debug!("Now removing {:?} after merge", &data_file_path);
      fs::remove_file(&data_file_path).chain_err(|| {
        format!(
          "Could not remove file {:?} after merge to file id {:?}",
          &data_file_path, &merge_output_file.id
        )
      })?;
      self.inactive_files.remove(&data_file_id);
    }

    Ok(())
  }

  pub fn put(&mut self, key: Vec<u8>, val: Vec<u8>) -> Result<()> {
    let _lock = self.mutex.lock().unwrap();
    let timestamp_now = Arc::get_mut(&mut self.clock)
      .expect("Failed to get clock for logical timestamps")
      .tick();
    let mut record = bitrust_pb::BitRustDataRecord::new();
    record.set_timestamp(timestamp_now);
    record.set_key(key.clone());
    record.set_value(val);

    let (offset, size) = self
      .active_file
      .append_record(&record)
      .chain_err(|| "Failed to write record to active file")?;

    debug!(
      "Record of size {} (data size ={}) written with offset {}",
      size,
      record.compute_size(),
      offset
    );

    self.keydir.insert(
      key,
      KeyDirEntry::new(self.active_file.id, size as u16, offset, timestamp_now),
    );

    debug!(
      "After this write, active file is {} bytes",
      self
        .active_file
        .tell()
        .chain_err(|| "Failed to ftell() active file to get current offset")?
    );

    // Borrowing the field directly is needed here since self cannot be
    // borrowed mutably because of the mutex in scope, which has an
    // immutable borrow on self until the function ends.
    //
    // This is also why maybe_seal_active_file() is a function accepting
    // our fields mutably rather than a method on &mut self.
    let inactive_files = &mut self.inactive_files;
    let active_file = &mut self.active_file;
    let config = &self.config;
    let file_id_gen = &mut self.data_file_id_gen;

    maybe_seal_active_file(active_file, config, file_id_gen)?.map(
      |inactive_file| inactive_files.insert(inactive_file.id, inactive_file),
    );

    Ok(())
  }

  pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
    let entry = self.keydir.get(key);

    if let Some(entry) = entry {
      let record: bitrust_pb::BitRustDataRecord =
        if entry.file_id == self.active_file.id {
          debug!(
            "Fetching from active file (id {}), at offset {}",
            entry.file_id, entry.record_offset
          );
          self
            .active_file
            .read_record(entry.record_offset, entry.record_size as u64)
            .chain_err(|| {
              format!(
                "Failed to read record from active file ({:?})",
                &self.active_file.name
              )
            })?
        } else {
          // if the key is not present in the store, we won't reach here
          // (`entry` would be None). Having an entry pointing to a file
          // we don't know about is bad.
          debug!("Fetching from inactive file id {}", entry.file_id);

          let file =
            self.inactive_files.get_mut(&entry.file_id).ok_or(format!(
              "Got a request for inactive file id {}, but \
               it was not loaded, this is really bad!",
              entry.file_id
            ))?;

          file
            .read_record(entry.record_offset, entry.record_size as u64)
            .chain_err(|| {
              format!(
                "Failed to read record from inactive\
                 file ({:?})",
                &file.name
              )
            })?
        };

      // Currently deletion and the application writing the tombstone
      // value directly are indistinguishable.
      if record.get_value() == BITRUST_TOMBSTONE_STR {
        Ok(None)
      } else {
        Ok(Some(record.get_value().to_vec()))
      }
    } else {
      Ok(None)
    }
  }

  pub fn delete(&mut self, key: &[u8]) -> Result<()> {
    self.put(key.to_vec(), BITRUST_TOMBSTONE_STR.to_vec())
  }

  pub fn keys<'a>(&'a self) -> Vec<&'a [u8]> {
    self.keydir.keys()
  }
}

fn should_seal_active_data(
  active_file: &mut ActiveFile,
  config: &Config,
) -> Result<bool> {
  active_file
    .tell()
    .map(|size| size >= config.max_file_fize_bytes())
    .chain_err(|| "Failed to find size of active file by ftelling it")
}

fn data_file_name_from_id<P: AsRef<Path>>(id: FileID, datadir: P) -> PathBuf {
  datadir.as_ref().join(&format!("{}.data", id))
}

fn update_active_file_id(id: FileID, config: &Config) -> Result<PathBuf> {
  let data_dir = config.datadir();
  let new_active_file_path = data_file_name_from_id(id, &data_dir);
  let ptr_path = active_file_pointer_path(&data_dir);

  util::write_to_file(
    &ptr_path,
    new_active_file_path
      .to_str()
      .expect("Garbled data file name"),
  )
  .chain_err(|| "Failed to update active file name in the pointer file")?;

  Ok(new_active_file_path)
}

// This fn is not thread safe, and assumes only one caller gets to call it at
// a time.
fn maybe_seal_active_file(
  active_file: &mut ActiveFile,
  config: &Config,
  file_id_gen: &mut util::FileIDGen,
) -> Result<Option<InactiveFile>> {
  // Ultimately we want to close the current active file and start
  // writing to a new one. The pointers into the old file should still
  // be active.
  if should_seal_active_data(active_file, config)? {
    debug!("Active file is too big, sealing");
    let old_active_file = {
      // XXX: ensure there are no conflicts
      let new_active_file_id = file_id_gen.take_next();
      let new_active_file_path =
        update_active_file_id(new_active_file_id, config)?;
      debug!("New active file is {:?}", &new_active_file_path);

      let mut new_active_file = ActiveFile::new(new_active_file_path.clone())
        .chain_err(|| {
        format!(
          "Could not create new active file (old:\
           {:?}, new: {:?})",
          &active_file.name, &new_active_file_path
        )
      })?;

      std::mem::swap(&mut new_active_file, active_file);
      new_active_file
    };
    debug!("Making file {} inactive", old_active_file.id);
    Ok(Some(old_active_file.into()))
  //self.inactive_files.insert(inactive_file.id, inactive_file);
  } else {
    Ok(None)
  }
}

// Returns the largest timestamp encountered, and the the fleshened keydir.
fn build_keydir(dd_contents: &util::DataDirContents) -> Result<(u64, KeyDir)> {
  info!("Making keydir");
  // Process files in increasing order of id.
  let mut file_ids: Vec<FileID> = dd_contents.keys().cloned().collect();
  file_ids.sort_unstable();
  let mut keydir = KeyDir::new();
  let mut max_timestamp = 0;
  for file_id in file_ids {
    match dd_contents.get(&file_id).unwrap() {
      // If we have the hint file, we prefer reading it since it is almost
      // a direct on-disk representation of the keydir.
      util::DataDirEntry::DataAndHintFile(_data_file_path, hint_file_path) => {
        debug!("Reading hint file for file id {}", file_id);
        let mut hint_file = BufReader::new(
          File::open(&hint_file_path)
            .chain_err(|| "Could not open hint file.")?,
        );
        max_timestamp =
          read_hint_file_into_keydir(file_id, &mut hint_file, &mut keydir)
            .chain_err(|| {
              format!(
                "Failed to read hint file {:?} into keydir",
                &hint_file_path
              )
            })?;
      }
      util::DataDirEntry::DataFile(data_file_path) => {
        debug!("Reading data file id {}", file_id);
        let mut data_file = BufReader::new(File::open(&data_file_path)?);
        max_timestamp =
          read_data_file_into_keydir(file_id, &mut data_file, &mut keydir)
            .chain_err(|| {
              format!(
                "Failed to read data file {:?} into keydir",
                &data_file_path
              )
            })?;
      }
    }
  }
  Ok((max_timestamp, keydir))
}

fn read_hint_file_into_keydir<R>(
  file_id: FileID,
  hint_file: &mut R,
  keydir: &mut KeyDir,
) -> io::Result<u64>
where
  R: BufRead,
{
  let mut max_timestamp = 0u64;
  // We receive a Some(_) when EOF hasn't been reached.
  while let Some(ts) = read_hint_file_record(file_id, hint_file, keydir)? {
    max_timestamp = ts;
  }
  Ok(max_timestamp)
}

// Return an Err(_) when an io error happens
//
// Return an Ok(Some(timestamp)) when we did not hit EOF when trying to read a
// record (i.e., more might come)
//
// Return an Ok(None) when we encountered an EOF when reading the first 8
// bytes of the record (means end of stream).
//
// There is a possibility that we read a part of the 8 bytes that we wanted to
// read, and that might indicate a subtle corruption. We handle that by reading
// the first field slightly more laboriously.
fn read_hint_file_record<R>(
  file_id: FileID,
  mut hint_file: &mut R,
  keydir: &mut KeyDir,
) -> io::Result<Option<u64>>
where
  R: BufRead,
{
  // XXX: Somehow using BufReader here does not work, investigate.
  if hint_file.fill_buf()?.is_empty() {
    // EOF
    return Ok(None);
  }
  let hint_record =
    protobuf::parse_from_reader::<bitrust_pb::HintFileRecord>(&mut hint_file)?;
  keydir.insert(
    hint_record.get_key().to_vec(),
    KeyDirEntry::new(
      file_id,
      hint_record.get_record_size() as u16,
      hint_record.get_record_offset().into(),
      hint_record.get_timestamp(),
    ),
  );
  Ok(Some(hint_record.get_timestamp()))
}

fn read_data_file_into_keydir<R>(
  file_id: FileID,
  mut data_file: &mut R,
  keydir: &mut KeyDir,
) -> io::Result<u64>
where
  R: BufRead,
{
  let mut offset = 0;
  let mut max_timestamp = 0u64;
  while let Some((timestamp, new_offset)) =
    read_data_file_record_into_keydir(file_id, &mut data_file, keydir, offset)?
  {
    max_timestamp = timestamp;
    offset = new_offset;
  }
  Ok(max_timestamp)
}

// Returns the timestamp of the record just read, and the offset at which the
// next read can begin.
fn read_data_file_record_into_keydir<R>(
  file_id: FileID,
  mut data_file: R,
  keydir: &mut KeyDir,
  offset: u64,
) -> io::Result<Option<(u64, u64)>>
where
  R: BufRead,
{
  if data_file.fill_buf()?.is_empty() {
    return Ok(None);
  }
  let rec: bitrust_pb::BitRustDataRecord =
    protobuf::parse_from_reader(&mut data_file)?;
  keydir.insert(
    rec.get_key().to_vec(),
    KeyDirEntry::new(
      file_id,
      rec.compute_size() as u16,
      offset,
      rec.get_timestamp(),
    ),
  );
  Ok(Some((
    rec.get_timestamp(),
    offset + rec.compute_size() as u64,
  )))
}

pub struct BitRust<ClockT> {
  state: BitRustState<ClockT>,
  running: Arc<atomic::AtomicBool>,
}

impl<ClockT> Drop for BitRust<ClockT> {
  fn drop(&mut self) {
    self.running.store(false, atomic::Ordering::SeqCst);
  }
}

impl<ClockT> BitRust<ClockT>
where
  ClockT: util::LogicalClock,
{
  pub fn open(config: Config, clock: ClockT) -> Result<BitRust<ClockT>> {
    let state = BitRustState::new(config, clock)?;
    let running = Arc::new(atomic::AtomicBool::new(true));

    Ok(BitRust { state, running })
  }

  pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
    debug_timeit!("get" => {
        self.state.get(key)
    })
  }

  pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
    debug_timeit!("put" => {
        self.state.put(key, value)
    })
  }

  pub fn delete(&mut self, key: &[u8]) -> Result<()> {
    debug_timeit!("delete" => {
        self.state.delete(key)
    })
  }

  pub fn keys<'a>(&'a self) -> Vec<&'a [u8]> {
    debug_timeit!("keys" => {
        self.state.keys()
    })
  }

  pub fn merge(&mut self) -> Result<()> {
    debug_timeit!("merge" => {
        self.state.merge()
    })
  }
}

fn active_file_pointer_path<P: AsRef<Path>>(data_dir: P) -> PathBuf {
  data_dir.as_ref().join(".activefile")
}

fn active_file_path<P: AsRef<Path>>(data_dir: P) -> Result<PathBuf> {
  let ptr_path = active_file_pointer_path(data_dir);
  util::read_from_file(&ptr_path)
    .map(PathBuf::from)
    .chain_err(|| {
      format!(
        "Failed to read active file name from pointer file {:?}",
        &ptr_path
      )
    })
}

fn merge_one_file<ClockT: util::LogicalClock>(
  keydir: &mut KeyDir,
  mut data_file: InactiveFile,
  mutex: Arc<Mutex<()>>,
  clock: &mut Arc<ClockT>,
  all_merge_file_ids: &HashSet<FileID>,
  merge_output_file: &mut ActiveFile,
) -> Result<()> {
  debug!("Merging file {:?}", &data_file.name);

  'record_loop: while let Some(record) = data_file.next_record()? {
    // Name of the file that the keydir points to for this key.
    if let Some((current_timestamp, current_file_id)) = keydir
      .get(record.get_key())
      .map(|entry| (entry.timestamp, entry.file_id))
    {
      if (!all_merge_file_ids.contains(&current_file_id)
        && current_file_id > data_file.id)
        || current_timestamp > record.get_timestamp()
      {
        // We have a newer record in the keydir
        debug!(
          "  Merge miss, current ts={}, our ts={}, current fid={}, our fid={}",
          current_timestamp,
          record.get_timestamp(),
          current_file_id,
          data_file.id,
        );
        continue 'record_loop;
      }
      debug!(
        "  Merge hit, current ts={}, our ts={}",
        current_timestamp,
        record.get_timestamp()
      );
      let record_key = record.get_key().to_vec();
      // We found the key in the keydir, and (at least for the time being)
      // the file_id pointed to from the keydir is the same as of the
      // file we are merging. So we write out the merge record to the
      // output file.
      let (record_offset, record_size) = merge_output_file
        .append_record(&record)
        .chain_err(|| "Failed to write merge record")?;
      // BUT between now and us writing the record, a fresh write for the
      // key might have come in. Before we update the keydir to point
      // to the new merge output file for this key, we must check for
      // this event, and the following sequence of operations must be
      // done atomically:
      //
      //  - check if data_file.id is still the current file id in the
      //    keydir.
      //    * If yes, update the keydir.
      //
      //  If we failed to update the keydir in the above sequence, we
      //  should erase the record we just wrote, and then continue with
      //  the merge.
      let timestamp_now = {
        let _lock = mutex.lock().unwrap();
        Arc::get_mut(clock)
          .expect("Failed to get logical timestamp generator")
          .tick()
      };

      let new_keydir_entry = KeyDirEntry::new(
        merge_output_file.id,
        record_size as u16,
        record_offset,
        timestamp_now,
      );

      // Update only if either:
      // 1. The file we are currently merging (data_file) is the same id as the
      // keydir entry for this key.
      // 2. Or, the file id in the keydir is a previous merge file id from this
      // merge cycle.
      let write_successful = keydir.update_keydir_entry_if(
        &record_key,
        new_keydir_entry,
        |old_keydir_entry| {
          data_file.id == old_keydir_entry.file_id
            || all_merge_file_ids.contains(&old_keydir_entry.file_id)
        },
      );

      if !write_successful {
        // unwrite the record we just wrote.
        debug!(
          "*** Concurrent write to a key being merged detected, unmerging\
          record {:?}",
          &record
        );
        merge_output_file
          .retreat(record_size as u64)
          .chain_err(|| {
            format!(
              "Could not unappend record during merge of file id {} to merge \
              output {}",
              data_file.id, merge_output_file.id
            )
          })?;
      }
    } else {
      // No need to merge, since the keydir does not contain this key,
      // which means the key was deleted.
      debug!(
        "Skipping the merge of key {:?} since it was not found in the keydir",
        &record.get_key()
      );
    }
  }
  Ok(())
}

#[cfg(test)]
mod tests {

  extern crate simplelog;
  extern crate tempfile;

  use simplelog::{CombinedLogger, LevelFilter, TermLogger};

  use super::*;
  use protobuf::Message;
  use std::ffi::OsStr;
  use std::io::Cursor;
  use test::Bencher;
  use util::LogicalClock;

  #[allow(dead_code)]
  fn setup_logging() -> Result<()> {
    CombinedLogger::init(vec![TermLogger::new(
      LevelFilter::Debug,
      simplelog::Config::default(),
    )
    .unwrap()])
    .chain_err(|| "Error setting up logging")
    .map(|_| ())
    .map_err(|e| e.into())
  }

  struct MockClock {
    time: u64,
  }

  impl MockClock {
    fn new() -> MockClock {
      MockClock { time: 0 }
    }
  }

  impl util::LogicalClock for MockClock {
    fn tick(&mut self) -> u64 {
      self.time
    }
    fn set_next(&mut self, t: u64) {
      self.time = t;
    }
  }

  #[test]
  fn test_active_file_retreat() {
    let data_dir = tempfile::tempdir().unwrap();
    let mut f =
      ActiveFile::new(data_dir.as_ref().join("0")).expect("Active file");

    let mut rec = bitrust_pb::BitRustDataRecord::new();
    rec.set_timestamp(42);
    rec.set_key(b"k".to_vec());
    rec.set_value(b"v".to_vec());

    let (_, size) = f
      .append_record(&rec)
      .expect("Appending record should succeed");
    f.retreat(size).expect("Retreating should succeed");

    let mut rec = bitrust_pb::BitRustDataRecord::new();
    rec.set_timestamp(42);
    rec.set_key(b"k2".to_vec());
    rec.set_value(b"v2".to_vec());
    f.append_record(&rec)
      .expect("Appending record after retreat should succeed");

    let read_rec = f
      .record_at_offset(0)
      .expect("Read record after retreat and write")
      .expect("Some record");
    assert!(read_rec == rec, "Expected {:?}, got {:?}", rec, read_rec);
  }

  #[test]
  fn test_merge() {
    let sz_limit = 100;
    let data_dir = tempfile::tempdir().unwrap();
    let cfg = ConfigBuilder::new(&data_dir)
      .max_file_fize_bytes(sz_limit)
      .build();
    let mut br = BitRust::open(cfg, MockClock::new()).unwrap();
    let keys = vec!["foo".to_string(), "bar".to_string(), "baz".to_string()];
    let num_writes = 4;
    for version in 0..num_writes {
      for key in &keys {
        debug!("Writing {} => {}_{:02}", key, key, version);
        let val = format!("{}_{:02}", key, version);
        br.put(key.clone().into_bytes(), val.into_bytes()).unwrap();
      }
    }
    // Trigger a merge and then assert we have the latest versions of the
    // keys only.
    br.merge().unwrap();
    debug!(
      "After merge, inactive files={:?}, keydir={:?}",
      br.state.inactive_files.keys().cloned().collect::<Vec<_>>(),
      &br.state.keydir,
    );
    for key in &keys {
      let expected_val = format!("{}_{:02}", key, num_writes - 1).into_bytes();
      let val = br.get(key.as_bytes()).unwrap().unwrap();
      assert!(
        val == expected_val,
        format!(
          "key={}, expect={}, got={}",
          key,
          std::str::from_utf8(&expected_val).unwrap(),
          std::str::from_utf8(&val).unwrap()
        )
      );
    }
  }

  #[test]
  fn test_read_hintfile_into_keydir() {
    let mut cursor = Cursor::new(Vec::new());
    let mut hint_records = Vec::new();
    for _ in 0..1 {
      let mut hint_record = bitrust_pb::HintFileRecord::new();
      hint_record.set_timestamp(42);
      hint_record.set_key(b"foo".to_vec());
      hint_record.set_record_offset(9);
      hint_record.set_record_size(103);
      hint_record.write_to_writer(&mut cursor).unwrap();
      hint_records.push(hint_record);
    }
    debug!("Done preparing test_read_hintfile_into_keydir records");
    cursor.set_position(0);
    let mut keydir = KeyDir::new();
    read_hint_file_into_keydir(0, &mut cursor, &mut keydir).unwrap();

    debug!("On to asserting now");
    for hint_record in hint_records.into_iter() {
      let keydir_entry = keydir.get(hint_record.get_key());
      assert!(keydir_entry.is_some(), "Expected to find keydir entry");
      let keydir_entry = keydir_entry.unwrap();
      assert!(keydir_entry.file_id == 0);
      assert!(keydir_entry.record_size == hint_record.get_record_size() as u16);
      assert!(
        keydir_entry.record_offset == hint_record.get_record_offset() as u64
      );
      assert!(keydir_entry.timestamp == hint_record.get_timestamp());
    }
  }

  #[test]
  fn test_read_datafile_into_keydir() {
    let mut cursor = Cursor::new(Vec::new());
    let mut data_records = Vec::new();
    let mut offsets_and_sizes = Vec::new();
    for _ in 0..1 {
      let mut data_record = bitrust_pb::BitRustDataRecord::new();
      data_record.set_timestamp(42);
      data_record.set_key(b"foo".to_vec());
      data_record.set_value(b"bar".to_vec());
      offsets_and_sizes.push((cursor.position(), data_record.compute_size()));
      data_record.write_to_writer(&mut cursor).unwrap();
      data_records.push(data_record);
    }
    cursor.set_position(0);
    let mut keydir = KeyDir::new();
    read_data_file_into_keydir(0, &mut cursor, &mut keydir).unwrap();
    debug!("keydir loaded as {:?}", &keydir.entries);
    assert!(keydir.entries.len() == data_records.len());
    for ((offset, size), data_record) in
      offsets_and_sizes.into_iter().zip(data_records.into_iter())
    {
      let keydir_entry = keydir
        .get(data_record.get_key())
        .expect("Expected keydir record not found");
      assert!(keydir_entry.file_id == 0);
      assert!(keydir_entry.record_size == size as u16);
      assert!(keydir_entry.record_offset == offset);
      assert!(keydir_entry.timestamp == data_record.get_timestamp());
    }
  }

  #[test]
  fn test_bitrust_state_evolution() {
    let sz_limit = 1_000;

    let data_dir = tempfile::tempdir().unwrap();
    let cfg = ConfigBuilder::new(&data_dir)
      .max_file_fize_bytes(sz_limit)
      .build();

    let mut br = BitRust::open(cfg, MockClock::new()).unwrap();

    let mut clock = MockClock::new();
    clock.set_next(42);

    let mut proto_record = bitrust_pb::BitRustDataRecord::new();
    proto_record.set_timestamp(clock.tick());
    proto_record.set_key(b"somekey".to_vec());
    proto_record.set_value(b"somevalue".to_vec());
    let entry_sz = storage::payload_size_for_record(&proto_record);

    let num_entries = 1000;

    for _ in 0..num_entries {
      br.put(
        proto_record.get_key().to_vec(),
        proto_record.get_value().to_vec(),
      )
      .unwrap();
    }

    let expected_num_data_files =
      (entry_sz as f64 * num_entries as f64 / sz_limit as f64).ceil() as usize;

    let data_files = fs::read_dir(&data_dir)
      .unwrap()
      .map(|entry| entry.unwrap())
      .filter_map(|entry| {
        let is_data_file = entry
          .path()
          .extension()
          .and_then(OsStr::to_str)
          .map_or(false, |s| s == "data");

        if is_data_file {
          Some(data_dir.as_ref().join(entry.path()))
        } else {
          None
        }
      })
      .collect::<Vec<_>>();

    let active_file_pointer_path = active_file_pointer_path(&data_dir);

    let persisted_active_file_name =
      PathBuf::from(util::read_from_file(active_file_pointer_path).unwrap());

    assert!(persisted_active_file_name == br.state.active_file.name);

    assert!(
      data_files.len() == expected_num_data_files,
      format!(
        "Expected {} data files, found {}",
        expected_num_data_files,
        data_files.len()
      )
    );
  }

  #[test]
  fn test_bitrust_state_init_from_scratch() {
    // we expect the active file to be sealed once it reaches 1kB
    let sz_limit = 1_000;

    let data_dir = tempfile::tempdir().unwrap();
    let cfg = ConfigBuilder::new(data_dir.as_ref())
      .max_file_fize_bytes(sz_limit)
      .build();

    let br = BitRust::open(cfg, MockClock::new()).unwrap();

    assert!(br.state.active_file.name == data_dir.as_ref().join("0.data"));
    assert!(br.state.keydir.entries.len() == 0);
    assert!(br.state.inactive_files.len() == 0);
  }

  #[test]
  fn test_get_files_for_merging() {
    let sz_limit = 1024; // bytes

    let data_dir = tempfile::tempdir().unwrap();
    let cfg = ConfigBuilder::new(&data_dir)
      .max_file_fize_bytes(sz_limit)
      .build();

    let mut br = BitRust::open(cfg, MockClock::new()).unwrap();
    let mut clock = MockClock::new();
    clock.set_next(42);

    let mut proto_record = bitrust_pb::BitRustDataRecord::new();
    proto_record.set_timestamp(clock.tick());
    proto_record.set_key(b"somekey".to_vec());
    proto_record.set_value(b"somevalue".to_vec());
    let entry_sz = storage::payload_size_for_record(&proto_record);

    let total_entries = 256;
    let entries_per_file = sz_limit / (entry_sz as u64);

    let total_files_needed = (total_entries as f64 * entry_sz as f64
      / sz_limit as f64)
      .ceil() as usize;
    let num_merge_files_expected = if total_files_needed * sz_limit as usize
      == (total_entries * entry_sz) as usize
    {
      total_files_needed
    } else {
      total_files_needed - 1
    };

    for _ in 0..total_entries {
      br.put(
        proto_record.get_key().to_vec(),
        proto_record.get_value().to_vec(),
      )
      .unwrap();
    }

    let files_to_merge = br.state.get_files_for_merging();

    assert!(files_to_merge.len() == num_merge_files_expected, "Total files created={}, merge files expected={}, merge files actual={}, record size={}, total entries={}, entries_per_file={}", total_files_needed, num_merge_files_expected, files_to_merge.len(), entry_sz, total_entries, entries_per_file);

    for fid in files_to_merge {
      assert!(
        fid.0 != br.state.active_file.id,
        "Merge file coverage includes active file id {}",
        br.state.active_file.id
      );
    }
  }

  #[test]
  fn test_overflow_puts() {
    // Test that when we overflow into multiple data files, the store still
    // returns expected values.

    let sz_limit = 100; // small size limit so we always overflow.

    let data_dir = tempfile::tempdir().unwrap();
    let cfg = ConfigBuilder::new(&data_dir)
      .max_file_fize_bytes(sz_limit)
      .build();

    let mut br = BitRust::open(cfg, MockClock::new()).unwrap();

    let key_vals = (0..1000)
      .map(|_| {
        (
          util::rand_str().as_bytes().to_vec(),
          util::rand_str().as_bytes().to_vec(),
        )
      })
      .collect::<Vec<_>>();

    for (key, val) in key_vals.iter().cloned() {
      br.put(key, val).unwrap();
    }

    for (key, val) in key_vals.into_iter() {
      assert!(br.get(&key).unwrap() == Some(val));
    }
  }

  #[test]
  fn test_creation() {
    let data_dir = tempfile::tempdir().unwrap();

    let cfg = ConfigBuilder::new(&data_dir).build();
    let mut br = BitRustState::new(cfg, MockClock::new()).unwrap();

    br.put(b"foo".to_vec(), b"bar".to_vec()).unwrap();
    let r = br.get(b"foo").unwrap().unwrap();
    assert!(r == b"bar");
  }

  #[test]
  fn test_locking_of_data_dir() {
    let data_dir = tempfile::tempdir().unwrap();

    let cfg = ConfigBuilder::new(&data_dir).build();
    let _br = BitRustState::new(cfg.clone(), MockClock::new()).unwrap();

    let another_br = BitRustState::new(cfg, MockClock::new());
    assert!(another_br.is_err());
  }

  #[test]
  fn test_deletion() {
    let data_dir = tempfile::tempdir().unwrap();

    let cfg = ConfigBuilder::new(&data_dir).build();
    let mut br = BitRustState::new(cfg, MockClock::new()).unwrap();

    br.put(b"foo".to_vec(), b"bar".to_vec()).unwrap();
    assert!(br.get(b"foo").unwrap().unwrap() == b"bar");

    br.delete(b"foo").unwrap();
    assert!(br.get(b"foo").unwrap().is_none());
  }

  #[bench]
  fn bench_put(b: &mut Bencher) {
    let data_dir = tempfile::tempdir().unwrap();
    let config = ConfigBuilder::new(&data_dir).build();
    let mut br = BitRust::open(config, MockClock::new()).unwrap();

    let key = util::rand_str().as_bytes().to_vec();
    let val = util::rand_str().as_bytes().to_vec();
    b.iter(move || {
      br.put(key.clone(), val.clone()).unwrap();
    });
  }
}
