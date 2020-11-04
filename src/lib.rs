#![recursion_limit = "1024"]
#![feature(test)]
extern crate byteorder;
extern crate bytes;
extern crate crc;
extern crate crossbeam;
extern crate num;
extern crate protobuf;
extern crate rand;
extern crate serde;
extern crate test;
extern crate toml;

#[macro_use]
extern crate log;
extern crate lazy_static;
extern crate regex;
extern crate simplelog;
#[macro_use]
extern crate error_chain;

pub mod bitrust_pb;
mod common;
pub mod config;
pub mod errors;
mod keydir;
mod lockfile;
mod locking;
mod storage;
pub mod util;

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use crossbeam::channel as chan;
use protobuf::Message;

pub use common::{BitrustOperation, FileID, BITRUST_TOMBSTONE_STR};
pub use config::*;
pub use errors::*;

use keydir::{KeyDir, KeyDirEntry};
use storage::{
  FileBasedRecordRW, FileBasedRecordReader, RecordAppend, RecordRead,
};

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

type ActiveFile = FileBasedRecordRW<bitrust_pb::BitRustDataRecord>;
type InactiveFile = FileBasedRecordReader<bitrust_pb::BitRustDataRecord>;
type HintFileWriter = FileBasedRecordRW<bitrust_pb::HintFileRecord>;
type HintFileReader = FileBasedRecordReader<bitrust_pb::HintFileRecord>;

#[derive(Debug)]
pub struct BitRustState<ClockT> {
  keydir: Arc<RwLock<KeyDir>>,
  active_file: Arc<RwLock<ActiveFile>>,
  inactive_files: Arc<RwLock<HashMap<FileID, InactiveFile>>>,
  lockfile: lockfile::LockFile,
  config: Config,
  // Apart from the initialization code, this is used by put() to overflow
  // data files (i.e., to start a new active file when the current one
  // becomes too big), and by merge(), to get a name for the merge output
  // files, which will replace all older inactive data files after the merge.
  //
  // When a write and a merge are happening concurrently, access to this
  // needs to be synchronized using `mutex` above.
  data_file_id_gen: Arc<Mutex<util::FileIDGen>>,
  clock: Arc<Mutex<ClockT>>,
  merge_thread: Option<std::thread::JoinHandle<()>>,
  merge_stopchan: Option<chan::Sender<()>>,
}

fn setup_bitrust(config: &Config) -> Result<PathBuf> {
  let ptr_path = active_file_pointer_path(&config.datadir);
  debug!(
    "Starting in an empty data directory, writing {:?}",
    &ptr_path
  );
  let first_active_file =
    util::FileKind::DataFile.path_from_id(0, &config.datadir);
  debug!("First active file name {:?}", &first_active_file);
  debug!("To str: {:?}", first_active_file);
  util::write_to_file(
    &ptr_path,
    first_active_file.to_str().chain_err(|| {
      format!(
        "Failed to write active file name to the\
           pointer file {:?}",
        &ptr_path
      )
    })?,
  )?;
  Ok(first_active_file)
}

fn read_inactive_files(
  dd_contents: util::DataDirContents,
  active_file_id: FileID,
) -> Result<HashMap<FileID, InactiveFile>> {
  let mut inactive_files = HashMap::new();
  for (file_id, dd_entry) in dd_contents {
    if file_id == active_file_id {
      continue;
    }
    let inactive_file = InactiveFile::new(dd_entry.data_file_path().clone())
      .chain_err(|| {
        format!(
          "Failed to open inactive file at {:?} for\
               reading into keydir",
          dd_entry.data_file_path()
        )
      })?;
    inactive_files.insert(file_id, inactive_file);
  }
  Ok(inactive_files)
}

impl<ClockT> BitRustState<ClockT>
where
  ClockT: util::LogicalClock + Send + Sync + 'static,
{
  pub fn new(
    config: Config,
    mut clock: ClockT,
  ) -> Result<BitRustState<ClockT>> {
    info!("Making a bitrust");
    fs::create_dir_all(&config.datadir).chain_err(|| {
      format!(
        "Failed to ensure that datadir is created at {:?}\
         (permission issues?)",
        &config.datadir
      )
    })?;

    // If the following fails, all it means is that we have a file at our
    // lockfile location that we did not create. We don't care at this
    // point why it exists, and just bail out.
    //
    // This lock will be released (and the lockfile removed) when the
    // returned object goes out of scope. Since we move it into the
    // returned BitRustState, this means the lock lives as long as the returned
    // BitRustState lives.
    let lockfile = locking::acquire(&config.datadir, BitrustOperation::Write)
      .chain_err(|| {
      format!("Failed to obtain write lock in {:?}", &config.datadir)
    })?;

    debug!("Obtained data directory lock");

    // Get the names of the data and hint files. Because these are named
    // like ${file_id}.data and ${file_id}.hint respectively, we can group
    // together the data and hint files for a given file_id together. A
    // hint file is optional, but cannot exist without a corresponding data
    // file (because hint files just contain pointers into the respective
    // data files)
    let dd_contents =
      util::read_data_dir_contents(&config.datadir).chain_err(|| {
        format!(
          "Failed to enumerate contents of the data dir\
           {:?} when starting up bitrust",
          &config.datadir
        )
      })?;

    let (latest_timestamp, keydir, active_file_name) = if dd_contents.is_empty()
    {
      let active_file_name = setup_bitrust(&config)?;
      (0u64, KeyDir::new(), active_file_name)
    } else {
      debug!("Now building keydir with these files: {:?}", &dd_contents);
      let (latest_timestamp, keydir) = build_keydir(&dd_contents)?;
      let active_file_name = active_file_path(&config.datadir)?;
      (latest_timestamp, keydir, active_file_name)
    };

    debug!("Done building keydir: {:?}", &keydir.entries);

    debug!("Using active file {:?}", &active_file_name);
    let active_file =
      ActiveFile::new(active_file_name.clone()).chain_err(|| {
        format!("Could not open active file {:?}", &active_file_name)
      })?;
    let active_file_id = active_file.id;
    let inactive_files = read_inactive_files(dd_contents, active_file_id)?;
    clock.set_next(latest_timestamp + 1);
    let mut bitrust = BitRustState {
      keydir: Arc::new(RwLock::new(keydir)),
      config: config.clone(),
      inactive_files: Arc::new(RwLock::new(inactive_files)),
      active_file: Arc::new(RwLock::new(active_file)),
      lockfile,
      data_file_id_gen: Arc::new(Mutex::new(util::FileIDGen::new(
        active_file_id + 1,
      ))),
      clock: Arc::new(Mutex::new(clock)),
      merge_thread: None,
      merge_stopchan: None,
    };

    if let Some(auto_merge_cfg) = config.merge_config.auto_merge_config {
      debug!("Automatic merging enabled: {:?}", &auto_merge_cfg);
      let (joinhandle, stopchan) = bitrust.start_merge_thread(
        std::time::Duration::from_secs(auto_merge_cfg.check_interval_secs),
      );
      bitrust.merge_thread = Some(joinhandle);
      bitrust.merge_stopchan = Some(stopchan);
    }

    debug!("Returning from BitRustState::new");
    Ok(bitrust)
  }

  // Starts a thread that periodically runs merge if required.
  // Returns a handle to join on the the thread, and a sender half of a channel
  // which should receive a unit value `()` to signal stop.
  // Clients should first send a unit value on this channel, and then join on
  // the returned handle. Ongoing merges will be finished before the function
  // returns.
  fn start_merge_thread(
    &self,
    check_interval: std::time::Duration,
  ) -> (std::thread::JoinHandle<()>, chan::Sender<()>) {
    let keydir = self.keydir.clone();
    let inactive_files = self.inactive_files.clone();
    let idgen = self.data_file_id_gen.clone();

    // Use a buffered channel for letting the main thread signal bedtime.
    // An unbuffered channel (with capacity 0) will cause Sender::try_send() to
    // fail if there is no active Receiver::recv(). This will happen if the main
    // thread sends a stop signal while we are in the merge() function below.
    // This will prevent the merge thread from ever going down.
    let (sender, receiver) = chan::bounded(1);

    let config = self.config.clone();
    let t = std::thread::spawn(move || loop {
      match receiver.recv_timeout(check_interval) {
        Ok(_) | Err(chan::RecvTimeoutError::Disconnected) => {
          info!("Merge thread going down");
          break;
        }
        Err(chan::RecvTimeoutError::Timeout) => {
          info!("Merging..");
          if let Err(e) = merge(
            keydir.clone(),
            inactive_files.clone(),
            idgen.clone(),
            config.clone(),
            false,
          ) {
            warn!("Error merging: {:?}", e);
          }
        }
      }
    });
    (t, sender)
  }

  pub fn active_file_id(&self) -> FileID {
    self.active_file.read().expect("Lock on active file").id
  }

  pub fn merge(&self, force_merge: bool) -> Result<()> {
    merge(
      self.keydir.clone(),
      self.inactive_files.clone(),
      self.data_file_id_gen.clone(),
      self.config.clone(),
      force_merge,
    )
  }

  pub fn put(&self, key: Vec<u8>, val: Vec<u8>) -> Result<()> {
    // Locks can be poisoned if a thread fails while holding them.
    // We just crash when this happens.
    let mut clock = self.clock.lock().expect("Lock clock for put");
    let mut keydir = self.keydir.write().expect("WLock keydir for put");
    let mut active_file =
      self.active_file.write().expect("WLock active_file for put");
    let mut inactive_files = self
      .inactive_files
      .write()
      .expect("WLock inactive_files for put");
    let mut file_id_gen = self
      .data_file_id_gen
      .lock()
      .expect("WLock data_file_id_gen for put");

    let timestamp_now = clock.tick();
    let mut record = bitrust_pb::BitRustDataRecord::new();
    record.set_timestamp(timestamp_now);
    record.set_key(key.clone());
    record.set_value(val);

    let (offset, size) = active_file
      .append_record(&record)
      .chain_err(|| "Failed to write record to active file")?;

    debug!(
      "Record of size {} (data size ={}) written with offset {}",
      size,
      record.compute_size(),
      offset
    );

    keydir.insert(
      key,
      KeyDirEntry::new(active_file.id, size as u16, offset, timestamp_now),
    );

    debug!(
      "After this write, active file is {} bytes",
      active_file
        .tell()
        .chain_err(|| "Failed to ftell() active file to get current offset")?
    );

    // Borrowing the field directly is needed here since self cannot be
    // borrowed mutably because of the mutex in scope, which has an
    // immutable borrow on self until the function ends.
    //
    // This is also why maybe_seal_active_file() is a function accepting
    // our fields mutably rather than a method on &mut self.
    let config = &self.config;

    if let Some(inactive_file) =
      maybe_rotate_output(&mut active_file, config, &mut file_id_gen)?
    {
      inactive_files.insert(inactive_file.id, inactive_file);
      update_active_file_id(active_file.id, config)?;
    }
    Ok(())
  }

  pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
    // Locks can be poisoned if a thread fails while holding them.
    // We just crash when this happens.
    let keydir = self.keydir.read().expect("rlock on keydir for get");
    let mut active_file = self
      .active_file
      .write()
      .expect("rlock on active_file for read");
    let mut inactive_files = self
      .inactive_files
      .write()
      .expect("rlock on inactive_files for read");

    if let Some(entry) = keydir.get(key) {
      let record: bitrust_pb::BitRustDataRecord =
        if entry.file_id == active_file.id {
          debug!(
            "Fetching from active file (id {}), at offset {}",
            entry.file_id, entry.record_offset
          );
          active_file
            .read_record(entry.record_offset, entry.record_size as u64)
            .chain_err(|| {
              format!(
                "Failed to read record from active file ({:?})",
                &active_file.path
              )
            })?
        } else {
          // if the key is not present in the store, we won't reach here
          // (`entry` would be None). Having an entry pointing to a file
          // we don't know about is bad.
          debug!("Fetching from inactive file id {}", entry.file_id);

          let file = inactive_files.get_mut(&entry.file_id).ok_or(format!(
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
                &file.path
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

  pub fn delete(&self, key: &[u8]) -> Result<()> {
    self.put(key.to_vec(), BITRUST_TOMBSTONE_STR.to_vec())
  }

  pub fn keys<'a>(&'a self) -> Vec<Vec<u8>> {
    let keydir = self.keydir.read().expect("rlock on keydir for keys");
    keydir.keys().iter().cloned().map(Vec::from).collect()
  }
}

fn is_recordio_overlimit<T: Message>(
  rw: &mut FileBasedRecordRW<T>,
  config: &Config,
) -> Result<bool> {
  rw.tell()
    .map(|size| size >= config.file_size_soft_limit_bytes as u64)
    .chain_err(|| "Failed to find size of active file by ftelling it")
}

fn update_active_file_id(id: FileID, config: &Config) -> Result<PathBuf> {
  let new_active_file_path =
    util::FileKind::DataFile.path_from_id(id, &config.datadir);
  let ptr_path = active_file_pointer_path(&config.datadir);
  util::write_to_file(
    &ptr_path,
    new_active_file_path.to_str().chain_err(|| {
      format!(
        "update_active_file_id: Failed to serialize active \
        file pathbuf {:?} to str",
        &new_active_file_path
      )
    })?,
  )
  .chain_err(|| "Failed to update active file name in the pointer file")?;
  Ok(new_active_file_path)
}

// This fn is not thread safe.
fn maybe_rotate_output<T: Message>(
  rw: &mut FileBasedRecordRW<T>,
  config: &Config,
  file_id_gen: &mut util::FileIDGen,
) -> Result<Option<FileBasedRecordReader<T>>> {
  // Ultimately we want to close the current active file and start
  // writing to a new one. The pointers into the old file should still
  // be active.
  if is_recordio_overlimit(rw, config)? {
    debug!("Active file is too big, sealing");
    rotate_output(rw, file_id_gen.take_next(), config).map(|f| Some(f))
  } else {
    Ok(None)
  }
}

// Closes the given active file, and swaps it with a new active file with
// the given id. Returns the old active file an InactiveFile.
// Not threadsafe.
fn rotate_output<T: Message>(
  rw: &mut FileBasedRecordRW<T>,
  new_id: FileID,
  config: &Config,
) -> Result<FileBasedRecordReader<T>> {
  let old_rw = {
    let new_path = util::FileKind::from_path(&rw.path)?
      .path_from_id(new_id, &config.datadir);
    debug!("Rotating {} -> {}", rw.id, new_id);
    let mut new_rw =
      FileBasedRecordRW::<T>::new(new_path.clone()).chain_err(|| {
        format!(
          "Could not create new active file (old:\
          {:?}, new: {:?})",
          &rw.path, &new_path
        )
      })?;
    std::mem::swap(&mut new_rw, rw);
    new_rw
  };
  debug!("Making file {} inactive", old_rw.id);
  Ok(old_rw.into())
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
      util::DataDirEntry::DataAndHintFile(data_file_path, hint_file_path) => {
        debug!("Reading hint file for file id {}", file_id);
        let mut hint_file = HintFileReader::new(hint_file_path.clone())?;
        match read_hint_file_into_keydir(file_id, &mut hint_file, &mut keydir) {
          Ok(hint_max_ts) => {
            max_timestamp = std::cmp::max(max_timestamp, hint_max_ts);
          }
          Err(e) => {
            warn!(
              "Error encountered when reading hint file {:?}: {:?},
              attempting to read data file instead.",
              hint_file_path, e
            );
            let mut data_file = InactiveFile::new(data_file_path.clone())?;
            let max_timestamp_in_file =
              read_data_file_into_keydir(file_id, &mut data_file, &mut keydir)
                .chain_err(|| {
                  format!(
                    "Failed to read data file {:?} into keydir",
                    &data_file_path
                  )
                })?;
            max_timestamp = std::cmp::max(max_timestamp, max_timestamp_in_file);
          }
        }
      }
      util::DataDirEntry::DataFile(data_file_path) => {
        debug!("Reading data file id {}", file_id);
        let mut data_file = InactiveFile::new(data_file_path.clone())?;
        let max_timestamp_in_file =
          read_data_file_into_keydir(file_id, &mut data_file, &mut keydir)
            .chain_err(|| {
              format!(
                "Failed to read data file {:?} into keydir",
                &data_file_path
              )
            })?;
        max_timestamp = std::cmp::max(max_timestamp, max_timestamp_in_file);
      }
    }
  }
  Ok((max_timestamp, keydir))
}

fn read_hint_file_into_keydir(
  file_id: FileID,
  hint_file: &mut HintFileReader,
  keydir: &mut KeyDir,
) -> Result<u64> {
  let mut max_timestamp = 0u64;
  // We receive a Some(_) when EOF hasn't been reached.
  while let Some(ts) =
    read_hint_file_record_into_keydir(file_id, hint_file, keydir)?
  {
    max_timestamp = std::cmp::max(ts, max_timestamp);
  }
  Ok(max_timestamp)
}

fn read_hint_file_record_into_keydir(
  file_id: FileID,
  hint_file: &mut HintFileReader,
  keydir: &mut KeyDir,
) -> Result<Option<u64>> {
  Ok(hint_file.next_record()?.map(|rec| {
    keydir.insert_if_newer(
      rec.get_key().to_vec(),
      KeyDirEntry::new(
        file_id,
        rec.get_record_size() as u16,
        rec.get_record_offset() as u64,
        rec.get_timestamp(),
      ),
    );
    rec.get_timestamp()
  }))
}

fn read_data_file_into_keydir(
  file_id: FileID,
  data_file: &mut InactiveFile,
  keydir: &mut KeyDir,
) -> Result<u64> {
  let mut offset = 0;
  let mut max_timestamp = 0u64;
  while let Some((timestamp, new_offset)) =
    read_data_file_record_into_keydir(file_id, data_file, keydir, offset)?
  {
    max_timestamp = std::cmp::max(timestamp, max_timestamp);
    offset = new_offset;
  }
  Ok(max_timestamp)
}

// Returns the timestamp of the record just read, and the offset at which the
// next read can begin.
fn read_data_file_record_into_keydir(
  file_id: FileID,
  data_file: &mut InactiveFile,
  keydir: &mut KeyDir,
  offset: u64,
) -> Result<Option<(u64, u64)>> {
  Ok(data_file.next_record()?.map(|rec| {
    let entry_sz = storage::payload_size_for_record(&rec);
    keydir.insert_if_newer(
      rec.get_key().to_vec(),
      KeyDirEntry::new(file_id, entry_sz as u16, offset, rec.get_timestamp()),
    );
    (rec.get_timestamp(), offset + entry_sz)
  }))
}

pub struct BitRust<ClockT> {
  pub state: BitRustState<ClockT>,
  pub running: Arc<atomic::AtomicBool>,
}

impl<ClockT> Drop for BitRust<ClockT> {
  fn drop(&mut self) {
    self.running.store(false, atomic::Ordering::SeqCst);
  }
}

impl<ClockT> BitRust<ClockT>
where
  ClockT: util::LogicalClock + Send + Sync + 'static,
{
  pub fn open(config: Config, clock: ClockT) -> Result<BitRust<ClockT>> {
    let state = BitRustState::new(config, clock)?;
    let running = Arc::new(atomic::AtomicBool::new(true));
    Ok(BitRust { state, running })
  }

  pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
    debug_timeit!("get" => {
        self.state.get(key)
    })
  }

  pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
    debug_timeit!("put" => {
        self.state.put(key, value)
    })
  }

  pub fn delete(&self, key: &[u8]) -> Result<()> {
    debug_timeit!("delete" => {
        self.state.delete(key)
    })
  }

  pub fn keys<'a>(&'a self) -> Vec<Vec<u8>> {
    debug_timeit!("keys" => {
        self.state.keys()
    })
  }

  pub fn merge(&self) -> Result<()> {
    self.state.merge(/*force_merge=*/ true)
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

fn write_hint_record(
  hint_file_writer: &mut HintFileWriter,
  entry_offset: u64,
  entry_size: u64,
  record: &bitrust_pb::BitRustDataRecord,
) -> Result<()> {
  let mut hint = bitrust_pb::HintFileRecord::new();
  hint.set_key(record.get_key().to_vec());
  hint.set_record_offset(entry_offset as u32);
  hint.set_record_size(entry_size as u32);
  hint.set_timestamp(record.get_timestamp());
  hint_file_writer.append_record(&hint).map(|_| ())
}

fn handle_merge_output_overflow(
  file_id_gen: Arc<Mutex<util::FileIDGen>>,
  inactive_files: Arc<RwLock<HashMap<FileID, InactiveFile>>>,
  config: &Config,
  merge_output_file: &mut ActiveFile,
  hint_file_writer: &mut HintFileWriter,
) -> Result<()> {
  let old_merge_output = {
    let mut idgen = file_id_gen
      .lock()
      .expect("Lock on file_id_gen to rotate mergefile");
    maybe_rotate_output(merge_output_file, config, &mut idgen)?
  };
  if let Some(inactive_file) = old_merge_output {
    let mut inactive_files = inactive_files
      .write()
      .expect("WLock on inactive_files for merge file rotation");
    inactive_files.insert(inactive_file.id, inactive_file);
    inactive_files.insert(
      merge_output_file.id,
      InactiveFile::new(merge_output_file.path.clone())
        .chain_err(|| "Creating an InactiveFile for the new merge file")?,
    );
    rotate_output(hint_file_writer, merge_output_file.id, config)?;
  }
  Ok(())
}

fn merge_one_file(
  keydir: Arc<RwLock<KeyDir>>,
  mut data_file: InactiveFile,
  all_merge_file_ids: &mut HashSet<FileID>,
  inactive_files: Arc<RwLock<HashMap<FileID, InactiveFile>>>,
  file_id_gen: Arc<Mutex<util::FileIDGen>>,
  config: &Config,
  merge_output_file: &mut ActiveFile,
  hint_file_writer: &mut HintFileWriter,
) -> Result<()> {
  debug!(
    "Merging file {:?}, all mergefiles: {:?}",
    &data_file.path, all_merge_file_ids
  );
  'record_loop: while let Some(record) = data_file.next_record()? {
    handle_merge_output_overflow(
      file_id_gen.clone(),
      inactive_files.clone(),
      config,
      merge_output_file,
      hint_file_writer,
    )?;

    let mut keydir = keydir
      .write()
      .expect("WLock on keydir to write merge record");

    if let Some(curr_ts) =
      keydir.get(record.get_key()).map(|entry| entry.timestamp)
    {
      if curr_ts > record.get_timestamp() {
        // We have a newer record in the keydir.
        debug!(
          "  Merge miss, current ts={}, our ts={}",
          curr_ts,
          record.get_timestamp(),
        );
        continue 'record_loop;
      }
      debug!(
        "  Merge hit, current ts={}, our ts={}",
        curr_ts,
        record.get_timestamp(),
      );
      let record_key = record.get_key().to_vec();
      // We found the key in the keydir, and (at least for the time being)
      // the file_id pointed to from the keydir is the same as of the
      // file we are merging. So we write out the merge record to the
      // output file.
      let (record_offset, record_size) = merge_output_file
        .append_record(&record)
        .chain_err(|| "Failed to write merge record")?;
      let new_keydir_entry = KeyDirEntry::new(
        merge_output_file.id,
        record_size as u16,
        record_offset,
        record.get_timestamp(),
      );
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
      //
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
      ); // chan
      if write_successful {
        if let Err(e) = write_hint_record(
          hint_file_writer,
          record_offset,
          record_size,
          &record,
        ) {
          if config.merge_config.require_hint_file_write_success {
            return Err(e);
          }
          warn!(
            "Failed to write record to hint file {:?}: {:?}",
            hint_file_writer.path, e
          );
        }
      } else {
        // unwrite the record we just wrote.
        debug!(
          "Concurrent write to key {:?} being merged detected, unmerging\
          record",
          record.get_key()
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

fn get_files_for_merging(
  inactive_files: &HashMap<FileID, InactiveFile>,
  config: &Config,
) -> Vec<(FileID, PathBuf)> {
  let mut files_to_merge = inactive_files
    .keys()
    .cloned()
    .map(|id| {
      (
        id,
        util::FileKind::DataFile.path_from_id(id, &config.datadir),
      )
    })
    .collect::<Vec<_>>();
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
fn merge(
  keydir: Arc<RwLock<KeyDir>>,
  inactive_files: Arc<RwLock<HashMap<FileID, InactiveFile>>>,
  idgen: Arc<Mutex<util::FileIDGen>>,
  config: Config,
  force_merge: bool,
) -> Result<()> {
  // Try to get a merge-lock on the active directory.
  // Go through the files in ascending order of ids
  let _merge_lockfile =
    locking::acquire(&config.datadir, BitrustOperation::Merge).chain_err(
      || format!("Failed to acquire merge lock in {:?}", config.datadir),
    )?;

  debug!("Acquired merge lockfile");
  let merge_files: Vec<InactiveFile> = {
    let files_to_merge = {
      let inactive_files = inactive_files
        .read()
        .expect("RLock on inactive_files for getting files to merge");
      get_files_for_merging(&inactive_files, &config)
    };
    debug!("Going to merge these files: {:?}", &files_to_merge);
    let mut merge_files = Vec::new(); // chan
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

  if !force_merge {
    if let Some(ref auto_merge_cfg) = &config.merge_config.auto_merge_config {
      if merge_files.len() < auto_merge_cfg.min_inactive_files as usize {
        info!(
          "Merge not proceeding because current inactive files: {},\
                 minimum inactive files for merge: {}",
          merge_files.len(),
          auto_merge_cfg.min_inactive_files
        );
        return Ok(());
      }
    }
  }

  let datadir = &config.datadir;
  let mut merge_output_file = {
    let merge_output_file_id = {
      idgen
        .lock()
        .expect("Lock idgen to generate merge output file id")
        .take_next()
    };
    let merge_output_file_name =
      util::FileKind::DataFile.path_from_id(merge_output_file_id, datadir);

    let merge_active_file = ActiveFile::new(merge_output_file_name.clone())
      .chain_err(|| "Failed to open a new merge file for writing")?;

    // We will also open an InactiveFile and make it known to the bitrust
    // state, since we will incrementally "port" keys from their current
    // files to the new merge file.
    let merge_inactive_file = InactiveFile::new(merge_output_file_name)
      .chain_err(|| "Failed to open merge file for reading")?;

    inactive_files
      .write()
      .expect("WLock inactive_files to insert merge output file")
      .insert(merge_inactive_file.id, merge_inactive_file);
    merge_active_file
  };
  let mut hint_file_writer = HintFileWriter::new(
    util::FileKind::HintFile.path_from_id(merge_output_file.id, datadir),
  )?;

  let mut all_merge_file_ids = HashSet::<FileID>::new();

  all_merge_file_ids.insert(merge_output_file.id);

  // Read records sequentially from the file.
  // If record.key does not exist in our keydir, move on.
  // If record.key exists, but the filename in the record does not match the
  // file we are merging
  for data_file in merge_files.into_iter() {
    let data_file_id = data_file.id;
    merge_one_file(
      keydir.clone(),
      data_file,
      &mut all_merge_file_ids,
      inactive_files.clone(),
      idgen.clone(),
      &config,
      &mut merge_output_file,
      &mut hint_file_writer,
    )?;
    let data_file_path =
      util::FileKind::DataFile.path_from_id(data_file_id, datadir);
    debug!("Now removing {:?} after merge", &data_file_path);
    fs::remove_file(&data_file_path).chain_err(|| {
      format!(
        "Could not remove file {:?} after merge to file id {:?}",
        &data_file_path, &merge_output_file.id
      )
    })?;
    let hint_file_path =
      util::FileKind::HintFile.path_from_id(data_file_id, datadir);
    if let Err(e) = fs::remove_file(&hint_file_path) {
      if e.kind() != std::io::ErrorKind::NotFound {
        warn!("Error removing hint file {:?}: {:?}", &hint_file_path, e);
      }
    }
    inactive_files
      .write()
      .expect("WLock inactive_files for removing merged file")
      .remove(&data_file_id);
  }
  Ok(())
}

impl<ClockT> Drop for BitRustState<ClockT> {
  fn drop(&mut self) {
    info!("Dropping BitRustState");
    if let Some(merge_stopchan) = &self.merge_stopchan {
      if let Err(e) = merge_stopchan.try_send(()) {
        error!("Error sending stop signal to merge thread: {:?}", e);
      }
    }
    if let Some(merge_thread) = self.merge_thread.take() {
      info!("Waiting for merge thread to finish");
      if let Err(e) = merge_thread.join() {
        error!("Error joining on merge thread: {:?}", e);
      }
    }
  }
}

pub mod test_utils {
  use super::*;
  extern crate simplelog;
  use simplelog::{CombinedLogger, LevelFilter, TermLogger};

  static ONCE: std::sync::Once = std::sync::Once::new();

  pub fn setup_logging() {
    ONCE.call_once(|| {
      if std::env::var("BITRUST_TEST_DEBUG_LOGS").is_ok() {
        CombinedLogger::init(vec![TermLogger::new(
          LevelFilter::Debug,
          simplelog::Config::default(),
        )
        .unwrap()])
        .chain_err(|| "Error setting up logging")
        .map(|_| ())
        .expect("Setup logging");
      }
    });
  }

  pub fn dump_datafile(path: PathBuf) -> Result<()> {
    debug!("Dumping file {:?}", &path);
    let mut f = InactiveFile::new(path.clone())?;
    while let Some(rec) = f.next_record()? {
      debug!("{:?}", &rec);
    }
    Ok(())
  }

  pub fn dump_all_datafiles<T>(state: &BitRustState<T>) -> Result<()> {
    debug!("Inactive files:");
    let inactive_files = state.inactive_files.read().unwrap();
    let mut file_ids = inactive_files.keys().collect::<Vec<_>>();
    file_ids.sort();
    for id in file_ids {
      dump_datafile(
        state
          .inactive_files
          .read()
          .unwrap()
          .get(&id)
          .expect("InactiveFile entry")
          .path
          .clone(),
      )?;
    }
    debug!("Active file:");
    dump_datafile(state.active_file.read().unwrap().path.clone())
  }
}

#[cfg(test)]
mod tests {

  extern crate simplelog;
  extern crate tempfile;

  use super::test_utils::*;
  use super::*;
  use std::ffi::OsStr;
  use test::Bencher;
  use util::{LogicalClock, SerialLogicalClock};

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
    setup_logging();
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
  fn test_merge_file_rotation() {
    setup_logging();
    let sz_limit = 50;
    let data_dir = tempfile::tempdir().unwrap();
    let cfg = Config {
      datadir: data_dir.as_ref().to_path_buf(),
      file_size_soft_limit_bytes: sz_limit,
      merge_config: MergeConfig::default(),
    };
    let br = BitRust::open(cfg, SerialLogicalClock::new(0)).unwrap();
    let mut i = 0;
    while br.state.active_file_id() != 2 {
      br.put(
        format!("foo_{:02}", i).into_bytes(),
        format!("foo_{:02}", i).into_bytes(),
      )
      .expect(&format!("Put foo => foo_{:02}", i));
      i += 1;
    }
    br.merge().expect("Merge");
    let inactive_files = br.state.inactive_files.read().unwrap();
    assert!(
      inactive_files.len() == 2,
      "Expected 2 merge files after merge, but {} were found",
      inactive_files.len()
    );
    assert!(
      inactive_files.contains_key(&3),
      "Expected merge file with id 3"
    );
    assert!(
      inactive_files.contains_key(&4),
      "Expected merge file with id 4"
    );
  }

  #[test]
  fn test_merge_after_merge() {
    setup_logging();
    // This tests the scenario where a file with a lower id contains later
    // writes, and asserts that these are not lost.
    // 1. Write file 0 just to overflow, it is now inactive, new active file is
    //    id 1.
    // 2. Write file 1 just to overflow, it is now inactive, new active file is
    //    id 2.
    // 3. Merge 0 and 1 into id 3.
    // 4. Write file id 2 to overflow, it is now inactive, new active file is id
    //    4.
    // 5. Call merge and assert.
    let sz_limit = 50;
    let data_dir = tempfile::tempdir().unwrap();
    let cfg = Config {
      datadir: data_dir.as_ref().to_path_buf(),
      file_size_soft_limit_bytes: sz_limit,
      merge_config: MergeConfig::default(),
    };
    let br = BitRust::open(cfg, SerialLogicalClock::new(0)).unwrap();
    let mut i = 0;
    while br.state.active_file_id() != 2 {
      br.put(b"foo".to_vec(), format!("foo_{:02}", i).into_bytes())
        .expect(&format!("Put foo => foo_{:02}", i));
      i += 1;
    }
    debug!("Before first merge:");
    dump_all_datafiles(&br.state).expect("Dump datafiles");
    br.merge().expect("First merge");
    debug!("After first merge");
    dump_all_datafiles(&br.state).expect("Dump datafiles");
    assert!(
      br.state.active_file_id() == 2,
      "Expected active file id to be 2, but found {}",
      br.state.active_file_id()
    );
    let mut last_val = String::from("");
    while br.state.active_file_id() == 2 {
      last_val = format!("foo_{:02}", i);
      br.put(b"foo".to_vec(), last_val.clone().into_bytes())
        .expect(&format!("Put foo => foo_{:02}", i));
      i += 1;
    }
    debug!("New active file id: {}", br.state.active_file_id());
    debug!("After filling, before second merge:");
    dump_all_datafiles(&br.state).expect("Dump datafiles");
    br.merge().expect("Second merge");
    debug!("After second merge:");
    dump_all_datafiles(&br.state).expect("Dump datafiles");
    let val = br.get(b"foo").expect("Get foo").expect("Some value");
    assert!(
      val == last_val.clone().into_bytes(),
      "Expected: {}, Got: {}",
      last_val,
      std::str::from_utf8(&val).expect("utf-8 string")
    );
  }

  #[test]
  fn test_merge() {
    setup_logging();
    let sz_limit = 100;
    let data_dir = tempfile::tempdir().unwrap();
    let cfg = Config {
      datadir: data_dir.as_ref().to_path_buf(),
      file_size_soft_limit_bytes: sz_limit,
      merge_config: MergeConfig::default(),
    };
    let br = BitRust::open(cfg, MockClock::new()).unwrap();
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
    {
      let inactive_files = br.state.inactive_files.read().unwrap();
      debug!(
        "After merge, inactive files={:?}, keydir={:?}",
        inactive_files.keys().cloned().collect::<Vec<_>>(),
        &br.state.keydir,
      );
    }
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
    setup_logging();
    let t = tempfile::tempdir().unwrap();
    {
      let mut hint_record = bitrust_pb::HintFileRecord::new();
      hint_record.set_timestamp(42);
      hint_record.set_key(b"foo".to_vec());
      hint_record.set_record_offset(9);
      hint_record.set_record_size(103);
      let mut rw = HintFileWriter::new(t.as_ref().join("0"))
        .expect("Create HintFileWriter");
      rw.append_record(&hint_record).expect("Write hint record");
    }
    let mut keydir = KeyDir::new();
    {
      let mut r = HintFileReader::new(t.as_ref().join("0"))
        .expect("Create HintFileReader");
      read_hint_file_into_keydir(99, &mut r, &mut keydir)
        .expect("read_hint_file_into_keydir");
      assert!(r.next_record().expect("read record at eof").is_none());
    }
    assert!(
      keydir.entries.len() == 1,
      "Expected one entry, got {}",
      keydir.entries.len()
    );
    let e = keydir.get(b"foo").expect("Entry with key foo");
    let expected = KeyDirEntry::new(99, 103, 9, 42);
    assert!(
      *e == expected,
      "Expected entry {:?}, found {:?}",
      expected,
      e
    );
  }

  #[test]
  fn test_read_datafile_into_keydir() {
    setup_logging();
    let t = tempfile::tempdir().unwrap();
    let mut f =
      ActiveFile::new(t.as_ref().join("0")).expect("Create ActiveFile");
    let mut data_records = Vec::new();
    let mut offsets_and_sizes = Vec::new();
    let mut data_record = bitrust_pb::BitRustDataRecord::new();
    data_record.set_timestamp(42);
    data_record.set_key(b"foo".to_vec());
    data_record.set_value(b"bar".to_vec());
    let (offset, size) = f.append_record(&data_record).expect("Append record");
    offsets_and_sizes.push((offset, size));
    data_records.push(data_record);
    let mut f: InactiveFile = f.into();
    let mut keydir = KeyDir::new();
    read_data_file_into_keydir(0, &mut f, &mut keydir)
      .expect("Read datafile into keydir");
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
  fn test_bitrust_read_existing_datadir() {
    setup_logging();
    let sz_limit = 1_000;
    let data_dir = tempfile::tempdir().unwrap();
    {
      let cfg = Config {
        datadir: data_dir.as_ref().to_path_buf(),
        file_size_soft_limit_bytes: sz_limit,
        merge_config: MergeConfig::default(),
      };
      let br = BitRust::open(cfg, SerialLogicalClock::new(0)).unwrap();
      br.put(b"foo".to_vec(), b"bar".to_vec())
        .expect("put to bitrust");
    }
    let cfg = Config {
      datadir: data_dir.as_ref().to_path_buf(),
      file_size_soft_limit_bytes: sz_limit,
      merge_config: MergeConfig::default(),
    };
    let br = BitRust::open(cfg, SerialLogicalClock::new(0)).unwrap();
    let read_val = br.get(b"foo").expect("Get key foo from bitrust");
    assert!(
      Some(b"bar".to_vec()) == read_val,
      "Expected Some({:?}), got {:?}",
      b"bar",
      &read_val
    );
  }

  #[test]
  fn test_bitrust_state_evolution() {
    setup_logging();
    let sz_limit = 1_000;
    let data_dir = tempfile::tempdir().unwrap();
    let cfg = Config {
      datadir: data_dir.as_ref().to_path_buf(),
      file_size_soft_limit_bytes: sz_limit,
      merge_config: MergeConfig::default(),
    };
    let br = BitRust::open(cfg, MockClock::new()).unwrap();
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
    assert!(
      persisted_active_file_name == br.state.active_file.read().unwrap().path
    );
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
    setup_logging();
    // we expect the active file to be sealed once it reaches 1kB
    let sz_limit = 1_000;
    let data_dir = tempfile::tempdir().unwrap();
    let cfg = Config {
      datadir: data_dir.as_ref().to_path_buf(),
      file_size_soft_limit_bytes: sz_limit,
      merge_config: MergeConfig::default(),
    };
    let br = BitRust::open(cfg, MockClock::new()).unwrap();
    assert!(
      br.state.active_file.read().unwrap().path
        == data_dir.as_ref().join("0.data")
    );
    assert!(br.state.keydir.read().unwrap().entries.len() == 0);
    assert!(br.state.inactive_files.read().unwrap().len() == 0);
  }

  #[test]
  fn test_get_files_for_merging() {
    setup_logging();
    let sz_limit = 1024; // bytes
    let data_dir = tempfile::tempdir().unwrap();
    let cfg = Config {
      datadir: data_dir.as_ref().to_path_buf(),
      file_size_soft_limit_bytes: sz_limit,
      merge_config: MergeConfig::default(),
    };
    let br = BitRust::open(cfg.clone(), MockClock::new()).unwrap();
    let mut clock = MockClock::new();
    clock.set_next(42);
    let mut proto_record = bitrust_pb::BitRustDataRecord::new();
    proto_record.set_timestamp(clock.tick());
    proto_record.set_key(b"somekey".to_vec());
    proto_record.set_value(b"somevalue".to_vec());
    let entry_sz = storage::payload_size_for_record(&proto_record);
    let total_entries = 256;
    let entries_per_file = sz_limit / (entry_sz as i64);
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
    let files_to_merge = get_files_for_merging(
      &br.state.inactive_files.read().expect("inactive_files"),
      &cfg,
    );
    assert!(
      files_to_merge.len() == num_merge_files_expected,
      "Total files created={}, merge files expected={}, merge files actual={},\
      record size={}, total entries={}, entries_per_file={}",
      total_files_needed,
      num_merge_files_expected,
      files_to_merge.len(),
      entry_sz,
      total_entries,
      entries_per_file
    );
    for fid in files_to_merge {
      assert!(
        fid.0 != br.state.active_file_id(),
        "Merge file coverage includes active file id {}",
        br.state.active_file_id()
      );
    }
  }

  #[test]
  fn test_overflow_puts() {
    setup_logging();
    // Test that when we overflow into multiple data files, the store still
    // returns expected values.
    let sz_limit = 100; // small size limit so we always overflow.
    let data_dir = tempfile::tempdir().unwrap();
    let cfg = Config {
      datadir: data_dir.as_ref().to_path_buf(),
      file_size_soft_limit_bytes: sz_limit,
      merge_config: MergeConfig::default(),
    };
    let br = BitRust::open(cfg, MockClock::new()).unwrap();
    let key_vals = (0..1000)
      .map(|_| {
        (
          util::rand_str_with_rand_size().as_bytes().to_vec(),
          util::rand_str_with_rand_size().as_bytes().to_vec(),
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
    setup_logging();
    let data_dir = tempfile::tempdir().unwrap();
    let cfg = Config {
      datadir: data_dir.as_ref().to_path_buf(),
      file_size_soft_limit_bytes: DEFAULT_FILE_SIZE_SOFT_LIMIT_BYTES,
      merge_config: MergeConfig::default(),
    };
    let br = BitRustState::new(cfg, MockClock::new()).unwrap();
    br.put(b"foo".to_vec(), b"bar".to_vec()).unwrap();
    let r = br.get(b"foo").unwrap().unwrap();
    assert!(r == b"bar");
  }

  #[test]
  fn test_locking_of_data_dir() {
    setup_logging();
    let data_dir = tempfile::tempdir().unwrap();
    let cfg = Config {
      datadir: data_dir.as_ref().to_path_buf(),
      file_size_soft_limit_bytes: DEFAULT_FILE_SIZE_SOFT_LIMIT_BYTES,
      merge_config: MergeConfig::default(),
    };
    let _br = BitRustState::new(cfg.clone(), MockClock::new()).unwrap();
    let another_br = BitRustState::new(cfg, MockClock::new());
    assert!(another_br.is_err());
  }

  #[test]
  fn test_deletion() {
    setup_logging();
    let data_dir = tempfile::tempdir().unwrap();
    let cfg = Config {
      datadir: data_dir.as_ref().to_path_buf(),
      file_size_soft_limit_bytes: DEFAULT_FILE_SIZE_SOFT_LIMIT_BYTES,
      merge_config: MergeConfig::default(),
    };
    let br = BitRustState::new(cfg, MockClock::new()).unwrap();
    br.put(b"foo".to_vec(), b"bar".to_vec()).unwrap();
    assert!(br.get(b"foo").unwrap().unwrap() == b"bar");
    br.delete(b"foo").unwrap();
    assert!(br.get(b"foo").unwrap().is_none());
  }

  #[bench]
  fn bench_put(b: &mut Bencher) {
    setup_logging();
    let data_dir = tempfile::tempdir().unwrap();
    let cfg = Config {
      datadir: data_dir.as_ref().to_path_buf(),
      file_size_soft_limit_bytes: DEFAULT_FILE_SIZE_SOFT_LIMIT_BYTES,
      merge_config: MergeConfig::default(),
    };
    let br = BitRust::open(cfg, MockClock::new()).unwrap();
    let key = util::rand_str_with_rand_size().as_bytes().to_vec();
    let val = util::rand_str_with_rand_size().as_bytes().to_vec();
    b.iter(move || {
      br.put(key.clone(), val.clone()).unwrap();
    });
  }
}
