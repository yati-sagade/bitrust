#![feature(test)]
extern crate byteorder;
extern crate bytes;
extern crate crc;
extern crate test;
extern crate rand;

#[macro_use]
extern crate log;
extern crate simplelog;
extern crate regex;
#[macro_use]
extern crate lazy_static;

use std::thread;

pub mod util;
mod locking;
mod lockfile;
mod config;
mod common;


use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH, Instant};
use std::path::{PathBuf, Path};
use std::io;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom};
use std::process;
use std::mem;
use std::sync::{RwLock, Arc};
use std::sync::atomic;

use bytes::{BytesMut, BufMut, IntoBuf, Buf};
use byteorder::{ReadBytesExt, BigEndian};

pub use config::*;
pub use common::{FileID, BITRUST_TOMBSTONE_STR, BitrustOperation};

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

type FileMap = HashMap<FileID, PathBuf>;

// The keydir is the main in-memory lookup table that bitcask uses, and
// KeyDirEntry is an entry in that table (which is keyed by a String key, see
// below). The KeyDirEntry for a given key tells us which file we need to look
// into, along with the offset into that file, and the size of the record we
// must read out.
#[derive(Debug)]
pub struct KeyDirEntry {
    /// Id of the file where this key lives
    file_id: FileID,
    /// Size in bytes of the record
    record_size: u16,
    /// Byte offset of the data record in the file
    record_offset: u64,
    /// local timestamp when this entry was written
    timestamp: u64,
}

impl KeyDirEntry {
    pub fn new(
        file_id: FileID,
        record_size: u16,
        record_offset: u64,
        timestamp: u64,
    ) -> KeyDirEntry {
        KeyDirEntry {
            file_id,
            record_size,
            record_offset,
            timestamp,
        }
    }
}

// The keydir is the main in memory data structure in bitcask. All of the
// keyspace (+ some constant size metadata per key) must fit in memory.
//
// During reads, keydirs are consulted to find where on disk to find the record.
// Keydirs are updated after each write.
#[derive(Debug)]
pub struct KeyDir {
    /// mapping from a key to its keydir entry
    entries: HashMap<String, KeyDirEntry>,

    /// mapping from file id to its path on disk
    file_map: FileMap,
}

impl KeyDir {
    fn new() -> KeyDir {
        KeyDir {
            entries: HashMap::new(),
            file_map: FileMap::new(),
        }
    }

    fn insert(&mut self, key: String, entry: KeyDirEntry) {
        self.entries.insert(key, entry);
    }

    fn get<'a>(&'a self, key: &str) -> Option<&'a KeyDirEntry> {
        self.entries.get(key)
    }
}

// struct ActiveFile is just a collection of things related to the active file
// -- the current log of writes. We hold two file handles to this file, one for
// writing, which happens only by appending, and one handle for reading, which
// we can seek freely.
#[derive(Debug)]
struct ActiveFile {
    write_handle: File,
    read_handle: File,
    pub name: PathBuf,
    pub id: FileID,
}

impl ActiveFile {
    fn new(path: PathBuf) -> io::Result<ActiveFile> {

        let mut write_handle = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&path)?;

        let size_in_bytes = write_handle.seek(SeekFrom::End(0))? as usize;

        let read_handle = OpenOptions::new().read(true).create(false).open(&path)?;

        let file_id = util::file_id_from_path(&path);

        let active_file = ActiveFile {
            write_handle,
            read_handle,
            name: path,
            id: file_id,
        };

        debug!("Initialized active file (size: {} bytes)", size_in_bytes);

        Ok(active_file)
    }

    /// Returns the offset of the first byte of written record in the file.
    pub fn append(&mut self, bytes: &[u8]) -> io::Result<u64> {
        let offset = self.tell()?;
        debug!("Appended {} bytes to at offset {}", bytes.len(), offset);
        self.write_handle.write_all(bytes).map(|_| offset)
    }

    fn tell(&mut self) -> io::Result<u64> {
        self.write_handle.seek(SeekFrom::Current(0))
    }
}

trait ReadableFile {
    fn file<'a>(&'a mut self) -> io::Result<&'a mut File>;

    fn read_exact(&mut self, offset_from_start: u64, bytes: &mut [u8]) -> io::Result<()> {
        let fp = self.file()?;
        fp.seek(SeekFrom::Start(offset_from_start))?;
        fp.read_exact(bytes)
    }
}

impl ReadableFile for ActiveFile {
    fn file<'a>(&'a mut self) -> io::Result<&'a mut File> {
        Ok(&mut self.read_handle)
    }
}

#[derive(Debug)]
struct InactiveFile {
    read_handle: File,
    pub name: PathBuf,
    pub id: FileID,
}

impl InactiveFile {
    fn new(path: PathBuf) -> io::Result<InactiveFile> {
        let read_handle = OpenOptions::new().read(true).create(false).open(&path)?;
        let id = util::file_id_from_path(&path);
        Ok(InactiveFile {
            read_handle: read_handle,
            name: path,
            id: id,
        })
    }
}

impl ReadableFile for InactiveFile {
    fn file<'a>(&'a mut self) -> io::Result<&'a mut File> {
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
pub struct BitRustState {
    keydir: KeyDir,
    active_file: ActiveFile,
    inactive_files: HashMap<FileID, InactiveFile>,
    lockfile: lockfile::LockFile,
    // A ghost lock object, used to hold R/W locks on the entire bitrust
    // instance.
    rwlock: Arc<RwLock<()>>,

    config: Config,
    data_file_id_gen: util::FileIDGen,
}

impl BitRustState {
    pub fn new(config: Config) -> io::Result<BitRustState> {
        info!("Making a bitrust");
        fs::create_dir_all(config.datadir())?;

        let data_dir = config.datadir().to_path_buf();

        // If the following fails, all it means is that we have a file at our
        // lockfile location that we did not create. We don't care at this
        // point why it exists, and just bail out.
        //
        // This lock will be released (and the lockfile removed) when the
        // returned object goes out of scope. Since we move it into the
        // returned BitRustState, this means the lock lives as long as the returned
        // BitRustState lives.
        let lockfile = locking::acquire(&data_dir, BitrustOperation::Write)?;

        debug!("Obtained data directory lock");

        // Get the names of the data and hint files. Because these are named
        // like ${file_id}.data and ${file_id}.hint respectively, we can group
        // together the data and hint files for a given file_id together. A
        // hint file is optional, but cannot exist without a corresponding data
        // file (because hint files just contain pointers into the respective
        // data files)
        let data_and_hint_files = util::get_data_and_hint_files(&data_dir)?;

        debug!(
            "Got {} data/hint file entries from {:?}",
            data_and_hint_files.len(),
            &data_dir
        );

        if data_and_hint_files.len() == 0 {
            // We are starting from scratch, write the name of the active file
            // as 0.data.
            let ptr_path = active_file_pointer_path(&data_dir);
            debug!(
                "Starting in an empty data directory, writing {:?}",
                &ptr_path
            );
            util::write_to_file(
                &ptr_path,
                data_dir.join("0.data").to_str().expect(
                    "Garbled initial data file name?",
                ),
            )?;
        }

        // We should probably build the keydir and the `active_file` and
        // `inactive_file` fields together, but it is just simpler to first
        // build the keydir and then do another pass to build the other fields.
        let keydir = build_keydir(data_and_hint_files.clone())?;

        let active_file_name = active_file_path(&data_dir)?;
        debug!("Using active file {:?}", &active_file_name);

        let active_file = ActiveFile::new(active_file_name)?;

        let mut inactive_files = HashMap::new();
        for (file_id, (data_file, _)) in data_and_hint_files.into_iter() {
            let data_file = data_file.expect("data file path is none!");
            if file_id != active_file.id {
                inactive_files.insert(file_id, InactiveFile::new(data_file)?);
            }
        }

        let active_file_id = active_file.id;

        let bitrust = BitRustState {
            keydir,
            config: config,
            inactive_files: inactive_files,
            active_file: active_file,
            lockfile,
            rwlock: Arc::new(RwLock::new(())),
            data_file_id_gen: util::FileIDGen::new(active_file_id + 1)
        };

        Ok(bitrust)
    }

    pub fn active_file_size(&mut self) -> io::Result<u64> {
        let _lock = self.rwlock.write().unwrap();
        self.active_file.tell()
    }

    /// Returns a vector of `(file_id, path_to_file)` tuples, sorted ascending
    /// by `file_id`.
    fn get_files_for_merging(&self) -> Vec<(FileID, PathBuf)> {
        let mut files_to_merge: Vec<(FileID, PathBuf)> = self.inactive_files
            .keys()
            .cloned()
            .map(|id| {
                let file_path = self.keydir.file_map.get(&id).expect(&format!(
                    "Could not find file id {} in the map! This is bad: {:?}",
                    id,
                    &self.keydir.file_map,
                ));
                (id, file_path.clone())
            })
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
    pub fn merge(&mut self) -> io::Result<()> {
        // Try to get a merge-lock on the active directory.
        // Go through the files in ascending order of ids
        let merge_lockfile = locking::acquire(&self.config.datadir(),
                                              BitrustOperation::Merge)?;

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
                let merge_file = InactiveFile::new(file_path)?;
                merge_files.push(merge_file);
            }
            merge_files
        };

        // Read records sequentially from the file.
        // If record.key does not exist in our keydir, move on.
        // If record.key exists, but


        Ok(())
    }

    pub fn put(&mut self, key: String, value: String) -> io::Result<()> {

        let _lock = self.rwlock.write().unwrap();

        let key_bytes = key.clone().into_bytes();
        let val_bytes = value.into_bytes();

        let payload_size = mem::size_of::<u32>()  // checksum
                         + mem::size_of::<u64>()  // timestamp
                         + mem::size_of::<u16>()  // key size
                         + mem::size_of::<u16>()  // value size
                         + key_bytes.len()        // key payload
                         + val_bytes.len()        // value payload
                         ;

        let mut payload = BytesMut::with_capacity(payload_size);

        // We split to after 32 bits so we can write the data first, and then
        // computer the checksum of this data to put in the initial 32 bits.
        let mut payload_head = payload.split_to(4);

        let timestamp_now = {
            let dur_since_unix_epoch = SystemTime::now().duration_since(UNIX_EPOCH).expect(
                "Time drift!",
            );
            dur_since_unix_epoch.as_secs()
        };
        payload.put_u64_be(timestamp_now);
        payload.put_u16_be(key_bytes.len() as u16);
        payload.put_u16_be(val_bytes.len() as u16);
        payload.put(key_bytes);
        payload.put(val_bytes);

        let checksum = util::checksum_crc32(&payload);

        payload_head.put_u32_be(checksum);

        // Now payload_head contains all of the record we want to write out,(
        // including the checksum.
        payload_head.unsplit(payload);

        let record_offset = self.active_file.append(&payload_head)?;

        self.keydir.insert(
            key,
            KeyDirEntry::new(
                self.active_file.id,
                payload_head.len() as u16,
                record_offset,
                timestamp_now,
            ),
        );

        debug!(
            "After this write, active file is {} bytes",
            self.active_file.tell()?
        );

        // Borrowing the field directly is needed here since self cannot be
        // borrowed mutably because of the rwlock in scope, which has an
        // immutable borrow on self until the function ends.
        //
        // This is also why maybe_seal_active_data() is a function accepting
        // our fields mutably rather than a method on &mut self.
        let inactive_files = &mut self.inactive_files;
        let keydir = &mut self.keydir;
        let active_file = &mut self.active_file;
        let config = &self.config;

        maybe_seal_active_data(active_file, config)?
            .map(|inactive_file| {

                add_data_file_to_keydir_filemap(keydir,
                                                active_file.id,
                                                &active_file.name);

                inactive_files.insert(inactive_file.id, inactive_file)
            });

        Ok(())
    }

    pub fn get(&mut self, key: &str) -> io::Result<Option<String>> {

        let _lock = self.rwlock.read().unwrap();

        let entry = self.keydir.get(key);
        if let Some(entry) = entry {
            // Read required bytes
            let mut read_buf = vec![0u8; entry.record_size as usize];

            debug!(
                "Asking to read {} bytes from offset {}",
                read_buf.len(),
                entry.record_offset
            );

            if entry.file_id == self.active_file.id {
                debug!("Fetching from active file (id {})", entry.file_id);
                self.active_file.read_exact(
                    entry.record_offset,
                    &mut read_buf,
                )?;
            } else {
                // if the key is not present in the store, we won't reach here
                // (`entry` would be None). Having an entry pointing to a file
                // we don't know about is bad.
                debug!("Fetching from inactive file id {}", entry.file_id);
                let mut file = self.inactive_files
                                   .get_mut(&entry.file_id)
                                   .unwrap_or_else(|| panic!(
"Got a request for inactive file id {}, but it was not loaded, this is really bad!",
                                        entry.file_id
                                    ));
                file.read_exact(entry.record_offset, &mut read_buf)?;
            }

            debug!("Read {} bytes", read_buf.len());

            // We use a BytesMut for the easy extractor methods for which
            // we'd otherwise have to use mem::transmute or something directly.
            let mut buf = BytesMut::from(read_buf).into_buf();

            let checksum = buf.get_u32_be();
            {
                let buf_bytes = Buf::bytes(&buf);
                let computed_checksum = util::checksum_crc32(buf_bytes);
                if computed_checksum != checksum {
                    panic!(
                        "Data integrity check failed! record checksum={}, computed checksum={}",
                        checksum,
                        computed_checksum
                    );
                }
            }
            let _timestamp = buf.get_u64_be();
            let key_size = buf.get_u16_be();
            let _val_size = buf.get_u16_be();

            // We don't care about the key, so just move ahead
            buf.advance(key_size as usize);

            // At this point, all we have in buf is the value bytes
            let val = String::from_utf8(Buf::bytes(&buf).to_vec()).unwrap();

            // Currently deletion and the application writing the tombstone
            // value directly are indistinguishable.
            if val.as_str() == BITRUST_TOMBSTONE_STR {
                Ok(None)
            } else {
                Ok(Some(val))
            }
        } else {
            Ok(None)
        }
    }

    pub fn delete(&mut self, key: &str) -> io::Result<()> {
        self.put(key.to_string(), String::from(BITRUST_TOMBSTONE_STR))
    }

    pub fn keys(&self) -> Vec<String> {
        let _lock = self.rwlock.read().unwrap();
        self.keydir.entries.keys().cloned().collect()
    }
}

fn should_seal_active_data(active_file: &mut ActiveFile, config: &Config) -> io::Result<bool> {
    active_file.tell().map(
        |size| size > config.max_file_fize_bytes(),
    )
}

fn update_active_file_id(id: FileID, config: &Config) -> io::Result<PathBuf> {
    let new_active_file_name = format!("{}.data", id);
    let data_dir = config.datadir();
    let new_active_file_path = data_dir.join(&new_active_file_name);
    let ptr_path = active_file_pointer_path(data_dir);

    util::write_to_file(
        &ptr_path,
        new_active_file_path.to_str().expect(
            "Garbled data file name",
        ),
    )?;
    Ok(new_active_file_path)
}

// This fn is not thread safe, and assumes that we have a write lock
// on the state.
fn maybe_seal_active_data(
    active_file: &mut ActiveFile,
    config: &Config,
) -> io::Result<Option<InactiveFile>> {
    // Ultimately we want to close the current active file and start
    // writing to a new one. The pointers into the old file should still
    // be active.
    if should_seal_active_data(active_file, config)? {
        debug!("Active file is too big, sealing");
        let old_active_file = {
            // XXX: ensure there are no conflicts
            let new_active_file_id = active_file.id + 1;
            let new_active_file_path = update_active_file_id(new_active_file_id, config)?;
            debug!("New active file is {:?}", &new_active_file_path);
            let mut new_active_file = ActiveFile::new(new_active_file_path)?;
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

fn build_keydir(dd_contents: util::DataDirContents) -> io::Result<KeyDir> {
    info!("Making keydir");

    // First sort the data and hint files by file_id ascending so we process
    // the oldest entries first so in the end we have the latest persisted
    // values in the keydir.
    let mut dd_entries = dd_contents.into_iter().collect::<Vec<_>>();
    dd_entries.sort_by(|v1, v2| v1.0.cmp(&v2.0));

    let mut keydir = KeyDir::new();

    for (file_id, (data_file, hint_file)) in dd_entries {
        // If we have the hint file, we prefer reading it since it is almost
        // a direct on-disk representation of the keydir.
        if let Some(hint_file) = hint_file {
            let hint_file_handle = File::open(&hint_file)?;

            let data_file = data_file
                .expect("Hint file {:?} present, but no corresponding data file");

            read_hint_file_into_keydir(file_id,
                                       hint_file,
                                       data_file,
                                       &mut keydir)?;
        } else {
            let data_file = data_file.unwrap_or_else(|| {
                panic!(
                    "Expected datafile for file id {} not found when building keydir",
                    file_id
                )
            });
            let data_file_handle = File::open(&data_file)?;
            read_data_file_into_keydir(file_id,
                                       data_file,
                                       &mut keydir)?;
        }
    }
    Ok(keydir)
}

fn add_data_file_to_keydir_filemap<P>(keydir: &mut KeyDir,
                                      file_id: FileID,
                                      data_file_path: P)
where P: AsRef<Path>
{
    if let Some(ref existing_data_file_path) = keydir.file_map.get(&file_id) {
        panic!("Unexpected path {:?} found for file id {} in the filemap",
               existing_data_file_path,
               file_id);
    }
    keydir.file_map.insert(file_id, data_file_path.as_ref().to_path_buf());
}

fn read_hint_file_into_keydir<P>(
    file_id: FileID,
    hint_file_path: P,
    data_file_path: P,
    keydir: &mut KeyDir,
) -> io::Result<()>
where
    P: AsRef<Path>,
{


    add_data_file_to_keydir_filemap(keydir, file_id, data_file_path);

    let mut hint_file_handle = File::open(hint_file_path.as_ref())?;

    // We receive a Some(_) when EOF hasn't been reached.
    while let Some(_) = read_hint_file_record(file_id,
                                              &mut hint_file_handle,
                                              keydir)?
    { }

    Ok(())
}


// Return an Err(_) when an io error happens
//
// Return an Ok(Some(())) when we did not hit EOF when trying to read a record
// (i.e., more might come)
//
// Return an Ok(None) when we encountered an EOF when reading the first 8
// bytes of the record (means end of stream).
//
// There is a possibility that we read a part of the 8 bytes that we wanted to
// read, and that might indicate a subtle corruption. We handle that by reading
// the first field slightly more laboriously.
fn read_hint_file_record<R>(
    file_id: FileID,
    hint_file: R,
    keydir: &mut KeyDir,
) -> io::Result<Option<()>>
where
    R: Read,
{

    // XXX: Somehow using BufReader here does not work, investigate.
    let mut reader = hint_file;

    // Record format
    // | tstamp (64) | ksz (16) | record_size (16) | record_offset (32) | key ($ksz) |

    // To signal end of stream, we try to read the first field, the timestamp,
    // and return None if we cannot read anything. However, if we can read a
    // part of that field, it could indicate a subtle corruption, so we panic.

    let timestamp = {
        let mut timestamp_bytes = [0u8; 8];
        let mut read_so_far = 0;
        while read_so_far != 8 {
            let read_bytes = reader.read(&mut timestamp_bytes[read_so_far..])?;
            if read_bytes == 0 {
                if read_so_far == 0 {
                    return Ok(None);
                }
                panic!(
                    "Expected to read 8 bytes but could read only {}",
                    read_so_far
                );
            } else {
                read_so_far += read_bytes;
            }
        }
        let mut timestamp_slice = &timestamp_bytes[..];
        timestamp_slice.read_u64::<BigEndian>()?
    };

    let key_size = reader.read_u16::<BigEndian>()?;
    let val_size = reader.read_u16::<BigEndian>()?;
    let val_pos = reader.read_u64::<BigEndian>()?;

    let key = {
        let mut key_bytes = vec![0u8; key_size as usize];
        reader.read_exact(&mut key_bytes)?;
        String::from_utf8(key_bytes.clone()).unwrap_or_else(|_| {
            panic!("Could not read key as utf8. Bytes={:?}", key_bytes)
        })
    };

    let entry = KeyDirEntry::new(file_id, val_size, val_pos, timestamp);
    keydir.insert(key, entry);

    Ok(Some(()))
}

fn read_data_file_into_keydir<P>(
    file_id: FileID,
    data_file_path: P,
    keydir: &mut KeyDir,
) -> io::Result<()>
where
    P: AsRef<Path>,
{
    let mut data_file_handle = File::open(&data_file_path)?;

    add_data_file_to_keydir_filemap(keydir, file_id, data_file_path);

    let mut offset = 0;
    while let Some(new_offset) = read_data_file_record_into_keydir(
        file_id,
        &mut data_file_handle,
        keydir,
        offset,
    )?
    {
        offset = new_offset;
    }
    Ok(())
}

fn read_data_file_record_into_keydir<R>(
    file_id: FileID,
    data_file: R,
    keydir: &mut KeyDir,
    offset: u64,
) -> io::Result<Option<u64>>
where
    R: Read,
{

    let mut reader = data_file;
    let mut new_offset = offset;

    // Record format (top-to-bottom contiguous sequence of bytes)
    //
    // | checksum (4 bytes)      |
    // | tstamp   (8 bytes)      |
    // | ksz      (2 bytes)      |
    // | valsz    (2 bytes)      |
    // | key      ($ksz bytes)   |
    // | val      ($valsz bytes) |

    let checksum = {
        let mut checksum_bytes = [0u8; 4];
        let mut read_so_far = 0;
        while read_so_far != 4 {
            let read_bytes = reader.read(&mut checksum_bytes[read_so_far..])?;
            if read_bytes == 0 {
                if read_so_far == 0 {
                    return Ok(None);
                }
                panic!(
                    "Expected to read 4 bytes of checksum but could read only {}",
                    read_so_far
                );
            } else {
                read_so_far += read_bytes;
            }
        }
        let mut checksum_slice = &checksum_bytes[..];
        checksum_slice.read_u32::<BigEndian>()?
    };
    // XXX: Check data integrity with the checksum
    new_offset += util::sizeof_val(&checksum) as u64;

    let timestamp = reader.read_u64::<BigEndian>()?;
    new_offset += util::sizeof_val(&timestamp) as u64;

    let key_size = reader.read_u16::<BigEndian>()?;
    new_offset += util::sizeof_val(&key_size) as u64;

    let val_size = reader.read_u16::<BigEndian>()?;
    new_offset += util::sizeof_val(&val_size) as u64;

    let key = {
        let mut key_bytes = vec![0u8; key_size as usize];
        reader.read_exact(&mut key_bytes)?;
        String::from_utf8(key_bytes).unwrap()
    };
    new_offset += u64::from(key_size);

    {
        let mut val_bytes = vec![0u8; val_size as usize];
        // Don't actually need the value, to advance the pointer.
        reader.read_exact(&mut val_bytes)?;
    }
    new_offset += u64::from(val_size);

    let entry = KeyDirEntry::new(file_id, (new_offset - offset) as u16, offset, timestamp);
    keydir.insert(key, entry);

    Ok(Some(new_offset))
}


pub struct BitRust {
    state: BitRustState,
    running: Arc<atomic::AtomicBool>,
}

impl Drop for BitRust {
    fn drop(&mut self) {
        self.running.store(false, atomic::Ordering::SeqCst);
    }
}

impl BitRust {
    pub fn open(config: Config) -> io::Result<BitRust> {
        let state = BitRustState::new(config)?;
        let running = Arc::new(atomic::AtomicBool::new(true));

        Ok(BitRust { state, running })
    }

    pub fn get(&mut self, key: &str) -> io::Result<Option<String>> {
        debug_timeit!("get" => {
            self.state.get(key)
        })
    }

    pub fn put(&mut self, key: String, value: String) -> io::Result<()> {
        debug_timeit!("put" => {
            self.state.put(key, value)
        })
    }

    pub fn delete(&mut self, key: &str) -> io::Result<()> {
        debug_timeit!("delete" => {
            self.state.delete(key)
        })
    }

    pub fn keys(&self) -> Vec<String> {
        debug_timeit!("keys" => {
            self.state.keys()
        })
    }

    pub fn merge(&mut self) -> io::Result<()> {
        debug_timeit!("merge" => {
            self.state.merge()
        })
    }
}

fn active_file_pointer_path<P: AsRef<Path>>(data_dir: P) -> PathBuf {
    data_dir.as_ref().join(".activefile")
}

fn active_file_path<P: AsRef<Path>>(data_dir: P) -> io::Result<PathBuf> {
    let ptr_path = active_file_pointer_path(data_dir);
    let mut fp = File::open(ptr_path)?;
    let mut buf = String::new();
    fp.read_to_string(&mut buf)?;
    Ok(PathBuf::from(buf))
}

#[cfg(test)]
mod tests {

    extern crate tempfile;

    use test::Bencher;
    use std::io::Cursor;
    use super::*;
    use std::ffi::OsStr;

    // The following two tests statically include reference data from ../aux,
    // along with "guide" files, which tell us what to expect once the binary
    // data/hint files are loaded.

    #[test]
    fn test_read_hintfile_into_keydir() {

        let hint_bytes = Vec::from(&include_bytes!("../aux/test_hintfile.hint")[..]);

        let data_dir = tempfile::tempdir().unwrap();

        let hint_file_path = data_dir.as_ref().join("0.hint");
        let data_file_path = data_dir.as_ref().join("0.data");

        {
            let mut file = OpenOptions::new()
                                .create(true)
                                .write(true)
                                .open(&hint_file_path)
                                .unwrap();
            file.write_all(&hint_bytes[..]).unwrap();
        }

        let mut keydir = KeyDir::new();

        read_hint_file_into_keydir(0,
                                   &hint_file_path,
                                   &data_file_path,
                                   &mut keydir).unwrap();

        assert!(keydir.file_map.len() == 1);
        assert!(keydir.file_map.get(&0).map_or(false, |path| *path == data_file_path));

        let hintfile_guide_str = include_str!("../aux/test_hintfile.hint.guide");

        let hintfile_guide_lines = hintfile_guide_str
            .split("\n")
            .filter(|line| line.len() > 0)
            .collect::<Vec<_>>();

        for line in hintfile_guide_lines {
            let line = line.trim();
            let fields = line.split(",").collect::<Vec<_>>();

            let timestamp = fields[0].parse::<u64>().unwrap();
            let _key_size = fields[1].parse::<u16>().unwrap();
            let val_size = fields[2].parse::<u16>().unwrap();
            let offset = fields[3].parse::<u64>().unwrap();
            let key = fields[4];

            let entry = keydir.entries.remove(key).unwrap();

            assert!(entry.file_id == 0);
            assert!(entry.record_size == val_size);
            assert!(entry.record_offset == offset);
            assert!(entry.timestamp == timestamp);
        }
        assert!(keydir.entries.len() == 0);
    }

    #[test]
    fn test_read_datafile_into_keydir() {

        let data_bytes = Vec::from(&include_bytes!("../aux/test_datafile.data")[..]);

        let data_dir = tempfile::tempdir().unwrap();
        let data_file_path = data_dir.as_ref().join("0.data");

        {
            let mut file = OpenOptions::new()
                                .create(true)
                                .write(true)
                                .open(&data_file_path)
                                .unwrap();
            file.write_all(&data_bytes[..]).unwrap();
        }

        let mut keydir = KeyDir::new();

        read_data_file_into_keydir(0, &data_file_path, &mut keydir).unwrap();

        assert!(keydir.file_map.len() == 1);
        assert!(keydir.file_map.get(&0).map_or(false, |path| *path == data_file_path));

        let datafile_guide_str = include_str!("../aux/test_datafile.data.guide");
        let datafile_guide_lines = datafile_guide_str
            .split("\n")
            .filter(|line| line.len() > 0)
            .collect::<Vec<_>>();

        for line in datafile_guide_lines {
            let line = line.trim();
            let fields = line.split(",").collect::<Vec<_>>();

            let key = fields[0];
            let file_id = fields[1].parse::<FileID>().unwrap();
            let record_size = fields[2].parse::<u16>().unwrap();
            let record_offset = fields[3].parse::<u64>().unwrap();
            let timestamp = fields[4].parse::<u64>().unwrap();

            let entry = keydir.entries.remove(key).unwrap();

            assert!(file_id == 0);
            assert!(entry.file_id == file_id);
            assert!(entry.record_size == record_size);
            assert!(entry.record_offset == record_offset);
            assert!(entry.timestamp == timestamp);
        }
        assert!(keydir.entries.len() == 0);

    }

    #[test]
    fn test_creation() {
        let data_dir = tempfile::tempdir().unwrap();
        let cfg = ConfigBuilder::new(&data_dir).build();
        let mut br = BitRustState::new(cfg).unwrap();

        br.put("foo".to_string(), "bar".to_string()).unwrap();
        let r = br.get("foo").unwrap().unwrap();
        assert!(r == "bar");
    }

    #[test]
    fn test_locking_of_data_dir() {
        let data_dir = tempfile::tempdir().unwrap();
        let cfg = ConfigBuilder::new(&data_dir).build();
        let _br = BitRustState::new(cfg.clone()).unwrap();

        let another_br = BitRustState::new(cfg);
        assert!(another_br.is_err());
        if let Err(e) = another_br {
            assert!(e.kind() == io::ErrorKind::AlreadyExists);
        }
    }

    #[test]
    fn test_deletion() {
        let data_dir = tempfile::tempdir().unwrap();
        let cfg = ConfigBuilder::new(&data_dir).build();
        let mut br = BitRustState::new(cfg).unwrap();

        br.put("foo".to_string(), "bar".to_string()).unwrap();
        assert!(br.get("foo").unwrap().unwrap() == "bar");

        br.delete("foo").unwrap();
        assert!(br.get("foo").unwrap().is_none());
    }

    #[test]
    fn test_active_file_sealing() {
        // we expect the active file to be sealed once it reaches 1kB
        let sz_limit = 1_000;

        let data_dir = tempfile::tempdir().unwrap();
        let cfg = ConfigBuilder::new(&data_dir)
            .max_file_fize_bytes(sz_limit)
            .build();

        let mut br = BitRust::open(cfg).unwrap();

        let key = String::from("somekey");
        let value = String::from("somevalue");
        for _ in 0..1000 {
            br.put(key.clone(), value.clone()).unwrap();
        }

        let num_data_files = fs::read_dir(&data_dir)
                                .unwrap()
                                .map(|entry| entry.unwrap())
                                //.filter(|entry| entry.path().extension())
                                .filter_map(|entry| {
                                    entry.path()
                                        .extension()
                                        .and_then(OsStr::to_str)
                                        .map(|s| s.ends_with(".data"))
                                        .map(Option::from)
                                        .unwrap_or(None)
                                })
                                .count();

        // we can do better here by computing about how many 1kb files will
        // be needed to store 1000 identical entries somekey => somevalue.
        assert!(num_data_files > 1);
    }

    #[test]
    fn test_get_files_for_merging() {
        let sz_limit = 1024; // bytes

        let data_dir = tempfile::tempdir().unwrap();
        let cfg = ConfigBuilder::new(&data_dir)
            .max_file_fize_bytes(sz_limit)
            .build();

        let mut br = BitRust::open(cfg).unwrap();

        // This will take 32 bytes to store:
        // what                 size in bytes
        // ----------------------------------
        // checksum                         4
        // timestamp                        8
        // keysize                          2
        // valsize                          2
        // key ("somekey")                  7
        // val ("somevalue")                9 
        // ----------------------------------
        // total                           32

        let key = String::from("somekey");
        let value = String::from("somevalue");
        let entry_sz: usize = 4 + 8 + 2 + 2 + key.len() + value.len();

        let total_entries = 256;
        let total_open_files = (total_entries as f64 /
                                entry_sz as f64).ceil() as usize;

        for _ in 0..total_entries {
            br.put(key.clone(), value.clone()).unwrap();
        }

        let files_to_merge = br.state.get_files_for_merging();

        assert!(files_to_merge.len() == total_open_files - 1);

    }

    #[bench]
    fn bench_put(b: &mut Bencher) {
        let data_dir = tempfile::tempdir().unwrap();
        let config = ConfigBuilder::new(&data_dir).build();
        let mut br = BitRust::open(config).unwrap();

        let key = util::rand_str();
        let val = util::rand_str();
        b.iter(move || { br.put(key.clone(), val.clone()).unwrap(); });
    }
}
