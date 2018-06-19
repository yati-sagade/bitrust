pub mod util;

extern crate byteorder;
extern crate bytes;
extern crate crc;
#[macro_use]
extern crate log;
extern crate simple_logger;

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use std::path::{PathBuf, Path};
use std::io;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, BufReader, Write, Seek, SeekFrom};

use bytes::{BytesMut, BufMut, IntoBuf, Buf};
use byteorder::{ReadBytesExt, BigEndian};

// In bitcask, a DataFile contains records the actual key and value with some
// metadata, encoded in a binary format. Hintfiles are written when a multiple
// data files are merged together into one data file, and are essentially
// indexes into those files. They deliberately contain very similary information
// to the in-memory keydir data structure. The goal of a hintfile is to allow
// fast reconstruction of the keydir when recovering.
enum FileKind {
    DataFile,
    HintFile,
}

// We'll just stick to the convention that *.data are datafiles, and *.hint
// are hintfiles.
impl FileKind {
    fn from_path(path: &PathBuf) -> Option<FileKind> {
        let ext = path.extension()?.to_str()?;
        match ext {
            "data" => Some(FileKind::DataFile),
            "hint" => Some(FileKind::HintFile),
            _ => None,
        }
    }
}

type FileID = u16;
type FileMap = HashMap<FileID, PathBuf>;

// The datadir is a folder on the filesystem where we store our datafiles and
// hintfiles. This data structure maps a given file id to a tuple that contains
// the path to the corresponding active file and the hint file, respectively.
//
// It only serves as a container when initializing the in-memory keydir.
//
// Note that it is an error to have a hint file without a corresponding data
// file, even though we represent both here using an Option<PathBuf>. This is
// done so we can scan through the datadir to build the DataDirContents
// structure, during which we might find a hint file before the corresponding
// data file.
type DataDirContents = HashMap<FileID, (Option<PathBuf>, Option<PathBuf>)>;

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
    entries: HashMap<String, KeyDirEntry>,
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
    pub write_handle: File,
    pub read_handle: File,
    pub name: PathBuf,
    pub id: FileID,
}

impl ActiveFile {
    fn new(path: PathBuf) -> io::Result<ActiveFile> {

        let write_handle = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&path)?;

        let read_handle = OpenOptions::new().read(true).create(false).open(&path)?;

        let file_id = file_id_from_path(&path);
        Ok(ActiveFile {
            write_handle,
            read_handle,
            name: path,
            id: file_id,
        })
    }

    pub fn read_exact(&mut self, offset_from_start: u64, bytes: &mut [u8]) -> io::Result<()> {
        self.read_handle.seek(SeekFrom::Start(offset_from_start))?;
        self.read_handle.read_exact(bytes)
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

#[derive(Debug)]
pub struct BitRust {
    keydir: KeyDir,
    data_dir: PathBuf,
    active_file: ActiveFile,
}

impl BitRust {
    pub fn new(data_dir: &Path) -> io::Result<BitRust> {
        info!("Making a bitrust");
        fs::create_dir_all(data_dir)?;

        let data_dir = data_dir.to_path_buf();
        let data_and_hint_files = get_data_and_hint_files(&data_dir)?;
        let keydir = build_key_dir(data_and_hint_files)?;

        let bitrust = BitRust {
            keydir,
            data_dir: data_dir.to_path_buf(),
            active_file: ActiveFile::new(data_dir.join("0.data"))?,
        };

        Ok(bitrust)
    }

    pub fn put(&mut self, key: String, value: String) -> io::Result<()> {

        let key_bytes = key.clone().into_bytes();
        let val_bytes = value.into_bytes();

        let payload_size = 32                   // checksum
                         + 64                   // timestamp
                         + 16                   // key size
                         + 16                   // value size
                         + key_bytes.len()      // key payload
                         + val_bytes.len()      // value payload
                         ;

        let mut payload = BytesMut::with_capacity(payload_size);

        // We split to after 32 bits so we can write the data first, and then
        // computer the checksum of this data to put in the initial 32 bits.
        let mut payload_head = payload.split_to(32);

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

        // Now payload_head contains all of the record we want to write out,
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

        Ok(())
    }

    pub fn get(&mut self, key: &str) -> io::Result<Option<String>> {
        let entry = self.keydir.get(key);
        if let Some(entry) = entry {
            // Read required bytes
            let mut read_buf = vec![0u8; entry.record_size as usize];

            debug!(
                "Asking to read {} bytes from offset {}",
                read_buf.len(),
                entry.record_offset
            );

            self.active_file.read_exact(
                entry.record_offset,
                &mut read_buf,
            )?;
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
            Ok(Some(val))
        } else {
            Ok(None)
        }
    }
}

fn build_key_dir(dd_contents: DataDirContents) -> io::Result<KeyDir> {
    info!("Making keydir");

    let mut dd_entries = dd_contents.into_iter().collect::<Vec<_>>();
    dd_entries.sort_by(|v1, v2| v1.0.cmp(&v2.0));

    let mut keydir = KeyDir::new();

    for (file_id, (data_file, hint_file)) in dd_entries {
        if let Some(hint_file) = hint_file {
            read_hint_file_into_keydir(file_id, &hint_file, &mut keydir)?;
        } else {
            let data_file = data_file.unwrap_or_else(|| {
                panic!(
                    "Expected datafile for file id {} not found when building keydir",
                    file_id
                )
            });
            read_data_file_into_keydir(file_id, &data_file, &mut keydir)?;
        }
    }
    Ok(keydir)
}

fn read_hint_file_into_keydir(
    file_id: FileID,
    hint_file: &PathBuf,
    key_dir: &mut KeyDir,
) -> io::Result<()> {

    let err_prefix = format!("When reading hint file {:?}", hint_file);
    info!("Reading hint file {:?}", hint_file);
    let mut reader = BufReader::new(File::open(hint_file)?);

    // | tstamp (64) | ksz (16) | record_size (16) | record_offset (32) | key ($ksz) |
    let timestamp = reader.read_u64::<BigEndian>()?;
    let key_size = reader.read_u16::<BigEndian>()?;
    let val_size = reader.read_u16::<BigEndian>()?;
    let val_pos = reader.read_u64::<BigEndian>()?;

    let key = {
        let mut key_bytes = vec![0u8; key_size as usize];
        reader.read_exact(&mut key_bytes)?;
        String::from_utf8(key_bytes.clone()).unwrap_or_else(|_| {
            panic!(
                "{}: Could not read key as utf8. Bytes={:?}",
                err_prefix,
                key_bytes
            )
        })
    };

    let entry = KeyDirEntry::new(file_id, val_size, val_pos, timestamp);
    key_dir.insert(key, entry);
    Ok(())
}


fn read_data_file_into_keydir(
    _file_id: FileID,
    _data_file: &PathBuf,
    _key_dir: &mut KeyDir,
) -> io::Result<()> {
    Ok(())
}

// Returns a HashMap from FileId to (DataFile, HintFile). HintFile can be absent
// when a DataFile is there, but the reverse should never happen, and this
// function panics in that case.
fn get_data_and_hint_files(data_dir: &PathBuf) -> io::Result<DataDirContents> {
    let mut retmap = DataDirContents::new();
    for entry in fs::read_dir(data_dir)? {
        let file_path = entry.expect("Error reading data directory").path();
        let file_id = file_id_from_path(&file_path);
        match FileKind::from_path(&file_path) {
            // XXX: both arms are almost identical except for which element
            // (0 or 1) of the tuple to insert/modify.
            Some(FileKind::DataFile) => {
                //data_files.insert(file_id, file_path);
                retmap
                    .entry(file_id)
                    .and_modify(|v: &mut (Option<PathBuf>, Option<PathBuf>)| {
                        v.0 = Some(file_path.clone());
                    })
                    .or_insert((Some(file_path.clone()), None));
            }
            Some(FileKind::HintFile) => {
                retmap
                    .entry(file_id)
                    .and_modify(|v: &mut (Option<PathBuf>, Option<PathBuf>)| {
                        v.1 = Some(file_path.clone());
                    })
                    .or_insert((None, Some(file_path.clone())));
            }
            _ => {}
        }
    }
    for (file_id, (data_file, hint_file)) in &retmap {
        match (data_file, hint_file) {
            (None, Some(hint_file)) => {
                panic!("Hint file {:?} found when data file was absent", hint_file)
            }
            (None, None) => panic!("No data AND hint file for file id {}!", file_id),
            _ => {}
        }
    }
    Ok(retmap)
}

fn file_id_from_path(path: &PathBuf) -> u16 {
    path.file_stem()
        .unwrap_or_else(|| panic!("Could not get stem path for {:?}", path))
        .to_str()
        .unwrap_or_else(|| panic!("Could not convert {:?} OsStr to &str", path))
        .parse::<u16>()
        .unwrap_or_else(|_| {
            panic!(
                "Non integral file id (or something that does not fit u16): {:?}",
                path
            )
        })
}
