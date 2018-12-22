use std::mem;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::fs::{self, OpenOptions};
use crc::crc32;
use rand::{self, Rng};
use regex::Regex;
use std::collections::HashMap;

use common::FileID;

pub fn checksum_crc32(msg: &[u8]) -> u32 {
    crc32::checksum_ieee(msg)
}

pub fn sizeof_val<T>(_: &T) -> usize {
    mem::size_of::<T>()
}

pub fn write_to_file<P, S>(path: P, buf: S) -> io::Result<()>
where
    P: AsRef<Path>,
    S: AsRef<str>,
{
    let mut file = OpenOptions::new().create(true).write(true).open(path)?;
    let buf = buf.as_ref().bytes().collect::<Vec<_>>();
    file.write_all(&buf[..])
}

pub fn rand_str() -> String {
    let size: usize = rand::thread_rng().gen_range(4, 1024);
    rand::thread_rng().gen_iter::<char>().take(size).collect()
}

pub fn file_id_from_path<P>(path: P) -> u16
where
    P: AsRef<Path>,
{
    let path = path.as_ref();
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

#[derive(Debug)]
pub struct FileIDGen {
    next_id: FileID,
}

impl FileIDGen {
    pub fn new(next_id: FileID) -> FileIDGen {
        FileIDGen { next_id }
    }

    pub fn take_next_id(&mut self) -> FileID {
        let ret = self.next_id;
        self.next_id += 1;
        ret
    }
}

/// The datadir is a folder on_di the filesystem where we store our datafiles and
/// hintfiles. This data structure maps a given file id to a tuple that contains
/// the path to the corresponding active file and the hint file, respectively.
///
/// It only serves as a container when initializing the in-memory keydir.
///
/// Note that it is an error to have a hint file without a corresponding data
/// file, even though we represent both here using an Option<PathBuf>. This is
/// done so we can scan through the datadir to build the DataDirContents
/// structure, during which we might find a hint file before the corresponding
/// data file.
pub type DataDirContents = HashMap<FileID, (Option<PathBuf>, Option<PathBuf>)>;

fn is_data_or_hint_file<P>(path: P) -> bool
where
    P: AsRef<Path>,
{
    lazy_static! {
        static ref RE: Regex = Regex::new(r"^[0-9]+\.(?:data|hint)$").unwrap();
    }
    let maybe_file_name_str = path.as_ref()
                                  .file_name()
                                  .and_then(|s| s.to_str()) // OsStr -> &str
                                  ;
    if let Some(file_name_str) = maybe_file_name_str {
        RE.is_match(file_name_str)
    } else {
        false
    }
}

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
    fn from_path<P: AsRef<Path>>(path: P) -> Option<FileKind> {
        let ext = path.as_ref().extension()?.to_str()?;
        match ext {
            "data" => Some(FileKind::DataFile),
            "hint" => Some(FileKind::HintFile),
            _ => None,
        }
    }
}

/// Returns a HashMap from FileId to (DataFile, HintFile). HintFile can be absent
/// when a DataFile is there, but the reverse should never happen, and this
/// function panics in that case.
pub fn get_data_and_hint_files<P>(data_dir: P) -> io::Result<DataDirContents>
where
    P: AsRef<Path>,
{
    let mut retmap = DataDirContents::new();
    for entry in fs::read_dir(data_dir)? {
        let file_path = entry.expect("Error reading data directory").path();

        if !is_data_or_hint_file(&file_path) {
            continue;
        }

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

#[cfg(test)]
mod tests {
    extern crate tempfile;

    use super::*;
    use std::fs::File;

    #[test]
    fn test_file_id_from_path() {
        let path = PathBuf::from("/some/path/to/42.data");
        let file_id = file_id_from_path(&path);
        assert!(file_id == 42);

        let path = PathBuf::from("/some/path/to/42.hint");
        let file_id = file_id_from_path(&path);
        assert!(file_id == 42);
    }

    #[test]
    fn test_get_data_and_hint_files() {
        let data_dir = tempfile::tempdir().unwrap();
        for idx in 0..10 {
            let data_file_path = data_dir.path().join(&format!("{}.data", idx));
            let _f = File::create(&data_file_path).unwrap();
            if idx < 5 {
                let hint_file_path = data_dir.path().join(&format!("{}.hint", idx));
                let _f = File::create(&hint_file_path).unwrap();
            }
            let spurious_file_path = data_dir.path().join(&format!("{}.spurious", idx));
            let _f = File::create(&spurious_file_path).unwrap();
        }
        let spurious_file_path = data_dir.path().join("10.spurious");
        let _f = File::create(&spurious_file_path).unwrap();

        let mut dd_contents = get_data_and_hint_files(data_dir.path()).unwrap();

        for idx in 0..10 {
            let (data_file, hint_file) = dd_contents.remove(&idx).unwrap_or_else(|| {
                panic!("Missing file_id {}", idx);
            });
            let data_file = data_file.unwrap_or_else(|| panic!("data file {} not found", idx));

            assert!(data_file == data_dir.path().join(&format!("{}.data", idx)));

            if idx < 5 {
                let hint_file = hint_file.unwrap_or_else(|| panic!("hint file {} not found", idx));
                assert!(hint_file == data_dir.path().join(&format!("{}.hint", idx)));
            }
        }
        assert!(dd_contents.len() == 0);
    }
}
