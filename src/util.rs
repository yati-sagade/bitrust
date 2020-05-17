use crc::crc32;
use rand::{self, Rng};
use regex::Regex;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::mem;
use std::path::{Path, PathBuf};

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

pub fn read_from_file<P>(path: P) -> io::Result<String>
where
  P: AsRef<Path>,
{
  let mut fp = File::open(path)?;
  let mut buf = String::new();
  fp.read_to_string(&mut buf)?;
  Ok(buf)
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
  path
    .file_stem()
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
pub struct Serial<T> {
  next: T,
}

impl<T> Serial<T>
where
  T: std::ops::AddAssign + num::One + Clone,
{
  pub fn new(next: T) -> Serial<T> {
    Serial { next }
  }

  /// Note this is not atomic; you are responsible for synchronized calling
  /// of this method.
  pub fn take_next(&mut self) -> T {
    let ret = self.next.clone();
    self.next += T::one();
    ret
  }
}

pub type FileIDGen = Serial<FileID>;

pub trait LogicalClock {
  fn tick(&mut self) -> u64;
  fn set_next(&mut self, t: u64);
}

pub type SerialLogicalClock = Serial<u64>;
impl LogicalClock for SerialLogicalClock {
  fn tick(&mut self) -> u64 {
    self.take_next()
  }
  fn set_next(&mut self, t: u64) {
    self.next = t;
  }
}

/// A single data file and optionally its associated hint-file.
/// Note that it is an error to have a hintfile witout a datafile.
#[derive(Debug, Clone)]
pub enum DataDirEntry {
  DataAndHintFile(PathBuf, PathBuf),
  DataFile(PathBuf),
}

impl DataDirEntry {
  pub fn data_file_path<'a>(&'a self) -> &'a PathBuf {
    match self {
      &DataDirEntry::DataFile(ref path) => path,
      &DataDirEntry::DataAndHintFile(ref path, _) => path,
    }
  }
}

/// The datadir is a folder on_di the filesystem where we store our datafiles and
/// hintfiles. This data structure maps a given file id to a tuple that contains
/// the path to the corresponding active file and the hint file, respectively.
///
/// It only serves as a container when initializing the in-memory keydir.
pub type DataDirContents = HashMap<FileID, DataDirEntry>;

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
pub fn read_data_dir_contents<P>(data_dir: P) -> io::Result<DataDirContents>
where
  P: AsRef<Path>,
{
  let mut data_files = HashMap::<FileID, PathBuf>::new();
  let mut hint_files = HashMap::<FileID, PathBuf>::new();
  for entry in fs::read_dir(data_dir)? {
    let file_path = entry.expect("Error reading data directory").path();
    if let Some(file_kind) = FileKind::from_path(&file_path) {
      let file_id = file_id_from_path(&file_path);
      match file_kind {
        FileKind::DataFile => {
          data_files.insert(file_id, file_path);
        }
        FileKind::HintFile => {
          hint_files.insert(file_id, file_path);
        }
      }
    }
  }
  let mut contents = DataDirContents::new();
  for (file_id, data_file_path) in data_files {
    if let Some(hint_file_path) = hint_files.remove(&file_id) {
      contents.insert(
        file_id,
        DataDirEntry::DataAndHintFile(data_file_path, hint_file_path),
      );
    } else {
      contents.insert(file_id, DataDirEntry::DataFile(data_file_path));
    }
  }
  if !hint_files.is_empty() {
    warn!(
      "Hint files {:?} without corresponding data files found: ",
      hint_files.values().collect::<Vec<_>>()
    );
  }
  Ok(contents)
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
      let spurious_file_path =
        data_dir.path().join(&format!("{}.spurious", idx));
      let _f = File::create(&spurious_file_path).unwrap();
    }
    let spurious_file_path = data_dir.path().join("10.spurious");
    let _f = File::create(&spurious_file_path).unwrap();

    let mut dd_contents = read_data_dir_contents(data_dir.path()).unwrap();

    for idx in 0..10 {
      let dd_entry = dd_contents.remove(&idx).unwrap_or_else(|| {
        panic!("Missing file_id {}", idx);
      });
      assert!(
        dd_entry.data_file_path()
          == &data_dir.path().join(&format!("{}.data", idx))
      );
      if idx < 5 {
        assert!(if let DataDirEntry::DataAndHintFile(_, _) = dd_entry {
          true
        } else {
          false
        });
      }
    }
    assert!(dd_contents.len() == 0);
  }
}
