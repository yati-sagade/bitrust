use std::io;
use std::process;
use std::path::Path;

use lockfile::LockFile;

pub enum LockType {
    Create,
    Write,
    Merge,
}

impl LockType {
    fn lock_file_base_name(&self) -> String {
        String::from(match self {
            &LockType::Create => ".create_lock",
            &LockType::Write => ".write_lock",
            &LockType::Merge => ".merge_lock",
        })
    }
}

pub fn acquire<P>(data_dir: P, lock_type: LockType) -> io::Result<LockFile>
where
    P: AsRef<Path>,
{
    let lockfile_path = data_dir.as_ref().join(&lock_type.lock_file_base_name());
    let pid_str = process::id().to_string();
    LockFile::new(lockfile_path, Some(pid_str.as_bytes()))
}
