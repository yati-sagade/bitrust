use std::io;
use std::process;
use std::path::Path;

use lockfile::LockFile;
use common::BitrustOperation;

fn lock_file_base_name(op: BitrustOperation) -> String {
    String::from(match op {
        BitrustOperation::Create => ".create_lock",
        BitrustOperation::Write => ".write_lock",
        BitrustOperation::Merge => ".merge_lock",
    })
}


pub fn acquire<P>(data_dir: P, lock_type: BitrustOperation) -> io::Result<LockFile>
where
    P: AsRef<Path>,
{
    let lockfile_path = data_dir.as_ref().join(lock_file_base_name(lock_type));
    let pid_str = process::id().to_string();
    LockFile::new(lockfile_path, Some(pid_str.as_bytes()))
}
