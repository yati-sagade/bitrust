use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::process;
use std::io::{self, Write};
use std::fs;

#[macro_use]
use log;


#[derive(Debug)]
pub struct LockFile {
    path: PathBuf,
}

impl LockFile {
    pub fn new<P>(path: P, lockfile_contents: Option<&[u8]>) -> io::Result<LockFile>
        where P: AsRef<Path>
    {

        {
            // create_new() translates to O_EXCL|O_CREAT being specified to the
            // underlying open() syscall on *nix (and CREATE_NEW to the
            // CreateFileW Windows API), which means that the call is successful
            // only if it is the one which created the file.
            let mut file = OpenOptions::new().write(true)
                                             .create_new(true)
                                             .open(path.as_ref())?;
            if let Some(contents) = lockfile_contents {
                file.write_all(contents)?;
            }
        }
        debug!("Successfully wrote lockfile {:?}, pid: {}",
               path.as_ref(), process::id());
        // By this time the file we created is closed, and we are sure that
        // we are the one who created it.
        Ok(LockFile { path: path.as_ref().to_path_buf() })
    }
}

impl Drop for LockFile {
    fn drop(&mut self) {
        debug!("Removing lockfile {:?}, pid: {}", &self.path, process::id());
        fs::remove_file(&self.path).unwrap();
    }
}

