use std::path::{Path, PathBuf};

pub const MAX_ACTIVE_FILE_SIZE_BYTES: u64 = 0x80000000; // 2GB by default

#[derive(Debug, Clone)]
pub struct Config {
    /// Maximum size of the active file in bytes.
    max_file_fize_bytes: u64,

    /// Main data directory where BitRust datafiles are kept.
    datadir: PathBuf,
}

impl Config {
    pub fn datadir(&self) -> &Path {
        &self.datadir
    }

    pub fn max_file_fize_bytes(&self) -> u64 {
        self.max_file_fize_bytes
    }
}

#[derive(Debug)]
pub struct ConfigBuilder {
    datadir: PathBuf,
    max_file_fize_bytes: u64,
}

impl ConfigBuilder {
    pub fn new<P: AsRef<Path>>(datadir: P) -> ConfigBuilder {
        ConfigBuilder {
            datadir: datadir.as_ref().to_path_buf(),
            max_file_fize_bytes: MAX_ACTIVE_FILE_SIZE_BYTES,
        }
    }

    pub fn datadir<'a, P: AsRef<Path>>(&'a mut self, datadir: P) -> &'a mut ConfigBuilder {
        self.datadir = datadir.as_ref().to_path_buf();
        self
    }

    pub fn max_file_fize_bytes<'a>(&'a mut self, size: u64) -> &'a mut ConfigBuilder {
        self.max_file_fize_bytes = size;
        self
    }

    pub fn build(&mut self) -> Config {
        Config {
            datadir: self.datadir.clone(),
            max_file_fize_bytes: self.max_file_fize_bytes,
        }
    }
}
