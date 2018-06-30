use std::mem;
use std::io::{self, Write};
use std::path::Path;
use std::fs::OpenOptions;
use crc::crc32;

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
