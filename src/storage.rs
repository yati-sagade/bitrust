use std::io::{self, Read, Seek, SeekFrom};
use std::fs::File;

pub trait ReadableFile {
    fn file<'a>(&'a mut self) -> io::Result<&'a mut File>;

    fn read_exact(&mut self, offset_from_start: u64, bytes: &mut [u8]) -> io::Result<()> {
        let fp = self.file()?;
        fp.seek(SeekFrom::Start(offset_from_start))?;
        fp.read_exact(bytes)
    }

    fn read_exact_from_current_offset(&mut self, bytes: &mut [u8]) -> io::Result<()> {
        let fp = self.file()?;
        fp.read_exact(bytes)
    }

    fn tell(&mut self) -> io::Result<u64> {
        let fp = self.file()?;
        fp.seek(SeekFrom::Current(0))
    }
}
