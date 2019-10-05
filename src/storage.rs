use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};

use bitrust_pb::BitRustDataRecord;
use errors::*;
use protobuf::Message;

pub trait ReadableFile {
  fn file<'a>(&'a mut self) -> io::Result<&'a mut File>;

  fn read_exact(
    &mut self,
    offset_from_start: u64,
    bytes: &mut [u8],
  ) -> io::Result<()> {
    let fp = self.file()?;
    fp.seek(SeekFrom::Start(offset_from_start))?;
    fp.read_exact(bytes)
  }

  fn read_exact_from_current_offset(
    &mut self,
    bytes: &mut [u8],
  ) -> io::Result<()> {
    let fp = self.file()?;
    fp.read_exact(bytes)
  }

  // TODO: Provide more efficient implementation for tell() by keeping track
  // of write offsets.
  fn tell(&mut self) -> io::Result<u64> {
    let fp = self.file()?;
    fp.seek(SeekFrom::Current(0))
  }

  fn next_record(&mut self) -> Result<Option<BitRustDataRecord>> {
    let result =
      protobuf::parse_from_reader::<BitRustDataRecord>(self.file()?)?;
    Ok(if result.compute_size() == 0 {
      None
    } else {
      Some(result)
    })
  }

  fn read_record(
    &mut self,
    offset_from_start: u64,
    record_size: u64,
  ) -> Result<BitRustDataRecord> {
    let mut bytes = vec![0u8; record_size as usize];
    self.read_exact(offset_from_start, &mut bytes)?;
    protobuf::parse_from_bytes(&bytes[..]).chain_err(|| "Error reading record")
  }
}
