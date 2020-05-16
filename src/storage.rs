use std::fs::File;
use std::io::{self, BufRead, Read, Seek, SeekFrom, Write};

use bitrust_pb::BitRustDataRecord;
use byteorder::{BigEndian, ReadBytesExt};
use bytes::{BufMut, BytesMut};
use errors::*;
use protobuf::Message;
use util;

// Offsets of recordio items in a payload.
// The offset of the data checksum is dependent on data size, so is not declared
// here.
const RECORD_SIZE_OFFSET: usize = 0;
const RECORD_SIZE_CRC_OFFSET: usize = 8;
const RECORD_DATA_OFFSET: usize = 12;

/// A trait for sequential and random read access to recordio logs. Each payload
/// in a log is laid out as follows:
///   
///   record_size: u64
///   record_size_crc: u32
///   record_data: [u8; record_size]
///   record_data_crc: u32
///
pub trait RecordReader {
  type Reader: BufRead + Seek;
  type Message: protobuf::Message;

  /// Impls must provide this method, and the rest of the behaviour is provided
  /// by default.
  fn reader<'a>(&'a mut self) -> Result<&'a mut Self::Reader>;

  /// Reads exactly the requested number of bytes from self.reader() using
  /// BufRead::read_exact(), returning any errors encountered in the process.
  fn read_exact_from_current_offset(&mut self, bytes: &mut [u8]) -> Result<()> {
    self.reader()?.read_exact(bytes).map_err(|e| e.into())
  }

  /// Reads exactly requested number of bytes from self.reader() starting at
  /// `offset_from_start` using BufRead::read_exact() (so a seek followed by
  /// read calls). Returns any errors encountered during seeking/reading.
  fn read_exact(
    &mut self,
    offset_from_start: u64,
    bytes: &mut [u8],
  ) -> Result<()> {
    self.reader()?.seek(SeekFrom::Start(offset_from_start))?;
    self.read_exact_from_current_offset(bytes)
  }

  /// Returns if self.reader() has reached EOF.
  fn eof(&mut self) -> Result<bool> {
    Ok(self.reader()?.fill_buf()?.is_empty())
  }

  /// Returns the next complete record in the reader wrapped by self.reader().
  /// If the reader is already at EOF, returns Ok(None).
  fn next_record(&mut self) -> Result<Option<Self::Message>> {
    let mut header = [0u8; RECORD_DATA_OFFSET];
    if self.eof()? {
      return Ok(None);
    }
    self
      .read_exact_from_current_offset(&mut header[..])
      .chain_err(|| "Error reading record size and record size CRC")?;
    let record_size = (&header[RECORD_SIZE_OFFSET..RECORD_SIZE_CRC_OFFSET])
      .read_u64::<BigEndian>()
      .chain_err(|| "Error reading record size from serialized header")?;
    let record_size_crc = (&header[RECORD_SIZE_CRC_OFFSET..])
      .read_u32::<BigEndian>()
      .chain_err(|| "Error reading record size CRC from serialized header")?;
    if util::checksum_crc32(&header[RECORD_SIZE_OFFSET..RECORD_SIZE_CRC_OFFSET])
      != record_size_crc
    {
      return Err(
        ErrorKind::InvalidData("CRC failed for record size".to_string()).into(),
      );
    }
    let mut data_and_crc = vec![0u8; (record_size + 4) as usize];
    self
      .read_exact_from_current_offset(&mut data_and_crc)
      .chain_err(|| "Error reading record data")?;
    let data_crc =
      (&data_and_crc[record_size as usize..]).read_u32::<BigEndian>()?;
    if util::checksum_crc32(&data_and_crc[..record_size as usize]) != data_crc {
      return Err(
        ErrorKind::InvalidData("CRC failed for record data".to_string()).into(),
      );
    }
    protobuf::parse_from_bytes::<Self::Message>(
      &data_and_crc[..record_size as usize],
    )
    .chain_err(|| "Error parsing data")
    .map(|m| Some(m))
  }

  /// Seeks self.reader() to `offset_from_start` and then reads a complete
  /// record starting at that offset.
  fn record_at_offset(
    &mut self,
    offset_from_start: u64,
  ) -> Result<Option<Self::Message>> {
    self.reader()?.seek(SeekFrom::Start(offset_from_start))?;
    self.next_record()
  }
}

pub trait RecordAppender {
  type Writer: Write + Seek;
  type Message: protobuf::Message;

  fn writer<'a>(&'a mut self) -> Result<&'a mut Self::Writer>;

  /// Returns the current position in self.writer() by seeking 0 bytes from
  /// the current position. Impls can provide a more efficient version by
  /// keeping track of write offsets themselves so we don't touch disk for
  /// this operation.
  fn tell(&mut self) -> Result<u64> {
    self
      .writer()?
      .seek(SeekFrom::Current(0))
      .map_err(|e| e.into())
  }

  /// Appends the given `record` to self.writer() and returns the starting
  /// offset of the record. So for the very first record appended, this offset
  /// is 0.
  fn append_record(&mut self, record: &Self::Message) -> Result<u64> {
    let offset = self.tell()?;
    let payload_size_bytes = 16 + record.compute_size();
    let mut payload = BytesMut::with_capacity(payload_size_bytes as usize);
    payload.put_u64_be(record.compute_size() as u64);
    let record_size_crc = util::checksum_crc32(&payload[..8]);
    payload.put_u32_be(record_size_crc);
    let record_data: Vec<u8> = record.write_to_bytes()?;
    let record_crc = util::checksum_crc32(&record_data);
    payload.put(record_data);
    payload.put_u32_be(record_crc);
    self
      .writer()?
      .write_all(&payload)
      .map_err(|e| e.into())
      .map(|_| offset)
  }

  /// Retreats the writer `bytes_to_retreat` bytes back. This is useful for
  /// "unappending" a log record, but only if its size is known.
  fn retreat(&mut self, bytes_to_retreat: u64) -> Result<u64> {
    self
      .writer()?
      .seek(SeekFrom::Current(-(bytes_to_retreat as i64)))
      .map_err(|e| e.into())
  }
}

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

#[cfg(test)]
mod record_reader_tests {
  use super::*;
  extern crate simplelog;
  extern crate tempfile;
  use bytes::{BufMut, BytesMut};
  use std::io::Cursor;

  struct CursorBasedReader<T> {
    cursor: Cursor<T>,
  }

  impl<T: AsRef<[u8]>> RecordReader for CursorBasedReader<T> {
    type Message = BitRustDataRecord;
    type Reader = Cursor<T>;

    fn reader<'a>(&'a mut self) -> Result<&'a mut Cursor<T>> {
      Ok(&mut self.cursor)
    }
  }

  #[test]
  fn test_next_record_fails_for_incomplete_data_at_end() {
    // Start with only three bytes in the cursor. The first read for
    // the record size should fail since it needs 4 bytes.
    let mut reader = CursorBasedReader {
      cursor: Cursor::new(vec![0u8, 0u8, 1u8]),
    };
    match reader.next_record() {
      Ok(record) => panic!("Expected error, but got Ok({:?})", record),
      Err(_) => {}
    }
  }

  #[test]
  fn test_next_record_succeeds_with_empty_stream() {
    let mut reader = CursorBasedReader {
      cursor: Cursor::new(vec![]),
    };
    let rec = reader.next_record();
    match rec {
      Ok(None) => {}
      _ => panic!("Expected Ok(None), but got {:?}", rec),
    }
  }

  #[test]
  fn test_next_record_succeeds_with_nonempty_stream() {
    let mut b = BytesMut::new();
    let mut record = BitRustDataRecord::new();
    record.set_timestamp(42);
    record.set_key(b"foo".to_vec());
    record.set_value(b"bar".to_vec());
    b.put_u64_be(record.compute_size() as u64);
    b.put_u32_be(util::checksum_crc32(&b[..8]));
    let record_data = record
      .write_to_bytes()
      .expect("Failed to write test record to bytes");
    b.put(&record_data);
    b.put_u32_be(util::checksum_crc32(&record_data));
    let mut reader = CursorBasedReader {
      cursor: Cursor::new(b),
    };
    let record_from_cursor = reader
      .next_record()
      .expect("Failed to read next record from cursor");
    assert!(
      record_from_cursor
        .clone()
        .expect("Expected a record from cursor, but found None")
        == record,
      format!("Expected {:?}, found {:?}", record, record_from_cursor)
    );
    let record_from_cursor =
      reader.next_record().expect("Expected ok result at EOF");
    assert!(
      record_from_cursor.is_none(),
      format!("Expected None, got {:?}", record_from_cursor)
    );
  }
}

#[cfg(test)]
mod record_appended_tests {
  use super::*;
  extern crate simplelog;
  extern crate tempfile;
  use byteorder::{BigEndian, ByteOrder};
  use std::io::Cursor;

  struct CursorBasedWriter {
    cursor: Cursor<Vec<u8>>,
  }

  impl RecordAppender for CursorBasedWriter {
    type Message = BitRustDataRecord;
    type Writer = Cursor<Vec<u8>>;

    fn writer<'a>(&'a mut self) -> Result<&'a mut Cursor<Vec<u8>>> {
      Ok(&mut self.cursor)
    }
  }

  #[test]
  fn test_append_record_works() {
    let mut rec = BitRustDataRecord::new();
    rec.set_timestamp(42);
    rec.set_key(b"k".to_vec());
    rec.set_value(b"v".to_vec());

    let mut writer = CursorBasedWriter {
      cursor: Cursor::new(vec![]),
    };
    writer
      .append_record(&rec)
      .expect("Writing record should succeed");

    let mut expected_buf = vec![0u8; (rec.compute_size() + 16) as usize];
    BigEndian::write_u64(
      &mut expected_buf[RECORD_SIZE_OFFSET..RECORD_SIZE_CRC_OFFSET],
      rec.compute_size() as u64,
    );
    let rec_size_crc = util::checksum_crc32(
      &expected_buf[RECORD_SIZE_OFFSET..RECORD_SIZE_CRC_OFFSET],
    );
    BigEndian::write_u32(
      &mut expected_buf[RECORD_SIZE_CRC_OFFSET..RECORD_DATA_OFFSET],
      rec_size_crc,
    );
    let rec_data = rec.write_to_bytes().expect("Vector of bytes");
    let record_data_crc_offset =
      RECORD_DATA_OFFSET + rec.compute_size() as usize;
    expected_buf[RECORD_DATA_OFFSET..record_data_crc_offset]
      .copy_from_slice(&rec_data);
    BigEndian::write_u32(
      &mut expected_buf[record_data_crc_offset..],
      util::checksum_crc32(&rec_data),
    );
    let written_buf = writer.cursor.into_inner();

    assert!(
      written_buf == expected_buf,
      "Expected {:?}, got {:?}",
      expected_buf,
      written_buf
    );
  }
}
