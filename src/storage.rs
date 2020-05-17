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

/// Returns the size of the given message, when stored. Currently there is a
/// constant overhead of 16 bytes (8 bytes for message size, 4 bytes for message
/// size CRC32, 4 bytes for data CRC32).
#[inline(always)]
pub fn payload_size_for_record<M>(msg: &M) -> u64
where
  M: protobuf::Message,
{
  msg.compute_size() as u64 + 16
}

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

  /// Seeks to `offset_from_start`, reads `payload_size` bytes, and attempts to
  /// read a log record from these bytes. If the payload size is known in advance, this can be faster
  /// than record_at_offset(), which first has to read the record size.
  fn read_record(
    &mut self,
    offset_from_start: u64,
    payload_size: u64,
  ) -> Result<Self::Message> {
    let mut payload = vec![0u8; payload_size as usize];
    self.read_exact(offset_from_start, &mut payload)?;
    let mut record_size_slice =
      &payload[RECORD_SIZE_OFFSET..RECORD_SIZE_CRC_OFFSET];
    let record_size_slice_crc = util::checksum_crc32(record_size_slice);
    let record_size = record_size_slice
      .read_u64::<BigEndian>()
      .chain_err(|| format!("Error reading record size from bytes"))?;
    let record_size_crc = (&payload
      [RECORD_SIZE_CRC_OFFSET..RECORD_DATA_OFFSET])
      .read_u32::<BigEndian>()
      .chain_err(|| format!("Error reading record size CRC from bytes"))?;
    if record_size_slice_crc != record_size_crc {
      return Err(
        ErrorKind::InvalidData("CRC failed for record size".to_string()).into(),
      );
    }
    let record_data_crc_offset = RECORD_DATA_OFFSET + record_size as usize;
    let record_data = &payload[RECORD_DATA_OFFSET..record_data_crc_offset];
    let data_crc =
      (&payload[record_data_crc_offset..]).read_u32::<BigEndian>()?;
    if util::checksum_crc32(record_data) != data_crc {
      return Err(
        ErrorKind::InvalidData("CRC failed for record data".to_string()).into(),
      );
    }
    protobuf::parse_from_bytes::<Self::Message>(record_data)
      .chain_err(|| "Error reading message body from bytes")
      .map_err(|e| e.into())
  }
}

// Start offset and size of the written payload (which includes record size and
// CRCs).
pub type AppendRecordResult = Result<(u64, u64)>;

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

  /// Appends the given `record` to self.writer() and returns the starting offset
  /// of the record, and the total size of the payload. So for the very first
  /// record appended, this offset is 0. Currently the size overhead is 16 bytes
  /// per record.
  fn append_record(&mut self, record: &Self::Message) -> AppendRecordResult {
    let offset = self.tell()?;
    let payload_size_bytes = payload_size_for_record(record);
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
      .map(|_| (offset, payload.len() as u64))
  }

  /// Retreats the writer `bytes_to_retreat` bytes back. This is useful for
  /// "unappending" a log record, but only if its size is known.
  fn retreat(&mut self, bytes_to_retreat: u64) -> Result<u64> {
    self.writer()?.flush()?;
    self
      .writer()?
      .seek(SeekFrom::Current(-(bytes_to_retreat as i64)))
      .map_err(|e| e.into())
  }
}

#[cfg(test)]
mod test_utils {
  use super::*;
  use std::io::Cursor;

  pub struct CursorBasedReader<T> {
    pub cursor: Cursor<T>,
  }

  impl<T: AsRef<[u8]>> RecordReader for CursorBasedReader<T> {
    type Message = BitRustDataRecord;
    type Reader = Cursor<T>;

    fn reader<'a>(&'a mut self) -> Result<&'a mut Cursor<T>> {
      Ok(&mut self.cursor)
    }
  }

  pub struct CursorBasedWriter {
    pub cursor: Cursor<Vec<u8>>,
  }

  impl RecordAppender for CursorBasedWriter {
    type Message = BitRustDataRecord;
    type Writer = Cursor<Vec<u8>>;

    fn writer<'a>(&'a mut self) -> Result<&'a mut Cursor<Vec<u8>>> {
      Ok(&mut self.cursor)
    }
  }
}

#[cfg(test)]
mod record_reader_tests {
  use super::*;
  extern crate simplelog;
  extern crate tempfile;
  use super::test_utils::*;
  use byteorder::{BigEndian, ByteOrder};
  use bytes::{BufMut, BytesMut};
  use std::io::Cursor;

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

  #[test]
  fn test_read_record_works() {
    let mut rec = BitRustDataRecord::new();
    rec.set_timestamp(42);
    rec.set_key(b"k".to_vec());
    rec.set_value(b"v".to_vec());

    let mut expected_buf = vec![0u8; payload_size_for_record(&rec) as usize];
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

    let mut buf_with_padding = vec![0u8; expected_buf.len() + 2];
    buf_with_padding[1..expected_buf.len() + 1].copy_from_slice(&expected_buf);

    let mut reader = CursorBasedReader {
      cursor: Cursor::new(buf_with_padding),
    };

    let read_rec = reader
      .read_record(1, expected_buf.len() as u64)
      .expect("Non-error record");
    assert!(read_rec == rec);
  }

  #[test]
  fn test_read_record_fails_for_invalid_payload_spec() {
    let mut rec = BitRustDataRecord::new();
    rec.set_timestamp(42);
    rec.set_key(b"k".to_vec());
    rec.set_value(b"v".to_vec());

    let mut expected_buf = vec![0u8; payload_size_for_record(&rec) as usize];
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

    let mut buf_with_padding = vec![0u8; expected_buf.len() + 2];
    buf_with_padding[1..expected_buf.len() + 1].copy_from_slice(&expected_buf);

    let mut reader = CursorBasedReader {
      cursor: Cursor::new(buf_with_padding),
    };

    let read_rec = reader.read_record(0, expected_buf.len() as u64);
    assert!(read_rec.is_err(), "Expected error, got {:?}", read_rec);
  }
}

#[cfg(test)]
mod record_appender_tests {
  use super::test_utils::*;
  use super::*;
  extern crate simplelog;
  extern crate tempfile;
  use byteorder::{BigEndian, ByteOrder};
  use std::io::Cursor;

  #[test]
  fn test_retreat_works() {
    let mut rec = BitRustDataRecord::new();
    rec.set_timestamp(42);
    rec.set_key(b"k".to_vec());
    rec.set_value(b"v".to_vec());

    let mut writer = CursorBasedWriter {
      cursor: Cursor::new(vec![]),
    };
    let (offset, size) = writer.append_record(&rec).expect("Writing record");
    assert!((offset, size) == (0, payload_size_for_record(&rec)));
    writer.retreat(size).expect("Retreat should succeed");

    let mut rec = BitRustDataRecord::new();
    rec.set_timestamp(42);
    rec.set_key(b"k2".to_vec());
    rec.set_value(b"v2".to_vec());
    writer
      .append_record(&rec)
      .expect("Writing record after retreat");

    let mut reader = CursorBasedReader {
      cursor: Cursor::new(writer.cursor.into_inner()),
    };
    let read_rec = reader
      .next_record()
      .expect("Read record")
      .expect("Some record");
    assert!(read_rec == rec);
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
    assert!(
      writer
        .append_record(&rec)
        .expect("Writing record should succeed")
        == (0, payload_size_for_record(&rec))
    );

    let mut expected_buf = vec![0u8; payload_size_for_record(&rec) as usize];
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
