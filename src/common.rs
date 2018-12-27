pub type FileID = u16;

pub const BITRUST_TOMBSTONE_STR: &'static [u8] = b"<bitrust_tombstone>";

#[derive(Debug)]
pub enum BitrustOperation {
    Write,
    Merge,
    Create,
}
