use crc::crc32;

pub fn checksum_crc32(msg: &[u8]) -> u32 {
    crc32::checksum_ieee(msg)
}
