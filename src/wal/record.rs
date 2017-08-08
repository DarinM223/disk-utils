use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};
use crc::crc32;

use std::io;
use std::io::{Cursor, Read, Write};

use enum_primitive::FromPrimitive;

enum_from_primitive! {
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RecordType {
    Zero = 1,
    Full = 2,

    First = 3,
    Middle = 4,
    Last = 5,
}
}

/// 32KB Block size.
pub const BLOCK_SIZE: i64 = 32768;
/// 7B Header size for record.
pub const HEADER_SIZE: usize = 7;

/// A single entry of the write ahead log stored in blocks.
///
/// # Examples
///
/// ```
/// extern crate disk_utils;
/// use disk_utils::wal::record::{Record, RecordType};
///
/// fn main() {
///     let record = Record::new(RecordType::Full, vec![123; 12345]);
///
///     // Write record into a byte buffer.
///     let mut bytes = Vec::new();
///     record.write(&mut bytes).unwrap();
///
///     // Read record from the byte buffer.
///     let test_record = Record::read(&mut &bytes[..]).unwrap();
///     assert_eq!(record, test_record);
/// }
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct Record {
    pub crc: u32,
    pub size: u16,
    pub record_type: RecordType,
    pub payload: Vec<u8>,
}

impl Record {
    pub fn new(record_type: RecordType, payload: Vec<u8>) -> Record {
        let crc = crc32::checksum_ieee(&payload[..]);
        Record {
            crc: crc,
            size: payload.len() as u16,
            record_type: record_type,
            payload: payload,
        }
    }

    pub fn read<R: Read>(reader: &mut R) -> io::Result<Record> {
        let mut buf = [0; HEADER_SIZE];
        reader.read_exact(&mut buf)?;

        let record_type = match RecordType::from_u8(buf[0]) {
            Some(rt) => rt,
            None => return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid record type")),
        };

        let mut rdr = Cursor::new(buf[1..5].to_vec());
        let crc = rdr.read_u32::<BigEndian>()?;

        rdr = Cursor::new(buf[5..7].to_vec());
        let size = rdr.read_u16::<BigEndian>()?;

        let mut payload = vec![0; size as usize];
        reader.read_exact(&mut payload)?;

        let payload_crc = crc32::checksum_ieee(&payload[..]);
        if payload_crc != crc {
            return Err(io::Error::new(io::ErrorKind::InvalidData,
                                      "CRC checksum failed, possibly corrupted record data"));
        }

        Ok(Record {
            crc: crc,
            size: size,
            record_type: record_type,
            payload: payload,
        })
    }

    pub fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        let record_type = self.record_type as u8;

        let mut wtr = Vec::new();
        wtr.write_u32::<BigEndian>(self.crc)?;
        let (crc1, crc2, crc3, crc4) = (wtr[0], wtr[1], wtr[2], wtr[3]);

        wtr = Vec::new();
        wtr.write_u16::<BigEndian>(self.size)?;
        let (size1, size2) = (wtr[0], wtr[1]);

        writer.write(&[record_type, crc1, crc2, crc3, crc4, size1, size2])?;
        writer.write(&self.payload)?;
        writer.flush()?;

        Ok(())
    }
}
