use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};

use std::io;
use std::io::{Cursor, Read, Write};
use std::mem;

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RecordType {
    Zero = 1,
    Full = 2,

    First = 3,
    Middle = 4,
    Last = 5,
}

impl RecordType {
    pub fn from_u8(i: u8) -> Option<RecordType> {
        if i >= RecordType::Zero as u8 && i <= RecordType::Last as u8 {
            return Some(unsafe { mem::transmute(i) });
        }
        None
    }
}

/// 32KB Block size.
pub const BLOCK_SIZE: i64 = 32000;
/// 7B Header size for record.
pub const HEADER_SIZE: usize = 7;

/// A single entry of the write ahead log stored in blocks.
///
/// # Examples
///
/// ```
/// extern crate disk_utils;
/// use disk_utils::wal::record::{Record, RecordType};
/// use std::io::{Read, Write};
///
/// fn main() {
///     let record = Record {
///         crc: 123456789,
///         size: 12345,
///         record_type: RecordType::Full,
///         payload: vec![123; 12345],
///     };
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
    pub fn read<R: Read>(reader: &mut R) -> io::Result<Record> {
        let mut buf = [0; HEADER_SIZE];
        reader.read_exact(&mut buf)?;

        let record_type = match RecordType::from_u8(buf[0]) {
            Some(rt) => rt,
            None => return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid record type")),
        };

        let mut rdr = Cursor::new(vec![buf[1], buf[2], buf[3], buf[4]]);
        let crc = rdr.read_u32::<BigEndian>()?;

        rdr = Cursor::new(vec![buf[5], buf[6]]);
        let size = rdr.read_u16::<BigEndian>()?;

        let mut payload = vec![0; size as usize];
        reader.read_exact(&mut payload)?;

        // TODO(DarinM223): check crc checksum for corruptions

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

#[cfg(test)]
mod tests {
    use std::fs;
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom};
    use std::panic;

    use super::*;

    #[test]
    fn test_file_read_write() {
        let path: &'static str = "./files/record_test";
        let result = panic::catch_unwind(move || {
            let record = Record {
                crc: 123456789,
                size: 12345,
                record_type: RecordType::Full,
                payload: vec![123; 12345],
            };

            let mut file = OpenOptions::new()
                .read(true)
                .append(true)
                .create(true)
                .open(path)
                .unwrap();

            record.write(&mut file).unwrap();
            file.seek(SeekFrom::Start(0)).unwrap();

            let test_record = Record::read(&mut file).unwrap();
            assert_eq!(record, test_record);
        });

        fs::remove_file(path).unwrap();
        if let Err(e) = result {
            panic!(e);
        }
    }

    #[test]
    fn test_read_write_invalid_record() {
        let mut bytes = vec![0; 100];
        if let Ok(_) = Record::read(&mut &bytes[..]) {
            panic!("Reading invalid record padded by zeros should return error");
        }

        bytes = vec![0; 1];
        if let Ok(_) = Record::read(&mut &bytes[..]) {
            panic!("Reading invalid record with a single zero should return error");
        }

        bytes = vec![1, 2, 3, 4, 5, 6];
        if let Ok(_) = Record::read(&mut &bytes[..]) {
            panic!("Reading invalid record with a smaller header size should return error");
        }

        bytes = vec![1, 2, 3, 4, 5, 6, 7, 0];
        if let Ok(_) = Record::read(&mut &bytes[..]) {
            panic!("Reading invalid record with a smaller data size should return error");
        }
    }
}
