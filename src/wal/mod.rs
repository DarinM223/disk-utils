use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};

use std::fs;
use std::fs::File;
use std::io;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::mem;
use std::path::Path;

/// Calls a method for an object contained inside
/// an option and returns an option of the result.
macro_rules! call_opt {
    ($var:expr, $meth:ident($( $param:expr ),*)) => (match $var {
        Some(ref v) => Some(v.$meth($($param),*)),
        None => None,
    });
}

#[repr(u8)]
#[derive(Clone, Copy)]
pub enum RecordType {
    Invalid = 0,
    Zero = 1,
    Full = 2,

    First = 3,
    Middle = 4,
    Last = 5,
}

impl RecordType {
    pub fn from_u8(i: u8) -> Option<RecordType> {
        if i >= RecordType::Invalid as u8 && i <= RecordType::Last as u8 {
            return Some(unsafe { mem::transmute(i) });
        }
        None
    }
}

/// 32KB Block size.
pub const BLOCK_SIZE: i64 = 32000;

#[derive(Clone)]
pub struct Record {
    crc: u32,
    size: u16,
    record_type: RecordType,
    payload: Vec<u8>,
}

impl Record {
    pub fn read<R: Read>(reader: &mut R) -> io::Result<Record> {
        let mut buf = [0; 7];
        reader.read(&mut buf)?;

        let mut rdr = Cursor::new(vec![buf[0], buf[1], buf[2], buf[3]]);
        let crc = rdr.read_u32::<BigEndian>()?;

        rdr = Cursor::new(vec![buf[4], buf[5]]);
        let size = rdr.read_u16::<BigEndian>()?;

        let record_type = match RecordType::from_u8(buf[6]) {
            Some(rt) => rt,
            None => unimplemented!(), // TODO(DarinM223): handle error
        };

        let mut payload = Vec::with_capacity(size as usize);
        reader.read(&mut payload)?;

        // TODO(DarinM223): check crc checksum for corruptions

        Ok(Record {
            crc: crc,
            size: size,
            record_type: record_type,
            payload: payload,
        })
    }

    pub fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        let mut wtr = Vec::new();
        wtr.write_u32::<BigEndian>(self.crc)?;
        let (crc1, crc2, crc3, crc4) = (wtr[0], wtr[1], wtr[2], wtr[3]);

        wtr = Vec::new();
        wtr.write_u16::<BigEndian>(self.size)?;
        let (size1, size2) = (wtr[0], wtr[1]);

        let record_type = self.record_type as u8;

        writer.write(&[crc1, crc2, crc3, crc4, size1, size2, record_type])?;
        writer.write(&self.payload)?;

        Ok(())
    }
}

pub trait Serializable {
    fn serialize(&self) -> io::Result<Vec<u8>>;
    fn deserialize(&mut self, bytes: Vec<u8>) -> io::Result<()>;
}

#[derive(PartialEq)]
enum SerializableState {
    None,
    First,
    Middle,
}

pub fn read_serializable<S: Serializable>(iter: &mut WalIterator,
                                          serializable: &mut S)
                                          -> io::Result<()> {
    let mut buf = Vec::new();
    let mut state = SerializableState::None;
    while let Some(mut record) = iter.next() {
        match record.record_type {
            RecordType::Invalid => {
                // TODO(DarinM223): return error
            }
            RecordType::Zero | RecordType::Full => {
                serializable.deserialize(record.payload)?;
                break;
            }
            RecordType::First => {
                if state != SerializableState::None {
                    // TODO(DarinM223): return error
                }
                state = SerializableState::First;
                buf.append(&mut record.payload);
            }
            RecordType::Middle => {
                if state != SerializableState::First || state != SerializableState::Middle {
                    // TODO(DarinM223): return error
                }
                state = SerializableState::Middle;
                buf.append(&mut record.payload);
            }
            RecordType::Last => {
                if state != SerializableState::Middle {
                    // TODO(DarinM223): return error
                }
                buf.append(&mut record.payload);
                serializable.deserialize(buf)?;
                break;
            }
        }
    }

    Ok(())
}

/// Iterator that reads through the write ahead log.
pub struct WalIterator {
    file: File,
    file_len: i64,
    block: Option<Vec<Record>>,
    block_index: Option<i32>,
    pos: Option<i64>,
}

impl WalIterator {
    pub fn new<P: AsRef<Path>>(path: &P) -> io::Result<WalIterator> {
        Ok(WalIterator {
            file: File::open(path)?,
            file_len: fs::metadata(path)?.len() as i64,
            block: None,
            block_index: None,
            pos: None,
        })
    }

    fn get_pos(&mut self, default: i64) -> i64 {
        match self.pos {
            Some(pos) => pos,
            None => {
                self.pos = Some(default);
                default
            }
        }
    }

    /// Fetches a block of records at the specified position.
    fn fetch_block(&mut self, position: i64) -> io::Result<()> {
        self.file.seek(SeekFrom::Start(position as u64))?;
        let mut buf = [0; BLOCK_SIZE as usize];
        self.file.read_exact(&mut buf)?;
        // TODO(DarinM223): read records from the bytes
        Ok(())
    }

    /// Fetches the correct block if the position has moved outside the current block
    /// or if the current block hasn't been loaded yet.
    fn load_block(&mut self, position: i64) -> io::Result<bool> {
        if let Some(block_index) = self.block_index {
            let block_len = call_opt!(self.block, len()).unwrap();
            if block_index < 0 {
                self.pos.as_mut().map(|pos| *pos -= BLOCK_SIZE);

                let pos = self.pos.unwrap();
                if is_out_of_bounds(pos, self.file_len) {
                    return Ok(true);
                }
                self.fetch_block(pos)?;

                let index = block_len as i32 - 1;
                self.block_index = Some(index);
            } else if block_index as usize >= block_len {
                self.pos.as_mut().map(|pos| *pos += BLOCK_SIZE);

                let pos = self.pos.unwrap();
                if is_out_of_bounds(pos, self.file_len) {
                    return Ok(true);
                }
                self.fetch_block(pos)?;

                self.block_index = Some(0);
            }
        } else {
            if is_out_of_bounds(position, self.file_len) {
                return Ok(true);
            }
            self.fetch_block(position)?;
        }

        Ok(false)
    }
}

impl Iterator for WalIterator {
    type Item = Record;

    /// Given the current position, return the record at the position and
    /// increment into the next record.
    fn next(&mut self) -> Option<Record> {
        let pos = self.get_pos(0);
        let out_of_bounds = self.load_block(pos).unwrap();
        if out_of_bounds {
            return None;
        }

        let block_index = self.block_index.unwrap() as usize;
        let next = call_opt!(self.block, get(block_index)).unwrap().unwrap();
        self.block_index.as_mut().map(|i| *i += 1);
        Some(next.clone())
    }
}

impl DoubleEndedIterator for WalIterator {
    fn next_back(&mut self) -> Option<Record> {
        let end_pos = self.file_len - 1;
        let pos = self.get_pos(end_pos);
        let out_of_bounds = self.load_block(pos).unwrap();
        if out_of_bounds {
            return None;
        }

        let block_index = self.block_index.unwrap() as usize;
        let next = call_opt!(self.block, get(block_index)).unwrap().unwrap();
        self.block_index.as_mut().map(|i| *i += 1);
        Some(next.clone())
    }
}

fn is_out_of_bounds(position: i64, file_length: i64) -> bool {
    if position + BLOCK_SIZE >= file_length || position - BLOCK_SIZE < 0 {
        return true;
    }
    false
}
