use std::fs::File;
use std::io;

pub enum RecordType {
    Zero,
    Full,

    First,
    Middle,
    Last,
}

/// 32KB Block size.
pub const BLOCK_SIZE: u32 = 32000;

pub struct Record {
    crc: u32,
    size: u16,
    record_type: RecordType,
    payload: Vec<u8>,
}

pub trait Serializable {
    fn serialize(&self) -> io::Result<Vec<Record>>;
    fn deserialize(&mut self, records: &mut WalIterator) -> io::Result<()>;
}

/// Iterator that reads through the write ahead log.
pub struct WalIterator {
    file: File,
    segment_size: u32,
    curr_block: Vec<Record>,
    curr_index: i32,
    position: Option<u64>,
}

impl WalIterator {
    pub fn new(segment_size: u32) -> WalIterator {
        unimplemented!()
    }
}

impl Iterator for WalIterator {
    type Item = Record;

    fn next(&mut self) -> Option<Record> {
        match self.position {
            Some(pos) => {}
            None => {
                // TODO(DarinM223): create position starting at beginning of file.
            }
        }
        unimplemented!()
    }
}

impl DoubleEndedIterator for WalIterator {
    fn next_back(&mut self) -> Option<Record> {
        match self.position {
            Some(pos) => {}
            None => {
                // TODO(DarinM223): create position starting at end of file.
            }
        }
        unimplemented!()
    }
}
