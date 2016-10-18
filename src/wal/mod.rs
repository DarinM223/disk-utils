use std::fs;
use std::fs::File;
use std::io;
use std::io::{Read, Write};
use std::path::Path;

macro_rules! call_opt {
    ($var:expr, $meth:ident($( $param:expr ),*)) => (match $var {
        Some(ref v) => Some(v.$meth($($param),*)),
        None => None,
    });
}

#[derive(Clone)]
pub enum RecordType {
    Zero,
    Full,

    First,
    Middle,
    Last,
}

/// 32KB Block size.
pub const BLOCK_SIZE: u64 = 32000;

#[derive(Clone)]
pub struct Record {
    crc: u32,
    size: u16,
    record_type: RecordType,
    payload: Vec<u8>,
}

impl Record {
    pub fn new<S: Serializable>(data: S) -> io::Result<Record> {
        unimplemented!()
    }

    pub fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        unimplemented!()
    }

    pub fn read<R: Read, S: Serializable>(reader: &mut R) -> io::Result<S> {
        unimplemented!()
    }
}

pub trait Serializable {
    fn serialize(&self) -> io::Result<Vec<u8>>;
    fn deserialize(&mut self, bytes: Vec<u8>) -> Self;
}

/// Iterator that reads through the write ahead log.
pub struct WalIterator {
    file: File,
    file_len: u64,
    block: Option<Vec<Record>>,
    block_index: Option<i32>,
    pos: Option<u64>,
}

impl WalIterator {
    pub fn new<P: AsRef<Path>>(path: &P) -> io::Result<WalIterator> {
        Ok(WalIterator {
            file: File::open(path)?,
            file_len: fs::metadata(path)?.len(),
            block: None,
            block_index: None,
            pos: None,
        })
    }

    fn get_pos(&mut self, default: u64) -> u64 {
        match self.pos {
            Some(pos) => pos,
            None => {
                self.pos = Some(default);
                default
            }
        }
    }

    /// Fetches a block of records at the specified position.
    fn fetch_block(&mut self, position: u64) -> io::Result<()> {
        unimplemented!()
    }
}

impl Iterator for WalIterator {
    type Item = Record;

    /// Given the current position, return the record at the position and
    /// increment into the next record.
    fn next(&mut self) -> Option<Record> {
        let pos = self.get_pos(0);
        if let Some(block_index) = self.block_index {
            //     pos       next_pos                             new_pos
            //      |          |   padding                         |
            //      V          V   ....                            V
            // +-----------------------+-----------------------+--------------------+
            // |-----BLOCK_SIZE--------|-------BLOCK_SIZE------|-----BLOCK_SIZE-----|
            //
            // if BLOCK_SIZE = 4
            // and new_pos = 14,
            //
            // 14 / 4 = 3.5 = 3
            //
            // the new block position should be the 4 * 3 = 12th byte,
            // which means starting from 0 it would be an index of 12 - 1 = 11.
            //
            // Fetch block where pos is located if pos is not in the current block.
            let block_len = call_opt!(self.block, len()).unwrap();
            let new_block_pos = (pos / BLOCK_SIZE) * BLOCK_SIZE - 1;
            if block_index < 0 {
                self.fetch_block(new_block_pos).unwrap();
                let index = block_len as i32 - 1;
                self.block_index = Some(index);
            } else if block_index as usize >= block_len {
                self.fetch_block(new_block_pos).unwrap();
                self.block_index = Some(0);
            }

            let next = call_opt!(self.block, get(block_index as usize)).unwrap().unwrap();
            if let Some(ref mut index) = self.block_index {
                *index += 1;
            }
            Some(next.clone())
        } else {
            self.fetch_block(pos).unwrap();
            let block_index = self.block_index.unwrap() as usize;
            let next = call_opt!(self.block, get(block_index)).unwrap().unwrap();
            if let Some(ref mut index) = self.block_index {
                *index += 1;
            }
            Some(next.clone())
        }
    }
}

impl DoubleEndedIterator for WalIterator {
    fn next_back(&mut self) -> Option<Record> {
        let end_pos = self.file_len - 1;
        let pos = self.get_pos(end_pos);
        unimplemented!()
    }
}
