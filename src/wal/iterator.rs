use std::fs::File;
use std::io;
use std::io::{Read, Seek, SeekFrom};
use std::result;

use wal::record::{BLOCK_SIZE, Record};

/// Calls a method for an object contained inside
/// an option and returns an option of the result.
macro_rules! call_opt {
    ($var:expr, $meth:ident($( $param:expr ),*)) => (match $var {
        Some(ref v) => Some(v.$meth($($param),*)),
        None => None,
    });
}

#[derive(PartialEq)]
pub enum ReadDirection {
    Forward,
    Backward,
}

#[derive(Debug)]
pub enum BlockError {
    IoError(io::Error),
    EmptyBlock,
    OutOfBounds,
}

impl From<io::Error> for BlockError {
    fn from(err: io::Error) -> BlockError {
        BlockError::IoError(err)
    }
}

pub type Result<T> = result::Result<T, BlockError>;

/// Iterator that reads through the write ahead log.
pub struct WalIterator<'a> {
    manager: BlockManager<'a>,
    direction: ReadDirection,
    block: Vec<Record>,
    index: i32,
}

impl<'a> WalIterator<'a> {
    pub fn new<'b>(file: &'b mut File, direction: ReadDirection) -> Result<WalIterator<'b>> {
        let manager = BlockManager::new(file, &direction)?;
        let block = manager.curr();
        let index = match direction {
            ReadDirection::Forward => -1,
            ReadDirection::Backward => block.len() as i32,
        };

        Ok(WalIterator {
            manager: manager,
            direction: direction,
            block: block,
            index: index,
        })
    }
}

impl<'a> Iterator for WalIterator<'a> {
    type Item = Record;

    /// Given the current position, return the record at the position and
    /// increment into the next record.
    fn next(&mut self) -> Option<Record> {
        if self.direction == ReadDirection::Backward {
            self.direction = ReadDirection::Forward;
            return self.block.get(self.index as usize).cloned();
        }

        if self.index + 1 >= self.block.len() as i32 {
            match self.manager.next() {
                Err(BlockError::OutOfBounds) |
                Err(BlockError::EmptyBlock) => return None,
                Err(e) => panic!("next() error: {:?}", e),
                _ => {}
            }
            self.block = self.manager.curr();
            self.index = 0;
        } else {
            self.index += 1;
        }

        self.block.get(self.index as usize).cloned()
    }
}

impl<'a> DoubleEndedIterator for WalIterator<'a> {
    fn next_back(&mut self) -> Option<Record> {
        if self.direction == ReadDirection::Forward {
            self.direction = ReadDirection::Backward;
            return self.block.get(self.index as usize).cloned();
        }

        if self.index - 1 < 0 {
            match self.manager.prev() {
                Err(BlockError::OutOfBounds) |
                Err(BlockError::EmptyBlock) => return None,
                Err(e) => panic!("next_back() error: {:?}", e),
                _ => {}
            }
            self.block = self.manager.curr();
            self.index = self.block.len() as i32 - 1;
        } else {
            self.index -= 1;
        }

        self.block.get(self.index as usize).cloned()
    }
}

struct BlockManager<'a> {
    file: &'a mut File,
    len: i64,
    pos: i64,
    block: Vec<Record>,
}

impl<'a> BlockManager<'a> {
    fn new<'b>(file: &'b mut File, direction: &ReadDirection) -> Result<BlockManager<'b>> {
        let file_len = file.metadata()?.len() as i64;
        let pos = match *direction {
            ReadDirection::Forward => 0,
            ReadDirection::Backward => {
                let end_pos = (file_len / BLOCK_SIZE) * BLOCK_SIZE;
                if end_pos >= file_len {
                    end_pos - BLOCK_SIZE
                } else {
                    end_pos
                }
            }
        };

        let block = if check_out_of_bounds(pos, file_len) {
            Vec::new()
        } else {
            match load_block(file, pos) {
                Ok(block) => block,
                Err(BlockError::EmptyBlock) |
                Err(BlockError::OutOfBounds) => Vec::new(),
                Err(e) => return Err(e),
            }
        };

        Ok(BlockManager {
            file: file,
            len: file_len,
            pos: pos,
            block: block,
        })
    }

    fn curr(&self) -> Vec<Record> {
        self.block.clone()
    }

    fn next(&mut self) -> Result<()> {
        if check_out_of_bounds(self.pos, self.len) {
            return Err(BlockError::OutOfBounds);
        }
        self.pos += BLOCK_SIZE;
        if check_out_of_bounds(self.pos, self.len) {
            return Err(BlockError::OutOfBounds);
        }

        self.block = load_block(self.file, self.pos)?;
        Ok(())
    }

    fn prev(&mut self) -> Result<()> {
        if check_out_of_bounds(self.pos, self.len) {
            return Err(BlockError::OutOfBounds);
        }
        self.pos -= BLOCK_SIZE;
        if check_out_of_bounds(self.pos, self.len) {
            return Err(BlockError::OutOfBounds);
        }

        self.block = load_block(self.file, self.pos)?;
        Ok(())
    }
}

fn load_block(file: &mut File, pos: i64) -> Result<Vec<Record>> {
    file.seek(SeekFrom::Start(pos as u64))?;
    let mut buf = [0; BLOCK_SIZE as usize];
    file.read(&mut buf)?;

    // Read records from the bytes and add them to the block.
    let mut block = Vec::new();
    let mut bytes = &buf[..];
    while let Ok(record) = Record::read(&mut bytes) {
        block.push(record);
    }
    if block.len() == 0 {
        return Err(BlockError::EmptyBlock);
    }

    Ok(block)
}

fn check_out_of_bounds(position: i64, file_length: i64) -> bool {
    if position < 0 || position > file_length {
        return true;
    }
    false
}
