use super::record::{BLOCK_SIZE, Record};
use std::fs;
use std::fs::File;
use std::io;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

/// Calls a method for an object contained inside
/// an option and returns an option of the result.
macro_rules! call_opt {
    ($var:expr, $meth:ident($( $param:expr ),*)) => (match $var {
        Some(ref v) => Some(v.$meth($($param),*)),
        None => None,
    });
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

        // Read records from the bytes and add them to the block.
        let mut block = Vec::new();
        let mut bytes = &buf[..];
        while let Ok(record) = Record::read(&mut bytes) {
            block.push(record);
        }
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

#[cfg(test)]
mod tests {
    #[test]
    fn test_perfect_file() {
        // TODO(DarinM223): test going from beginning to end.
        // TODO(DarinM223): test going from end to beginning.
        // TODO(DarinM223): test going forward and backward.
    }

    #[test]
    fn test_padding_file() {
        // TODO(DarinM223): test going from beginning to end.
        // TODO(DarinM223): test going from end to beginning.
        // TODO(DarinM223): test going forward and backward.
    }

    #[test]
    fn test_invalid_file() {
        // TODO(DarinM223): test going from beginning to end.
        // TODO(DarinM223): test going from end to beginning.
        // TODO(DarinM223): test going forward and backward.
    }
}
