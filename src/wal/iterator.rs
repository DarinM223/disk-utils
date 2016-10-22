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
    pub fn new<P: AsRef<Path> + ?Sized>(path: &P) -> io::Result<WalIterator> {
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
        if block.len() == 0 {
            return Err(io::Error::new(io::ErrorKind::NotFound, "Retrieved block is empty"));
        }
        self.block = Some(block);
        Ok(())
    }

    /// Fetches the correct block if the position has moved outside the current block
    /// or if the current block hasn't been loaded yet.
    fn load_block(&mut self, position: i64, forward: bool) -> io::Result<bool> {
        if let Some(block_index) = self.block_index {
            let block_len = call_opt!(self.block, len()).unwrap();
            if block_index < 0 {
                self.pos.as_mut().map(|pos| *pos -= BLOCK_SIZE);

                let pos = self.pos.unwrap();
                if check_forward_bounds(pos, self.file_len) {
                    return Ok(true);
                }
                self.fetch_block(pos)?;

                let index = call_opt!(self.block, len()).map(|len| {
                    self.block_index = Some(len as i32 - 1);
                });
            } else if block_index as usize >= block_len {
                self.pos.as_mut().map(|pos| *pos += BLOCK_SIZE);

                let pos = self.pos.unwrap();
                if check_forward_bounds(pos, self.file_len) {
                    return Ok(true);
                }
                self.fetch_block(pos)?;

                self.block_index = Some(0);
            }
        } else {
            let out_of_bounds = match forward {
                true => check_forward_bounds(position, self.file_len),
                false => check_backward_bounds(position),
            };
            if out_of_bounds {
                return Ok(true);
            }
            self.fetch_block(position)?;

            let end_index = call_opt!(self.block, len()).unwrap() as i32 - 1;
            match forward {
                true => self.block_index = Some(0),
                false => self.block_index = Some(end_index),
            }
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
        let out_of_bounds = self.load_block(pos, true).unwrap();
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
        let end_pos = self.file_len - BLOCK_SIZE;
        let pos = self.get_pos(end_pos);
        let out_of_bounds = self.load_block(pos, false).unwrap();
        if out_of_bounds {
            return None;
        }

        let block_index = self.block_index.unwrap() as usize;
        let next = call_opt!(self.block, get(block_index)).unwrap().unwrap();
        self.block_index.as_mut().map(|i| *i -= 1);
        Some(next.clone())
    }
}

fn check_forward_bounds(position: i64, file_length: i64) -> bool {
    if position < 0 || position + BLOCK_SIZE > file_length {
        return true;
    }
    false
}

fn check_backward_bounds(position: i64) -> bool {
    if position - BLOCK_SIZE < 0 {
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::fs::File;
    use std::io::{Seek, SeekFrom};
    use std::panic;
    use super::*;
    use super::super::record::{BLOCK_SIZE, Record, RecordType};

    #[test]
    fn test_perfect_file() {
        let record_size = (BLOCK_SIZE / 4) as u16;
        let payload_size = record_size - 7;
        let mut records = Vec::with_capacity(8);
        for i in 0..8 {
            let record_type = match i {
                0 => RecordType::First,
                7 => RecordType::Last,
                _ => RecordType::Middle,
            };

            records.push(Record {
                crc: 123456789,
                size: payload_size,
                record_type: record_type,
                payload: vec![123; payload_size as usize],
            });
        }

        let path: &'static str = "./files/perfect_file";
        let result = panic::catch_unwind(move || {
            {
                let mut file = File::create(path).unwrap();
                for record in records.iter() {
                    record.write(&mut file).unwrap();
                }
            }

            let mut count = 0;

            // Test going from beginning to end.
            let mut iter = WalIterator::new(path).unwrap();
            for (i, record) in iter.enumerate() {
                assert_eq!(record, records[i]);
                count += 1;
            }
            assert_eq!(count, 8);
            count = 0;

            // Test going from end to beginning.
            iter = WalIterator::new(path).unwrap();
            while let Some(record) = iter.next_back() {
                assert_eq!(record, records[records.len() - count - 1]);
                count += 1;
            }
            assert_eq!(count, 8);

            // TODO(DarinM223): test going forward and backward.
        });

        fs::remove_file(path).unwrap();
        if let Err(e) = result {
            panic!(e);
        }
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
