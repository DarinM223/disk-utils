use super::record::{BLOCK_SIZE, Record};
use std::fs::File;
use std::io;
use std::io::{Read, Seek, SeekFrom};

/// Calls a method for an object contained inside
/// an option and returns an option of the result.
macro_rules! call_opt {
    ($var:expr, $meth:ident($( $param:expr ),*)) => (match $var {
        Some(ref v) => Some(v.$meth($($param),*)),
        None => None,
    });
}

/// Iterator that reads through the write ahead log.
pub struct WalIterator<'a> {
    file: &'a mut File,
    file_len: i64,
    block: Option<Vec<Record>>,
    block_index: Option<i32>,
    pos: Option<i64>,
}

impl<'a> WalIterator<'a> {
    pub fn new<'b>(file: &'b mut File) -> io::Result<WalIterator<'b>> {
        let file_len = file.metadata()?.len() as i64;
        Ok(WalIterator {
            file: file,
            file_len: file_len,
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
                if let Some(mut pos) = self.pos.take() {
                    pos -= BLOCK_SIZE;
                    if check_forward_bounds(pos, self.file_len) {
                        return Ok(true);
                    }
                    self.fetch_block(pos)?;
                    self.pos = Some(pos);
                    call_opt!(self.block, len()).map(|len| {
                        self.block_index = Some(len as i32 - 1);
                    });
                }
            } else if block_index as usize >= block_len {
                if let Some(mut pos) = self.pos.take() {
                    pos += BLOCK_SIZE;
                    if check_forward_bounds(pos, self.file_len) {
                        return Ok(true);
                    }
                    self.fetch_block(pos)?;
                    self.pos = Some(pos);
                    self.block_index = Some(0);
                }
            }
        } else {
            let out_of_bounds = match forward {
                true => check_forward_bounds(position, self.file_len),
                false => check_backward_bounds(position, self.file_len),
            };
            if out_of_bounds {
                return Ok(true);
            }
            self.fetch_block(position)?;

            call_opt!(self.block, len()).map(move |len| {
                match forward {
                    true => self.block_index = Some(0),
                    false => self.block_index = Some(len as i32 - 1),
                }
            });
        }

        Ok(false)
    }
}

impl<'a> Iterator for WalIterator<'a> {
    type Item = Record;

    /// Given the current position, return the record at the position and
    /// increment into the next record.
    fn next(&mut self) -> Option<Record> {
        let pos = self.get_pos(0);
        let out_of_bounds = self.load_block(pos, true).unwrap();
        if out_of_bounds {
            return None;
        }

        self.block_index.take().map(|block_index| {
            let next = call_opt!(self.block, get(block_index as usize)).unwrap().unwrap();
            self.block_index = Some(block_index + 1);
            next.clone()
        })
    }
}

impl<'a> DoubleEndedIterator for WalIterator<'a> {
    fn next_back(&mut self) -> Option<Record> {
        let end_pos = self.file_len - BLOCK_SIZE;
        let pos = self.get_pos(end_pos);
        let out_of_bounds = self.load_block(pos, false).unwrap();
        if out_of_bounds {
            return None;
        }

        self.block_index.take().map(|block_index| {
            let next = call_opt!(self.block, get(block_index as usize)).unwrap().unwrap();
            self.block_index = Some(block_index - 1);
            next.clone()
        })
    }
}

fn check_forward_bounds(position: i64, file_length: i64) -> bool {
    if position < 0 || position + BLOCK_SIZE > file_length {
        return true;
    }
    false
}

fn check_backward_bounds(position: i64, file_length: i64) -> bool {
    if position - BLOCK_SIZE < 0 || position > file_length {
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::fs::{File, OpenOptions};
    use std::io::{Seek, SeekFrom};
    use std::panic;
    use super::*;
    use super::super::record::{BLOCK_SIZE, HEADER_SIZE, Record, RecordType};

    fn test_file(file: &mut File, records: Vec<Record>) {
        // Test going from beginning to end.
        {
            let mut count = 0;
            let iter = WalIterator::new(file).unwrap();
            for (i, record) in iter.enumerate() {
                assert_eq!(record, records[i]);
                count += 1;
            }
            assert_eq!(count, 8);
        }

        file.seek(SeekFrom::Start(0)).unwrap();

        // Test going from end to beginning.
        {
            let mut count = 0;
            let mut iter = WalIterator::new(file).unwrap();
            while let Some(record) = iter.next_back() {
                assert_eq!(record, records[records.len() - count - 1]);
                count += 1;
            }
            assert_eq!(count, 8);
        }
    }

    #[test]
    fn test_perfect_file() {
        let record_size = (BLOCK_SIZE / 4) as u16;
        let payload_size = record_size - HEADER_SIZE as u16;
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
            let mut file = OpenOptions::new()
                .read(true)
                .append(true)
                .create(true)
                .open(path)
                .unwrap();
            for record in records.iter() {
                record.write(&mut file).unwrap();
            }
            file.seek(SeekFrom::Start(0)).unwrap();

            test_file(&mut file, records);
        });

        fs::remove_file(path).unwrap();
        if let Err(e) = result {
            panic!(e);
        }
    }

    #[test]
    fn test_padding_file() {
        // TODO(DarinM223): set up file with padding.
    }

    #[test]
    fn test_invalid_file() {
        // TODO(DarinM223): set up invalid file.
    }
}
