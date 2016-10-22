use super::record::{BLOCK_SIZE, Record};
use std::fs::File;
use std::io;
use std::io::Write;

pub struct Writer<'a> {
    file: &'a mut File,
}

impl<'a> Writer<'a> {
    pub fn new<'b>(file: &'b mut File) -> Writer<'b> {
        Writer { file: file }
    }

    pub fn append(&mut self, record: &Record) -> io::Result<()> {
        let file_len = self.file.metadata()?.len();
        let curr_block_len = file_len - (file_len / BLOCK_SIZE as u64) * BLOCK_SIZE as u64;
        if curr_block_len + record.payload.len() as u64 > BLOCK_SIZE as u64 {
            let padding_len = BLOCK_SIZE as u64 - curr_block_len;
            let padding = vec![0; padding_len as usize];
            self.file.write(&padding[..])?;
        }

        record.write(&mut self.file)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_no_padding_on_same_block() {}

    #[test]
    fn test_padding_before_new_block() {}
}
