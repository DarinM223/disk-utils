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
    use std::fs;
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom};
    use std::panic;
    use super::*;
    use super::super::iterator::WalIterator;
    use super::super::record::{BLOCK_SIZE, HEADER_SIZE, Record, RecordType};

    #[test]
    fn test_no_padding_on_same_block() {
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

        let direct_write_path: &'static str = "./files/direct_write_file";
        let writer_file_path: &'static str = "./files/writer_file_path";
        let mut direct_write_file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(direct_write_path)
            .unwrap();
        let mut writer_file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(writer_file_path)
            .unwrap();
        let result = panic::catch_unwind(move || {
            for record in records.iter() {
                record.write(&mut direct_write_file).unwrap();
            }
            direct_write_file.seek(SeekFrom::Start(0)).unwrap();

            {
                let mut writer = Writer::new(&mut writer_file);
                for record in records.iter() {
                    writer.append(record).unwrap();
                }
            }
            writer_file.seek(SeekFrom::Start(0)).unwrap();

            let mut num_comparisons = 0;
            let file_len = direct_write_file.metadata().unwrap().len();
            for (b1, b2) in direct_write_file.bytes().zip(writer_file.bytes()) {
                assert_eq!(b1.unwrap(), b2.unwrap());
                num_comparisons += 1;
            }
            assert_eq!(num_comparisons, file_len);
        });

        fs::remove_file(direct_write_path).unwrap();
        fs::remove_file(writer_file_path).unwrap();
        if let Err(e) = result {
            panic!(e);
        }
    }

    #[test]
    fn test_padding_before_new_block() {
        let record_size = (BLOCK_SIZE / 3) as u16;
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
        let direct_write_path: &'static str = "./files/direct_write_file2";
        let writer_file_path: &'static str = "./files/writer_file_path2";
        let mut direct_write_file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(direct_write_path)
            .unwrap();
        let mut writer_file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(writer_file_path)
            .unwrap();
        let result = panic::catch_unwind(move || {
            for record in records.iter() {
                record.write(&mut direct_write_file).unwrap();
            }
            direct_write_file.seek(SeekFrom::Start(0)).unwrap();

            {
                let mut writer = Writer::new(&mut writer_file);
                for record in records.iter() {
                    writer.append(record).unwrap();
                }
            }
            writer_file.seek(SeekFrom::Start(0)).unwrap();

            let direct_write_file_len = direct_write_file.metadata().unwrap().len();
            let writer_file_len = writer_file.metadata().unwrap().len();
            assert!(direct_write_file_len != writer_file_len);

            {
                let mut count = 0;
                let iter = WalIterator::new(&mut writer_file).unwrap();
                for (i, record) in iter.enumerate() {
                    assert_eq!(record, records[i]);
                    count += 1;
                }
                assert_eq!(count, 8);
            }
        });

        fs::remove_file(direct_write_path).unwrap();
        fs::remove_file(writer_file_path).unwrap();
        if let Err(e) = result {
            panic!(e);
        }
    }
}
