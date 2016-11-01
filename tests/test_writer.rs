extern crate disk_utils;

use std::fs;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};
use std::panic;

use disk_utils::wal::iterator::WalIterator;
use disk_utils::wal::record::{BLOCK_SIZE, HEADER_SIZE, Record, RecordType};
use disk_utils::wal::writer::Writer;

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

#[test]
fn test_single_bytes() {
    let num_records = BLOCK_SIZE * 2;
    let mut records = Vec::with_capacity(num_records as usize);
    for i in 0..num_records {
        let record_type = match i {
            0 => RecordType::First,
            pos if pos == num_records - 1 => RecordType::Last,
            _ => RecordType::Middle,
        };

        records.push(Record {
            crc: 0,
            size: 1,
            record_type: record_type,
            payload: vec![0],
        });
    }

    let path: &'static str = "./files/single_byte_test";
    let mut file = OpenOptions::new()
        .read(true)
        .append(true)
        .create(true)
        .open(path)
        .unwrap();
    let result = panic::catch_unwind(move || {
        {
            let mut writer = Writer::new(&mut file);
            for record in records.iter() {
                writer.append(record).unwrap();
            }
        }

        file.seek(SeekFrom::Start(0)).unwrap();

        {
            let mut count = 0;
            let iter = WalIterator::new(&mut file).unwrap();
            for (i, record) in iter.enumerate() {
                assert_eq!(record, records[i]);
                count += 1;
            }
            assert_eq!(count, num_records);
        }
    });
    fs::remove_file(path).unwrap();
    if let Err(e) = result {
        panic!(e);
    }
}
