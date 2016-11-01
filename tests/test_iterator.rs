extern crate disk_utils;

use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom};
use std::panic;

use disk_utils::wal::iterator::WalIterator;
use disk_utils::wal::record::{BLOCK_SIZE, HEADER_SIZE, Record, RecordType};

fn test_file(file: &mut File, records: Vec<Record>) {
    // Test going from beginning to end.
    {
        let mut count = 0;
        let iter = WalIterator::new(file).unwrap();
        for (i, record) in iter.enumerate() {
            assert_eq!(record, records[i]);
            count += 1;
        }
        assert_eq!(count, records.len());
    }

    file.seek(SeekFrom::Start(0)).unwrap();

    // Test going from end to beginning.
    {
        let mut count = 0;
        let mut iter = WalIterator::new(file).unwrap();
        while let Some(record) = iter.next_back() {
            assert_eq!(record.payload.len(),
            records[records.len() - count - 1].payload.len());
            assert_eq!(record, records[records.len() - count - 1]);
            count += 1;
        }
        assert_eq!(count, records.len());
    }
}

#[test]
fn test_small_file() {
    let record = Record {
        crc: 123456789,
        size: 1,
        record_type: RecordType::Full,
        payload: vec![0],
    };

    let path: &'static str = "./files/small_file";
    let mut file = OpenOptions::new()
        .read(true)
        .append(true)
        .create(true)
        .open(path)
        .unwrap();
    let result = panic::catch_unwind(move || {
        record.write(&mut file).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();

        test_file(&mut file, vec![record]);
    });

    fs::remove_file(path).unwrap();
    if let Err(e) = result {
        panic!(e);
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
    let mut file = OpenOptions::new()
        .read(true)
        .append(true)
        .create(true)
        .open(path)
        .unwrap();
    let result = panic::catch_unwind(move || {
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
fn test_invalid_file() {
    // TODO(DarinM223): set up invalid file.
}
