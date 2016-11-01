extern crate disk_utils;

use std::fs;
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom};
use std::panic;

use disk_utils::wal::record::{Record, RecordType};

#[test]
fn test_file_read_write() {
    let path: &'static str = "./files/record_test";
    let result = panic::catch_unwind(move || {
        let record = Record {
            crc: 123456789,
            size: 12345,
            record_type: RecordType::Full,
            payload: vec![123; 12345],
        };

        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)
            .unwrap();

        record.write(&mut file).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();

        let test_record = Record::read(&mut file).unwrap();
        assert_eq!(record, test_record);
    });

    fs::remove_file(path).unwrap();
    if let Err(e) = result {
        panic!(e);
    }
}

#[test]
fn test_single_byte_read_write() {
    let record = Record {
        crc: 123456789,
        size: 1,
        record_type: RecordType::Full,
        payload: vec![0],
    };

    let mut bytes = Vec::new();
    record.write(&mut bytes).unwrap();

    let test_record = Record::read(&mut &bytes[..]).unwrap();
    assert_eq!(record, test_record);
}

#[test]
fn test_read_write_invalid_record() {
    let mut bytes = vec![0; 100];
    if let Ok(_) = Record::read(&mut &bytes[..]) {
        panic!("Reading invalid record padded by zeros should return error");
    }

    bytes = vec![0; 1];
    if let Ok(_) = Record::read(&mut &bytes[..]) {
        panic!("Reading invalid record with a single zero should return error");
    }

    bytes = vec![1, 2, 3, 4, 5, 6];
    if let Ok(_) = Record::read(&mut &bytes[..]) {
        panic!("Reading invalid record with a smaller header size should return error");
    }

    bytes = vec![1, 2, 3, 4, 5, 6, 7, 0];
    if let Ok(_) = Record::read(&mut &bytes[..]) {
        panic!("Reading invalid record with a smaller data size should return error");
    }
}
