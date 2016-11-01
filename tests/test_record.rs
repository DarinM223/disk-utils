extern crate disk_utils;

use std::io::{Seek, SeekFrom};

use disk_utils::testing::create_test_file;
use disk_utils::wal::record::{Record, RecordType};

#[test]
fn test_file_read_write() {
    create_test_file("./files/record_test", |_, mut file| {
        let record = Record {
            crc: 123456789,
            size: 12345,
            record_type: RecordType::Full,
            payload: vec![123; 12345],
        };

        record.write(&mut file).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();

        let test_record = Record::read(&mut file).unwrap();
        assert_eq!(record, test_record);
    }).unwrap();
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
