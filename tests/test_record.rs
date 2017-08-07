extern crate disk_utils;

use std::io::{Seek, SeekFrom};

use disk_utils::testing::create_test_file;
use disk_utils::wal::record::{Record, RecordType};

#[test]
fn test_file_read_write() {
    create_test_file("./files/record_test", |_, mut file| {
        let record = Record::new(RecordType::Full, vec![123; 12345]);
        record.write(&mut file).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();

        let test_record = Record::read(&mut file).unwrap();
        assert_eq!(record, test_record);
    }).unwrap();
}

#[test]
fn test_single_byte_read_write() {
    let record = Record::new(RecordType::Full, vec![0]);
    let mut bytes = Vec::new();
    record.write(&mut bytes).unwrap();

    let test_record = Record::read(&mut &bytes[..]).unwrap();
    assert_eq!(record, test_record);
}

#[test]
fn test_corrupted_record() {
    let record = Record::new(RecordType::Full, vec![123; 12345]);
    let mut bytes = Vec::new();
    record.write(&mut bytes).unwrap();

    // Modify a byte in the buffer.
    bytes[123] = 0;

    // Check that the corruption is detected.
    assert!(Record::read(&mut &bytes[..]).is_err());
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

#[test]
fn test_enum_primative() {
    assert_eq!(None, RecordType::from_u8(0 as u8));
    assert_eq!(Some(RecordType::Zero), RecordType::from_u8(1 as u8));
    assert_eq!(Some(RecordType::Full), RecordType::from_u8(2 as u8));
    assert_eq!(Some(RecordType::First), RecordType::from_u8(3 as u8));
    assert_eq!(Some(RecordType::Middle), RecordType::from_u8(4 as u8));
    assert_eq!(Some(RecordType::Last), RecordType::from_u8(5 as u8));
    assert_eq!(None, RecordType::from_u8(6 as u8));
}