extern crate disk_utils;

use std::fs::File;
use std::io::{Seek, SeekFrom};

use disk_utils::testing::create_test_file;
use disk_utils::wal::iterator::{ReadDirection, WalIterator};
use disk_utils::wal::record::{BLOCK_SIZE, HEADER_SIZE, Record, RecordType};

fn test_file(file: &mut File, records: Vec<Record>) {
    // Test going from beginning to end.
    let mut count = 0;
    let iter = WalIterator::new(file, ReadDirection::Forward).unwrap();
    for (i, record) in iter.enumerate() {
        assert_eq!(record, records[i]);
        count += 1;
    }
    assert_eq!(count, records.len());

    file.seek(SeekFrom::Start(0)).unwrap();

    // Test going from end to beginning.
    let mut count = 0;
    let mut iter = WalIterator::new(file, ReadDirection::Backward).unwrap();
    while let Some(record) = iter.next_back() {
        assert_eq!(
            record.payload.len(),
            records[records.len() - count - 1].payload.len()
        );
        assert_eq!(record, records[records.len() - count - 1]);
        count += 1;
    }
    assert_eq!(count, records.len());
}

#[test]
fn test_small_file() {
    create_test_file("./files/small_file", |_, mut file| {
        let record = Record::new(RecordType::Full, vec![0]);
        record.write(&mut file).unwrap();

        test_file(&mut file, vec![record]);
    })
    .unwrap();
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

        records.push(Record::new(record_type, vec![123; payload_size as usize]));
    }

    create_test_file("./files/perfect_file", move |_, mut file| {
        for record in records.iter() {
            record.write(&mut file).unwrap();
        }

        test_file(&mut file, records);
    })
    .unwrap();
}

#[test]
fn test_back_and_forth() {
    let record1 = Record::new(RecordType::First, vec![0; 1]);
    let record2 = Record::new(RecordType::Middle, vec![1; 1]);
    let record3 = Record::new(RecordType::Last, vec![2; 1]);

    create_test_file("./files/back_and_forth", move |_, mut file| {
        record1.write(&mut file).unwrap();
        record2.write(&mut file).unwrap();
        record3.write(&mut file).unwrap();

        let mut iter = WalIterator::new(&mut file, ReadDirection::Forward).unwrap();
        assert_eq!(iter.next(), Some(record1.clone()));
        assert_eq!(iter.next(), Some(record2.clone()));
        assert_eq!(iter.next_back(), Some(record2.clone()));
        assert_eq!(iter.next_back(), Some(record1.clone()));
    })
    .unwrap();
}
