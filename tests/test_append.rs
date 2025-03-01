extern crate disk_utils;

use std::io::{Read, Seek, SeekFrom};

use disk_utils::testing::{create_test_file, create_two_test_files};
use disk_utils::wal::append_to_file;
use disk_utils::wal::iterator::{ReadDirection, WalIterator};
use disk_utils::wal::record::{BLOCK_SIZE, HEADER_SIZE, Record, RecordType};

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

        records.push(Record::new(record_type, vec![123; payload_size as usize]));
    }

    create_two_test_files(
        "./files/direct_write_file",
        "./files/writer_file_path",
        move |_, _, mut direct_write_file, mut writer_file| {
            for record in records.iter() {
                record.write(&mut direct_write_file).unwrap();
            }
            direct_write_file.seek(SeekFrom::Start(0)).unwrap();

            for record in records.iter() {
                append_to_file(&mut writer_file, record).unwrap();
            }
            writer_file.seek(SeekFrom::Start(0)).unwrap();

            let mut num_comparisons = 0;
            let file_len = direct_write_file.metadata().unwrap().len();
            for (b1, b2) in direct_write_file.bytes().zip(writer_file.bytes()) {
                assert_eq!(b1.unwrap(), b2.unwrap());
                num_comparisons += 1;
            }
            assert_eq!(num_comparisons, file_len);
        },
    )
    .unwrap();
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

        records.push(Record::new(record_type, vec![123; payload_size as usize]));
    }

    create_two_test_files(
        "./files/direct_write_file2",
        "./files/writer_file_path2",
        move |_, _, mut direct_write_file, mut writer_file| {
            for record in records.iter() {
                record.write(&mut direct_write_file).unwrap();
            }

            for record in records.iter() {
                append_to_file(&mut writer_file, record).unwrap();
            }

            let direct_write_file_len = direct_write_file.metadata().unwrap().len();
            let writer_file_len = writer_file.metadata().unwrap().len();
            assert!(direct_write_file_len != writer_file_len);

            let mut count = 0;
            let iter = WalIterator::new(&mut writer_file, ReadDirection::Forward).unwrap();
            for (i, record) in iter.enumerate() {
                assert_eq!(record, records[i]);
                count += 1;
            }
            assert_eq!(count, 8);
        },
    )
    .unwrap();
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

        records.push(Record::new(record_type, vec![0]));
    }

    create_test_file("./files/single_byte_test", move |_, mut file| {
        for record in records.iter() {
            append_to_file(&mut file, record).unwrap();
        }

        let mut count = 0;
        let iter = WalIterator::new(&mut file, ReadDirection::Forward).unwrap();
        for (i, record) in iter.enumerate() {
            assert_eq!(record, records[i]);
            count += 1;
        }
        assert_eq!(count, num_records);
    })
    .unwrap();
}
