extern crate disk_utils;

use disk_utils::testing::create_test_file;
use disk_utils::wal::entries::ChangeEntry;
use disk_utils::wal::iterator::{ReadDirection, WalIterator};
use disk_utils::wal::record::RecordType;
use disk_utils::wal::{
    append_to_file, read_serializable, read_serializable_backwards, split_bytes_into_records,
    LogData,
};
use disk_utils::Serializable;

#[derive(Clone, PartialEq, Debug)]
struct MyLogData;

impl LogData for MyLogData {
    type Key = i32;
    type Value = String;
}

#[test]
fn test_split_bytes() {
    let entry: ChangeEntry<MyLogData> = ChangeEntry {
        tid: 123,
        key: 20,
        value: "Hello world".to_string(),
    };

    let mut bytes = Vec::new();
    entry.serialize(&mut bytes).unwrap();
    let mut records = split_bytes_into_records(&bytes, 2).unwrap();

    assert_eq!(records[0].record_type, RecordType::First);
    for i in 1..(records.len() - 1) {
        assert_eq!(records[i].record_type, RecordType::Middle);
    }
    assert_eq!(records[records.len() - 1].record_type, RecordType::Last);

    let mut buf = Vec::new();
    for record in records.iter_mut() {
        buf.append(&mut record.payload);
    }

    for (b1, b2) in bytes.iter().zip(buf.iter()) {
        assert_eq!(b1, b2);
    }
}

#[test]
fn test_read_serializable() {
    create_test_file("./files/read_serializable_test", |_, mut file| {
        let entry = ChangeEntry {
            tid: 123,
            key: 20,
            value: "Hello world".to_string(),
        };

        let mut bytes = Vec::new();
        entry.serialize(&mut bytes).unwrap();
        let records = split_bytes_into_records(&bytes, 1).unwrap();
        for record in records.iter() {
            append_to_file(&mut file, record).unwrap();
        }

        let mut iter = WalIterator::new(&mut file, ReadDirection::Forward).unwrap();
        let result_entry = read_serializable::<ChangeEntry<MyLogData>>(&mut iter).unwrap();
        assert_eq!(entry, result_entry);

        let mut iter = WalIterator::new(&mut file, ReadDirection::Backward).unwrap();
        let result_entry =
            read_serializable_backwards::<ChangeEntry<MyLogData>>(&mut iter).unwrap();
        assert_eq!(entry, result_entry);
    })
    .unwrap();
}

#[test]
fn test_read_serializable_back_and_forth() {
    create_test_file("./files/read_serializable_back_and_forth", |_, mut file| {
        let entries: Vec<ChangeEntry<MyLogData>> = vec![
            ChangeEntry {
                tid: 123,
                key: 20,
                value: "Hello world!".to_string(),
            },
            ChangeEntry {
                tid: 234,
                key: 50,
                value: "Foo Bar".to_string(),
            },
            ChangeEntry {
                tid: 90,
                key: 60,
                value: "ABC".to_string(),
            },
        ];

        for entry in entries.iter() {
            let mut bytes = Vec::new();
            entry.serialize(&mut bytes).unwrap();
            let records = split_bytes_into_records(&bytes, 1).unwrap();
            for record in records.iter() {
                append_to_file(&mut file, record).unwrap();
            }
        }

        let mut iter = WalIterator::new(&mut file, ReadDirection::Forward).unwrap();
        assert_eq!(
            read_serializable::<ChangeEntry<MyLogData>>(&mut iter).unwrap(),
            entries[0]
        );
        assert_eq!(
            read_serializable::<ChangeEntry<MyLogData>>(&mut iter).unwrap(),
            entries[1]
        );
        assert_eq!(
            read_serializable_backwards::<ChangeEntry<MyLogData>>(&mut iter).unwrap(),
            entries[1]
        );
        assert_eq!(
            read_serializable_backwards::<ChangeEntry<MyLogData>>(&mut iter).unwrap(),
            entries[0]
        );
    })
    .unwrap();
}
