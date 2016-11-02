extern crate disk_utils;

use disk_utils::testing::create_test_file;
use disk_utils::wal::{LogData, read_serializable, read_serializable_backwards, Serializable, split_bytes_into_records};
use disk_utils::wal::entries::ChangeEntry;
use disk_utils::wal::iterator::WalIterator;
use disk_utils::wal::record::RecordType;
use disk_utils::wal::writer::Writer;

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
        old: "Hello world".to_string(),
    };

    let mut bytes = Vec::new();
    entry.serialize(&mut bytes).unwrap();
    let mut records = split_bytes_into_records(bytes.clone(), 2).unwrap();

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
            old: "Hello world".to_string(),
        };

        let mut bytes = Vec::new();
        entry.serialize(&mut bytes).unwrap();
        let records = split_bytes_into_records(bytes, 1).unwrap();
        {
            let mut writer = Writer::new(&mut file);
            for record in records.iter() {
                writer.append(record).unwrap();
            }
        }

        {
            let mut iter = WalIterator::new(&mut file).unwrap();
            let result_entry = read_serializable::<ChangeEntry<MyLogData>>(&mut iter).unwrap();
            assert_eq!(entry, result_entry);
        }
        {
            let mut iter = WalIterator::new(&mut file).unwrap();
            let result_entry = read_serializable_backwards::<ChangeEntry<MyLogData>>(&mut iter)
                .unwrap();
            assert_eq!(entry, result_entry);
        }
    }).unwrap();
}
