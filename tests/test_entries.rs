extern crate disk_utils;

use disk_utils::wal::{LogData, Serializable};
use disk_utils::wal::entries::{ChangeEntry, Checkpoint, InsertEntry};

#[derive(Clone, PartialEq, Debug)]
struct MyLogData;

impl LogData for MyLogData {
    type Key = i32;
    type Value = String;
}

#[test]
fn test_insert_entry() {
    let entry: InsertEntry<MyLogData> = InsertEntry {
        tid: 123,
        key: 20,
    };

    let mut bytes = Vec::new();
    entry.serialize(&mut bytes).unwrap();

    let test_entry = InsertEntry::deserialize(&mut &bytes[..]).unwrap();
    assert_eq!(entry, test_entry);
}

#[test]
fn test_change_entry() {
    let entry: ChangeEntry<MyLogData> = ChangeEntry {
        tid: 123,
        key: 20,
        old: "Hello world!".to_string(),
    };

    let mut bytes = Vec::new();
    entry.serialize(&mut bytes).unwrap();

    let test_entry = ChangeEntry::deserialize(&mut &bytes[..]).unwrap();
    assert_eq!(entry, test_entry);
}

#[test]
fn test_checkpoint() {
    let checkpoints = vec![Checkpoint::Begin(vec![20, 30]), Checkpoint::End];

    for checkpoint in checkpoints {
        let mut bytes = Vec::new();
        checkpoint.serialize(&mut bytes).unwrap();

        let test_checkpoint = Checkpoint::deserialize(&mut &bytes[..]).unwrap();
        assert_eq!(checkpoint, test_checkpoint);
    }
}
