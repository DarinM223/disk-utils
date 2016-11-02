extern crate disk_utils;

use std::collections::HashMap;
use std::io;
use std::sync::{Arc, RwLock};

use disk_utils::testing::create_test_file;
use disk_utils::wal::{LogData, read_serializable};
use disk_utils::wal::entries::{ChangeEntry, InsertEntry, Transaction};
use disk_utils::wal::iterator::WalIterator;
use disk_utils::wal::undo_log::{UndoLog, UndoLogEntry, UndoLogStore};

#[derive(Clone, PartialEq, Debug)]
struct MyLogData;

impl LogData for MyLogData {
    type Key = i32;
    type Value = String;
}

#[derive(Clone)]
struct MyStore<Data: LogData> {
    map: Arc<RwLock<HashMap<Data::Key, Data::Value>>>,
    flush_err: Arc<RwLock<bool>>,
}

impl<Data> MyStore<Data>
    where Data: LogData
{
    pub fn new() -> MyStore<Data> {
        MyStore {
            map: Arc::new(RwLock::new(HashMap::new())),
            flush_err: Arc::new(RwLock::new(false)),
        }
    }

    pub fn set_flush_err(&mut self, flush_err: bool) {
        *self.flush_err.write().unwrap() = flush_err;
    }
}

impl<Data> UndoLogStore<Data> for MyStore<Data>
    where Data: LogData
{
    fn get(&self, key: &Data::Key) -> Option<Data::Value> {
        self.map.read().unwrap().get(key).cloned()
    }

    fn remove(&mut self, key: &Data::Key) {
        self.map.write().unwrap().remove(key);
    }

    fn update(&mut self, key: Data::Key, val: Data::Value) {
        self.map.write().unwrap().insert(key, val);
    }

    fn flush(&mut self) -> io::Result<()> {
        if *self.flush_err.read().unwrap() {
            Err(io::Error::new(io::ErrorKind::Interrupted, "Flush error occurred"))
        } else {
            Ok(())
        }
    }
}

#[test]
fn test_start() {
    create_test_file("./files/start_undo_log", |path, _| {
        let store: MyStore<MyLogData> = MyStore::new();
        let mut undo_log = UndoLog::new(path, store).unwrap();
        let tid = undo_log.start();

        assert_eq!(tid, 1);
        assert_eq!(undo_log.entries().len(), 1);
        assert_eq!(undo_log.entries()[0], UndoLogEntry::Transaction(Transaction::Start(1)));
    }).unwrap();
}

#[test]
fn test_write() {
    create_test_file("./files/write_undo_log", |path, _| {
        let store: MyStore<MyLogData> = MyStore::new();
        let mut undo_log = UndoLog::new(path, store).unwrap();

        let tid = undo_log.start();
        assert_eq!(tid, 1);

        undo_log.write(tid, 20, "Hello".to_string());

        assert_eq!(undo_log.entries().len(), 2);
        assert_eq!(undo_log.entries()[1], UndoLogEntry::InsertEntry(InsertEntry { tid: 1, key: 20 }));

        undo_log.write(tid, 20, "World".to_string());

        assert_eq!(undo_log.entries().len(), 3);
        assert_eq!(undo_log.entries()[2],
                   UndoLogEntry::ChangeEntry(ChangeEntry {
                       tid: 1,
                       key: 20,
                       old: "Hello".to_string(),
                   }));
    }).unwrap();
}

#[test]
fn test_commit() {
    create_test_file("./files/commit_undo_log", |path, mut file| {
        let store: MyStore<MyLogData> = MyStore::new();
        let mut undo_log = UndoLog::new(path, store).unwrap();
        let tid = undo_log.start();
        assert_eq!(tid, 1);
        undo_log.write(tid, 20, "Hello".to_string());
        undo_log.write(tid, 20, "World".to_string());
        undo_log.commit(tid).unwrap();

        let mut expected_entries =
            vec![UndoLogEntry::Transaction(Transaction::Start(1)),
                 UndoLogEntry::InsertEntry(InsertEntry { tid: 1, key: 20 }),
                 UndoLogEntry::ChangeEntry(ChangeEntry {
                     tid: 1,
                     key: 20,
                     old: "Hello".to_string(),
                 }),
                 UndoLogEntry::Transaction(Transaction::Commit(1))]
                .into_iter();
        let mut iter = WalIterator::new(&mut file).unwrap();
        while let Ok(data) = read_serializable::<UndoLogEntry<MyLogData>>(&mut iter) {
            assert_eq!(data, expected_entries.next().unwrap());
        }
    }).unwrap();
}

#[test]
fn test_recover() {
    create_test_file("./files/recover_undo_log", |path, mut file| {
        let mut store: MyStore<MyLogData> = MyStore::new();
        {
            let mut undo_log = UndoLog::new(path, store.clone()).unwrap();
            let tid = undo_log.start();
            undo_log.write(tid, 20, "Hello".to_string());
            undo_log.commit(tid).unwrap();

            store.set_flush_err(true);

            let tid = undo_log.start();
            undo_log.write(tid, 20, "World".to_string());
            undo_log.write(tid, 30, "Hello".to_string());
            assert!(undo_log.commit(tid).is_err());

            store.set_flush_err(false);
        }

        // Create a new undo log which should automatically recover data.
        let mut undo_log = UndoLog::new(path, store.clone()).unwrap();
        assert_eq!(undo_log.start(), 3);

        let mut expected_entries =
            vec![UndoLogEntry::Transaction(Transaction::Start(1)),
                 UndoLogEntry::InsertEntry(InsertEntry { tid: 1, key: 20 }),
                 UndoLogEntry::Transaction(Transaction::Commit(1)),
                 UndoLogEntry::Transaction(Transaction::Start(2)),
                 UndoLogEntry::ChangeEntry(ChangeEntry {
                     tid: 2,
                     key: 20,
                     old: "Hello".to_string(),
                 }),
                 UndoLogEntry::InsertEntry(InsertEntry { tid: 2, key: 30 }),
                 UndoLogEntry::Transaction(Transaction::Abort(2))]
                .into_iter();
        let mut iter = WalIterator::new(&mut file).unwrap();
        while let Ok(data) = read_serializable::<UndoLogEntry<MyLogData>>(&mut iter) {
            assert_eq!(data, expected_entries.next().unwrap());
        }

        assert_eq!(store.get(&20), Some("Hello".to_string()));
        assert_eq!(store.get(&30), None);
    }).unwrap();
}

#[test]
fn test_multiple_recover() {
    create_test_file("./files/multiple_recover_undo_log", |path, mut file| {
        let mut store: MyStore<MyLogData> = MyStore::new();
        {
            let mut undo_log = UndoLog::new(path, store.clone()).unwrap();
            let tid1 = undo_log.start();
            let tid2 = undo_log.start();
            undo_log.write(tid1, 20, "Hello".to_string());
            undo_log.write(tid2, 30, "World".to_string());
            undo_log.write(tid1, 30, "Blah".to_string());
            undo_log.commit(tid1).unwrap();
            undo_log.write(tid2, 20, "World".to_string());
            undo_log.commit(tid2).unwrap();

            let tid3 = undo_log.start();
            let tid4 = undo_log.start();

            undo_log.write(tid3, 40, "Foo".to_string());
            undo_log.write(tid4, 30, "Bar".to_string());
            undo_log.commit(tid3).unwrap();

            undo_log.write(tid4, 50, "Hello".to_string());
            store.set_flush_err(true);
            assert!(undo_log.commit(tid4).is_err());
            store.set_flush_err(false);
        }

        // Create a new undo log which should automatically recover data.
        let mut undo_log = UndoLog::new(path, store.clone()).unwrap();
        assert_eq!(undo_log.start(), 5);

        let mut expected_entries =
            vec![UndoLogEntry::Transaction(Transaction::Start(1)),
                 UndoLogEntry::Transaction(Transaction::Start(2)),
                 UndoLogEntry::InsertEntry(InsertEntry { tid: 1, key: 20 }),
                 UndoLogEntry::InsertEntry(InsertEntry { tid: 2, key: 30 }),
                 UndoLogEntry::ChangeEntry(ChangeEntry {
                     tid: 1,
                     key: 30,
                     old: "World".to_string(),
                 }),
                 UndoLogEntry::Transaction(Transaction::Commit(1)),
                 UndoLogEntry::ChangeEntry(ChangeEntry {
                     tid: 2,
                     key: 20,
                     old: "Hello".to_string(),
                 }),
                 UndoLogEntry::Transaction(Transaction::Commit(2)),
                 UndoLogEntry::Transaction(Transaction::Start(3)),
                 UndoLogEntry::Transaction(Transaction::Start(4)),
                 UndoLogEntry::InsertEntry(InsertEntry { tid: 3, key: 40 }),
                 UndoLogEntry::ChangeEntry(ChangeEntry {
                     tid: 4,
                     key: 30,
                     old: "Blah".to_string(),
                 }),
                 UndoLogEntry::Transaction(Transaction::Commit(3)),
                 UndoLogEntry::InsertEntry(InsertEntry { tid: 4, key: 50 }),
                 UndoLogEntry::Transaction(Transaction::Abort(4))]
                .into_iter();
        let mut iter = WalIterator::new(&mut file).unwrap();
        while let Ok(data) = read_serializable::<UndoLogEntry<MyLogData>>(&mut iter) {
            assert_eq!(data, expected_entries.next().unwrap());
        }

        // Expected state after recovery:
        // 20 -> "World"
        // 30 -> "Blah"
        // 40 -> "Foo"
        // 50 -> None
        assert_eq!(store.get(&20), Some("World".to_string()));
        assert_eq!(store.get(&30), Some("Blah".to_string()));
        assert_eq!(store.get(&40), Some("Foo".to_string()));
        assert_eq!(store.get(&50), None);
    }).unwrap();
}
