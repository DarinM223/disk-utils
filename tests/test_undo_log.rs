extern crate disk_utils;

use std::collections::HashMap;
use std::io;
use std::sync::{Arc, RwLock};

use disk_utils::testing::create_test_file;
use disk_utils::wal::{LogData, LogStore, read_serializable};
use disk_utils::wal::entries::{ChangeEntry, Checkpoint, InsertEntry, SingleLogEntry, Transaction};
use disk_utils::wal::iterator::WalIterator;
use disk_utils::wal::undo_log::UndoLog;

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

impl<Data> LogStore<Data> for MyStore<Data>
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

    fn flush_change(&mut self, _: Data::Key, _: Data::Value) -> io::Result<()> {
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
        assert_eq!(undo_log.entries()[0], SingleLogEntry::Transaction(Transaction::Start(1)));
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
        assert_eq!(undo_log.entries()[1], SingleLogEntry::InsertEntry(InsertEntry { tid: 1, key: 20 }));

        undo_log.write(tid, 20, "World".to_string());

        assert_eq!(undo_log.entries().len(), 3);
        assert_eq!(undo_log.entries()[2],
                   SingleLogEntry::ChangeEntry(ChangeEntry {
                       tid: 1,
                       key: 20,
                       value: "Hello".to_string(),
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
            vec![SingleLogEntry::Transaction(Transaction::Start(1)),
                 SingleLogEntry::InsertEntry(InsertEntry { tid: 1, key: 20 }),
                 SingleLogEntry::ChangeEntry(ChangeEntry {
                     tid: 1,
                     key: 20,
                     value: "Hello".to_string(),
                 }),
                 SingleLogEntry::Transaction(Transaction::Commit(1))]
                .into_iter();
        let mut iter = WalIterator::new(&mut file).unwrap();
        while let Ok(data) = read_serializable::<SingleLogEntry<MyLogData>>(&mut iter) {
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
            vec![SingleLogEntry::Transaction(Transaction::Start(1)),
                 SingleLogEntry::InsertEntry(InsertEntry { tid: 1, key: 20 }),
                 SingleLogEntry::Transaction(Transaction::Commit(1)),
                 SingleLogEntry::Transaction(Transaction::Start(2)),
                 SingleLogEntry::ChangeEntry(ChangeEntry {
                     tid: 2,
                     key: 20,
                     value: "Hello".to_string(),
                 }),
                 SingleLogEntry::InsertEntry(InsertEntry { tid: 2, key: 30 }),
                 SingleLogEntry::Transaction(Transaction::Abort(2))]
                .into_iter();
        let mut iter = WalIterator::new(&mut file).unwrap();
        while let Ok(data) = read_serializable::<SingleLogEntry<MyLogData>>(&mut iter) {
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
            vec![SingleLogEntry::Transaction(Transaction::Start(1)),
                 SingleLogEntry::Transaction(Transaction::Start(2)),
                 SingleLogEntry::InsertEntry(InsertEntry { tid: 1, key: 20 }),
                 SingleLogEntry::InsertEntry(InsertEntry { tid: 2, key: 30 }),
                 SingleLogEntry::ChangeEntry(ChangeEntry {
                     tid: 1,
                     key: 30,
                     value: "World".to_string(),
                 }),
                 SingleLogEntry::Transaction(Transaction::Commit(1)),
                 SingleLogEntry::ChangeEntry(ChangeEntry {
                     tid: 2,
                     key: 20,
                     value: "Hello".to_string(),
                 }),
                 SingleLogEntry::Transaction(Transaction::Commit(2)),
                 SingleLogEntry::Transaction(Transaction::Start(3)),
                 SingleLogEntry::Transaction(Transaction::Start(4)),
                 SingleLogEntry::InsertEntry(InsertEntry { tid: 3, key: 40 }),
                 SingleLogEntry::ChangeEntry(ChangeEntry {
                     tid: 4,
                     key: 30,
                     value: "Blah".to_string(),
                 }),
                 SingleLogEntry::Transaction(Transaction::Commit(3)),
                 SingleLogEntry::InsertEntry(InsertEntry { tid: 4, key: 50 }),
                 SingleLogEntry::Transaction(Transaction::Abort(4))]
                .into_iter();
        let mut iter = WalIterator::new(&mut file).unwrap();
        while let Ok(data) = read_serializable::<SingleLogEntry<MyLogData>>(&mut iter) {
            assert_eq!(data, expected_entries.next().unwrap());
        }

        // Test expected state after recovery:
        assert_eq!(store.get(&20), Some("World".to_string()));
        assert_eq!(store.get(&30), Some("Blah".to_string()));
        assert_eq!(store.get(&40), Some("Foo".to_string()));
        assert_eq!(store.get(&50), None);
    }).unwrap();
}

#[test]
fn test_add_end_checkpoint() {
    create_test_file("./files/add_end_checkpoint", |path, mut file| {
        let store: MyStore<MyLogData> = MyStore::new();
        {
            let mut undo_log = UndoLog::new(path, store.clone()).unwrap();
            let tid1 = undo_log.start();
            let tid2 = undo_log.start();
            undo_log.commit(tid1).unwrap();
            let tid3 = undo_log.start();
            let tid4 = undo_log.start();
            undo_log.checkpoint().unwrap();
            undo_log.commit(tid3).unwrap();
            undo_log.commit(tid4).unwrap();
            undo_log.commit(tid2).unwrap();
        }

        let mut undo_log = UndoLog::new(path, store.clone()).unwrap();
        assert_eq!(undo_log.start(), 5);

        let mut expected_entries =
            vec![SingleLogEntry::Transaction(Transaction::Start(1)),
                 SingleLogEntry::Transaction(Transaction::Start(2)),
                 SingleLogEntry::Transaction(Transaction::Commit(1)),
                 SingleLogEntry::Transaction(Transaction::Start(3)),
                 SingleLogEntry::Transaction(Transaction::Start(4)),
                 SingleLogEntry::Checkpoint(Checkpoint::Begin(vec![2, 3, 4])),
                 SingleLogEntry::Transaction(Transaction::Commit(3)),
                 SingleLogEntry::Transaction(Transaction::Commit(4)),
                 SingleLogEntry::Transaction(Transaction::Commit(2)),
                 SingleLogEntry::Checkpoint(Checkpoint::End)]
                .into_iter();
        let mut iter = WalIterator::new(&mut file).unwrap();
        while let Ok(data) = read_serializable::<SingleLogEntry<MyLogData>>(&mut iter) {
            if let SingleLogEntry::Checkpoint(Checkpoint::Begin(mut data)) = data {
                data.sort();
                assert_eq!(SingleLogEntry::Checkpoint(Checkpoint::Begin(data)),
                           expected_entries.next().unwrap());
            } else {
                assert_eq!(data, expected_entries.next().unwrap());
            }
        }
    }).unwrap();
}

#[test]
fn test_checkpoint_recover_before_end() {
    create_test_file("./files/checkpoint_recover_before_end", |path, _| {
        let mut store: MyStore<MyLogData> = MyStore::new();
        {
            let mut undo_log = UndoLog::new(path, store.clone()).unwrap();
            let tid1 = undo_log.start();
            let tid2 = undo_log.start();

            undo_log.write(tid1, 20, "Hello".to_string());
            undo_log.write(tid2, 20, "World".to_string());
            undo_log.write(tid2, 30, "Blah".to_string());
            undo_log.write(tid1, 30, "Foo".to_string());

            undo_log.commit(tid1).unwrap();
            undo_log.commit(tid2).unwrap();

            let tid3 = undo_log.start();
            let tid4 = undo_log.start();
            let tid5 = undo_log.start();

            undo_log.write(tid3, 20, "A".to_string());
            undo_log.write(tid5, 30, "B".to_string());
            undo_log.write(tid4, 30, "C".to_string());
            undo_log.write(tid4, 50, "D".to_string());

            undo_log.commit(tid4).unwrap();
            undo_log.checkpoint().unwrap();

            let tid6 = undo_log.start();
            undo_log.write(tid6, 60, "E".to_string());
            undo_log.commit(tid6).unwrap();

            store.set_flush_err(true);
            assert!(undo_log.commit(tid3).is_err());
            store.set_flush_err(false);
        }

        // Create a new undo log which should automatically recover data.
        let mut undo_log = UndoLog::new(path, store.clone()).unwrap();
        assert_eq!(undo_log.start(), 7);

        assert_eq!(store.get(&20), Some("World".to_string()));
        assert_eq!(store.get(&30), Some("Foo".to_string()));
        assert_eq!(store.get(&50), Some("D".to_string()));
        assert_eq!(store.get(&60), Some("E".to_string()));
    }).unwrap();
}

#[test]
fn test_checkpoint_recover_after_end() {
    create_test_file("./files/checkpoint_recover_after_end", |path, _| {
        let mut store: MyStore<MyLogData> = MyStore::new();
        {
            let mut undo_log = UndoLog::new(path, store.clone()).unwrap();
            let tid1 = undo_log.start();
            let tid2 = undo_log.start();

            undo_log.write(tid1, 20, "Hello".to_string());
            undo_log.write(tid2, 20, "World".to_string());
            undo_log.write(tid2, 30, "Blah".to_string());
            undo_log.write(tid1, 30, "Foo".to_string());

            undo_log.commit(tid1).unwrap();
            undo_log.commit(tid2).unwrap();

            let tid3 = undo_log.start();
            let tid4 = undo_log.start();
            let tid5 = undo_log.start();

            undo_log.write(tid3, 20, "A".to_string());
            undo_log.write(tid5, 30, "B".to_string());
            undo_log.write(tid4, 30, "C".to_string());
            undo_log.write(tid4, 50, "D".to_string());

            undo_log.checkpoint().unwrap();
            undo_log.commit(tid4).unwrap();
            undo_log.commit(tid3).unwrap();
            undo_log.commit(tid5).unwrap();

            let tid6 = undo_log.start();
            undo_log.write(tid6, 60, "E".to_string());
            undo_log.write(tid6, 30, "F".to_string());

            store.set_flush_err(true);
            assert!(undo_log.commit(tid6).is_err());
            store.set_flush_err(false);
        }

        // Create a new undo log which should automatically recover data.
        let mut undo_log = UndoLog::new(path, store.clone()).unwrap();
        assert_eq!(undo_log.start(), 7);

        assert_eq!(store.get(&20), Some("A".to_string()));
        assert_eq!(store.get(&30), Some("C".to_string()));
        assert_eq!(store.get(&50), Some("D".to_string()));
        assert_eq!(store.get(&60), None);
    }).unwrap();
}
