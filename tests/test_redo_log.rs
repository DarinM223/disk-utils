extern crate disk_utils;

use std::collections::HashMap;
use std::io;
use std::sync::{Arc, RwLock};

use disk_utils::testing::create_test_file;
use disk_utils::wal::{LogData, LogStore, read_serializable};
use disk_utils::wal::entries::{ChangeEntry, Checkpoint, SingleLogEntry, Transaction};
use disk_utils::wal::iterator::{ReadDirection, WalIterator};
use disk_utils::wal::redo_log::RedoLog;

#[derive(Clone, PartialEq, Debug)]
struct MyLogData;

impl LogData for MyLogData {
    type Key = i32;
    type Value = String;
}

#[derive(Clone)]
struct MyStore<Data: LogData> {
    data: Arc<RwLock<HashMap<Data::Key, Data::Value>>>,
    flushed_data: Arc<RwLock<HashMap<Data::Key, Data::Value>>>,
    flush_err: Arc<RwLock<bool>>,
}

impl<Data> MyStore<Data>
    where Data: LogData
{
    pub fn new() -> MyStore<Data> {
        MyStore {
            data: Arc::new(RwLock::new(HashMap::new())),
            flushed_data: Arc::new(RwLock::new(HashMap::new())),
            flush_err: Arc::new(RwLock::new(false)),
        }
    }

    pub fn set_flush_err(&mut self, flush_err: bool) {
        *self.flush_err.write().unwrap() = flush_err;
    }

    pub fn get_flushed(&self, key: &Data::Key) -> Option<Data::Value> {
        self.flushed_data.read().unwrap().get(key).cloned()
    }

    pub fn discard_changes(&mut self) {
        *self.data.write().unwrap() = self.flushed_data.read().unwrap().clone();
    }
}

impl<Data> LogStore<Data> for MyStore<Data>
    where Data: LogData
{
    fn get(&self, key: &Data::Key) -> Option<Data::Value> {
        self.data.read().unwrap().get(key).cloned()
    }

    fn remove(&mut self, key: &Data::Key) {
        self.data.write().unwrap().remove(key);
    }

    fn update(&mut self, key: Data::Key, val: Data::Value) {
        self.data.write().unwrap().insert(key, val);
    }

    fn flush(&mut self) -> io::Result<()> {
        *self.flushed_data.write().unwrap() = self.data.read().unwrap().clone();
        if *self.flush_err.read().unwrap() {
            Err(io::Error::new(io::ErrorKind::Interrupted, "Flush error occurred"))
        } else {
            Ok(())
        }
    }

    fn flush_change(&mut self, key: Data::Key, val: Data::Value) -> io::Result<()> {
        self.flushed_data.write().unwrap().insert(key, val);
        if *self.flush_err.read().unwrap() {
            Err(io::Error::new(io::ErrorKind::Interrupted, "Flush error occurred"))
        } else {
            Ok(())
        }
    }
}

#[test]
fn test_start() {
    create_test_file("./files/start_redo_log", |path, _| {
        let store: MyStore<MyLogData> = MyStore::new();
        let mut redo_log = RedoLog::new(path, store).unwrap();
        let tid = redo_log.start();

        assert_eq!(tid, 1);
        assert_eq!(redo_log.entries().len(), 1);
        assert_eq!(redo_log.entries()[0], SingleLogEntry::Transaction(Transaction::Start(1)));
    }).unwrap();
}

#[test]
fn test_write() {
    create_test_file("./files/write_redo_log", |path, _| {
        let store: MyStore<MyLogData> = MyStore::new();
        let mut redo_log = RedoLog::new(path, store).unwrap();

        let tid = redo_log.start();
        assert_eq!(tid, 1);

        redo_log.write(tid, 20, "Hello".to_string());

        assert_eq!(redo_log.entries().len(), 2);
        assert_eq!(redo_log.entries()[1], SingleLogEntry::ChangeEntry(ChangeEntry {
            tid: 1,
            key: 20,
            value: "Hello".to_string(),
        }));

        redo_log.write(tid, 20, "World".to_string());

        assert_eq!(redo_log.entries().len(), 3);
        assert_eq!(redo_log.entries()[2],
                   SingleLogEntry::ChangeEntry(ChangeEntry {
                       tid: 1,
                       key: 20,
                       value: "World".to_string(),
                   }));
    }).unwrap();
}

#[test]
fn test_commit() {
    create_test_file("./files/commit_redo_log", |path, mut file| {
        let store: MyStore<MyLogData> = MyStore::new();
        let mut redo_log = RedoLog::new(path, store).unwrap();
        let tid = redo_log.start();
        assert_eq!(tid, 1);
        redo_log.write(tid, 20, "Hello".to_string());
        redo_log.write(tid, 20, "World".to_string());
        redo_log.commit(tid).unwrap();

        let mut expected_entries =
            vec![SingleLogEntry::Transaction(Transaction::Start(1)),
                 SingleLogEntry::ChangeEntry(ChangeEntry {
                     tid: 1,
                     key: 20,
                     value: "Hello".to_string(),
                 }),
                 SingleLogEntry::ChangeEntry(ChangeEntry {
                     tid: 1,
                     key: 20,
                     value: "World".to_string(),
                 }),
                 SingleLogEntry::Transaction(Transaction::Commit(1))]
                .into_iter();
        let mut iter = WalIterator::new(&mut file, ReadDirection::Forward).unwrap();
        while let Ok(data) = read_serializable::<SingleLogEntry<MyLogData>>(&mut iter) {
            assert_eq!(data, expected_entries.next().unwrap());
        }
    }).unwrap();
}

#[test]
fn test_recover() {
    create_test_file("./files/recover_redo_log", |path, mut file| {
        let mut store: MyStore<MyLogData> = MyStore::new();
        {
            let mut redo_log = RedoLog::new(path, store.clone()).unwrap();
            let tid = redo_log.start();
            redo_log.write(tid, 20, "Hello".to_string());
            redo_log.commit(tid).unwrap();

            let tid = redo_log.start();
            redo_log.write(tid, 20, "World".to_string());
            redo_log.write(tid, 30, "Hello".to_string());

            let tid = redo_log.start();
            redo_log.commit(tid).unwrap();
        }

        store.discard_changes();
        // Create a new redo log which should automatically recover data.
        let mut redo_log = RedoLog::new(path, store.clone()).unwrap();
        assert_eq!(redo_log.start(), 4);

        let mut expected_entries =
            vec![SingleLogEntry::Transaction(Transaction::Start(1)),
                 SingleLogEntry::ChangeEntry(ChangeEntry {
                     tid: 1,
                     key: 20,
                     value: "Hello".to_string(),
                 }),
                 SingleLogEntry::Transaction(Transaction::Commit(1)),
                 SingleLogEntry::Transaction(Transaction::Start(2)),
                 SingleLogEntry::ChangeEntry(ChangeEntry {
                     tid: 2,
                     key: 20,
                     value: "World".to_string(),
                 }),
                 SingleLogEntry::ChangeEntry(ChangeEntry {
                     tid: 2,
                     key: 30,
                     value: "Hello".to_string(),
                 }),
                 SingleLogEntry::Transaction(Transaction::Start(3)),
                 SingleLogEntry::Transaction(Transaction::Commit(3)),
                 SingleLogEntry::Transaction(Transaction::Abort(2))]
                .into_iter();
        let mut iter = WalIterator::new(&mut file, ReadDirection::Forward).unwrap();
        while let Ok(data) = read_serializable::<SingleLogEntry<MyLogData>>(&mut iter) {
            assert_eq!(data, expected_entries.next().unwrap());
        }

        assert_eq!(store.get_flushed(&20), Some("Hello".to_string()));
        assert_eq!(store.get_flushed(&30), None);
    }).unwrap();
}

#[test]
fn test_multiple_recover() {
    create_test_file("./files/multiple_recover_redo_log", |path, mut file| {
        let mut store: MyStore<MyLogData> = MyStore::new();
        {
            let mut redo_log = RedoLog::new(path, store.clone()).unwrap();
            let tid1 = redo_log.start();
            let tid2 = redo_log.start();
            redo_log.write(tid1, 20, "Hello".to_string());
            redo_log.write(tid2, 30, "World".to_string());
            redo_log.write(tid1, 30, "Blah".to_string());
            redo_log.commit(tid1).unwrap();
            redo_log.write(tid2, 20, "World".to_string());
            redo_log.commit(tid2).unwrap();

            let tid3 = redo_log.start();
            let tid4 = redo_log.start();

            redo_log.write(tid3, 40, "Foo".to_string());
            redo_log.write(tid4, 30, "Bar".to_string());
            redo_log.commit(tid3).unwrap();

            redo_log.write(tid4, 50, "Hello".to_string());
        }

        store.discard_changes();

        // Create a new redo log which should automatically recover data.
        let mut redo_log = RedoLog::new(path, store.clone()).unwrap();
        assert_eq!(redo_log.start(), 5);

        let mut expected_entries =
            vec![SingleLogEntry::Transaction(Transaction::Start(1)),
                 SingleLogEntry::Transaction(Transaction::Start(2)),
                 SingleLogEntry::ChangeEntry(ChangeEntry {
                     tid: 1,
                     key: 20,
                     value: "Hello".to_string(),
                 }),
                 SingleLogEntry::ChangeEntry(ChangeEntry {
                     tid: 2,
                     key: 30,
                     value: "World".to_string(),
                 }),
                 SingleLogEntry::ChangeEntry(ChangeEntry {
                     tid: 1,
                     key: 30,
                     value: "Blah".to_string(),
                 }),
                 SingleLogEntry::Transaction(Transaction::Commit(1)),
                 SingleLogEntry::ChangeEntry(ChangeEntry {
                     tid: 2,
                     key: 20,
                     value: "World".to_string(),
                 }),
                 SingleLogEntry::Transaction(Transaction::Commit(2)),
                 SingleLogEntry::Transaction(Transaction::Start(3)),
                 SingleLogEntry::Transaction(Transaction::Start(4)),
                 SingleLogEntry::ChangeEntry(ChangeEntry {
                     tid: 3,
                     key: 40,
                     value: "Foo".to_string(),
                 }),
                 SingleLogEntry::ChangeEntry(ChangeEntry {
                     tid: 4,
                     key: 30,
                     value: "Bar".to_string(),
                 }),
                 SingleLogEntry::Transaction(Transaction::Commit(3)),
                 SingleLogEntry::Transaction(Transaction::Abort(4))]
                .into_iter();
        let mut iter = WalIterator::new(&mut file, ReadDirection::Forward).unwrap();
        while let Ok(data) = read_serializable::<SingleLogEntry<MyLogData>>(&mut iter) {
            assert_eq!(data, expected_entries.next().unwrap());
        }

        // Test expected state after recovery:
        assert_eq!(store.get_flushed(&20), Some("World".to_string()));
        assert_eq!(store.get_flushed(&30), Some("Blah".to_string()));
        assert_eq!(store.get_flushed(&40), Some("Foo".to_string()));
        assert_eq!(store.get_flushed(&50), None);
    }).unwrap();
}

#[test]
fn test_add_end_checkpoint() {
    create_test_file("./files/add_end_checkpoint", |path, mut file| {
        let store: MyStore<MyLogData> = MyStore::new();
        {
            let mut redo_log = RedoLog::new(path, store.clone()).unwrap();
            let tid1 = redo_log.start();
            let tid2 = redo_log.start();
            redo_log.commit(tid1).unwrap();
            let tid3 = redo_log.start();
            let tid4 = redo_log.start();
            redo_log.checkpoint().unwrap();
            redo_log.commit(tid3).unwrap();
            redo_log.commit(tid4).unwrap();
            redo_log.commit(tid2).unwrap();
        }

        let mut redo_log = RedoLog::new(path, store.clone()).unwrap();
        assert_eq!(redo_log.start(), 5);

        let mut expected_entries =
            vec![SingleLogEntry::Transaction(Transaction::Start(1)),
                 SingleLogEntry::Transaction(Transaction::Start(2)),
                 SingleLogEntry::Transaction(Transaction::Commit(1)),
                 SingleLogEntry::Transaction(Transaction::Start(3)),
                 SingleLogEntry::Transaction(Transaction::Start(4)),
                 SingleLogEntry::Checkpoint(Checkpoint::Begin(vec![2, 3, 4])),
                 SingleLogEntry::Checkpoint(Checkpoint::End),
                 SingleLogEntry::Transaction(Transaction::Commit(3)),
                 SingleLogEntry::Transaction(Transaction::Commit(4)),
                 SingleLogEntry::Transaction(Transaction::Commit(2))]
                .into_iter();
        let mut iter = WalIterator::new(&mut file, ReadDirection::Forward).unwrap();
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
fn test_checkpoint_recover_after_end() {
    create_test_file("./files/checkpoint_recover_after_end", |path, _| {
        let mut store: MyStore<MyLogData> = MyStore::new();
        {
            let mut redo_log = RedoLog::new(path, store.clone()).unwrap();
            let tid1 = redo_log.start();
            let tid2 = redo_log.start();

            redo_log.write(tid1, 20, "Hello".to_string());
            redo_log.write(tid2, 20, "World".to_string());
            redo_log.write(tid2, 30, "Blah".to_string());
            redo_log.write(tid1, 30, "Foo".to_string());

            redo_log.commit(tid1).unwrap();
            redo_log.commit(tid2).unwrap();

            let tid3 = redo_log.start();
            let tid4 = redo_log.start();
            let tid5 = redo_log.start();

            redo_log.write(tid3, 20, "A".to_string());
            redo_log.write(tid5, 30, "B".to_string());
            redo_log.write(tid4, 30, "C".to_string());
            redo_log.write(tid4, 50, "D".to_string());

            redo_log.checkpoint().unwrap();
            redo_log.commit(tid4).unwrap();
            redo_log.commit(tid3).unwrap();
            redo_log.commit(tid5).unwrap();
        }

        store.discard_changes();

        // Create a new redo log which should automatically recover data.
        let mut redo_log = RedoLog::new(path, store.clone()).unwrap();
        assert_eq!(redo_log.start(), 6);

        assert_eq!(store.get_flushed(&20), Some("A".to_string()));
        assert_eq!(store.get_flushed(&30), Some("C".to_string()));
        assert_eq!(store.get_flushed(&50), Some("D".to_string()));
        assert_eq!(store.get_flushed(&60), None);
    }).unwrap();
}

#[test]
fn test_checkpoint_flushed_changes() {
    create_test_file("./files/checkpoint_flushed_changes", |path, _| {
        let mut store: MyStore<MyLogData> = MyStore::new();
        {
            let mut redo_log = RedoLog::new(path, store.clone()).unwrap();
            let tid1 = redo_log.start();
            let tid2 = redo_log.start();

            redo_log.write(tid1, 20, "Hello".to_string());
            redo_log.write(tid2, 30, "World".to_string());
            redo_log.write(tid2, 20, "World".to_string());
            redo_log.write(tid1, 30, "Hello".to_string());

            redo_log.commit(tid2).unwrap();
            // Should  flush (20 -> "World") and (30 -> "World") to disk.
            redo_log.checkpoint().unwrap();
            assert_eq!(store.get_flushed(&20), Some("World".to_string()));
            assert_eq!(store.get_flushed(&30), Some("World".to_string()));

            redo_log.write(tid1, 40, "New key".to_string());

            let tid3 = redo_log.start();
            let tid4 = redo_log.start();
            redo_log.write(tid3, 50, "New key".to_string());
            redo_log.write(tid4, 50, "New new key".to_string());
            redo_log.commit(tid3).unwrap();
        }

        store.discard_changes();
        // Create a new redo log which should automatically recover data.
        let _ = RedoLog::new(path, store.clone()).unwrap();
        assert_eq!(store.get_flushed(&20), Some("World".to_string()));
        assert_eq!(store.get_flushed(&30), Some("World".to_string()));
        assert_eq!(store.get_flushed(&40), None);
        assert_eq!(store.get_flushed(&50), Some("New key".to_string()));
    }).unwrap();
}
