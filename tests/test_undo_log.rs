extern crate disk_utils;

use std::collections::HashMap;
use std::fs;
use std::fs::OpenOptions;
use std::io;
use std::panic;
use std::sync::{Arc, RwLock};

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
fn test_new_log_has_zero_tid() {
    let path = "./files/new_undo_log";
    let store: MyStore<MyLogData> = MyStore::new();
    let undo_log = UndoLog::new(path, store).unwrap();
    let result = panic::catch_unwind(move || {
        assert_eq!(*undo_log.tid.read().unwrap(), 0);
    });
    fs::remove_file(path).unwrap();
    if let Err(e) = result {
        panic!(e);
    }
}

#[test]
fn test_start() {
    let path = "./files/start_undo_log";
    let store: MyStore<MyLogData> = MyStore::new();
    let mut undo_log = UndoLog::new(path, store).unwrap();
    let result = panic::catch_unwind(move || {
        undo_log.start().unwrap();

        assert_eq!(undo_log.mem_log.lock().unwrap().len(), 1);
        assert_eq!(undo_log.mem_log.lock().unwrap()[0],
                   UndoLogEntry::Transaction(Transaction::Start(1)));
    });
    fs::remove_file(path).unwrap();
    if let Err(e) = result {
        panic!(e);
    }
}

#[test]
fn test_write() {
    let path = "./files/write_undo_log";
    let store: MyStore<MyLogData> = MyStore::new();
    let mut undo_log = UndoLog::new(path, store).unwrap();
    let result = panic::catch_unwind(move || {
        undo_log.start().unwrap();
        undo_log.write(20, "Hello".to_string()).unwrap();

        assert_eq!(undo_log.mem_log.lock().unwrap().len(), 2);
        assert_eq!(undo_log.mem_log.lock().unwrap()[1],
                   UndoLogEntry::InsertEntry(InsertEntry { tid: 1, key: 20 }));

        undo_log.write(20, "World".to_string()).unwrap();

        assert_eq!(undo_log.mem_log.lock().unwrap().len(), 3);
        assert_eq!(undo_log.mem_log.lock().unwrap()[2],
                   UndoLogEntry::ChangeEntry(ChangeEntry {
                       tid: 1,
                       key: 20,
                       old: "Hello".to_string(),
                   }));
    });
    fs::remove_file(path).unwrap();
    if let Err(e) = result {
        panic!(e);
    }
}

#[test]
fn test_commit() {
    let path = "./files/commit_undo_log";
    let store: MyStore<MyLogData> = MyStore::new();
    let mut undo_log = UndoLog::new(path, store).unwrap();
    let result = panic::catch_unwind(move || {
        undo_log.start().unwrap();
        undo_log.write(20, "Hello".to_string()).unwrap();
        undo_log.write(20, "World".to_string()).unwrap();
        undo_log.commit().unwrap();

        assert_eq!(*undo_log.tid.read().unwrap(), 1);

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

        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)
            .unwrap();
        let mut iter = WalIterator::new(&mut file).unwrap();
        while let Ok(data) = read_serializable::<UndoLogEntry<MyLogData>>(&mut iter) {
            assert_eq!(data, expected_entries.next().unwrap());
        }
    });
    fs::remove_file(path).unwrap();
    if let Err(e) = result {
        panic!(e);
    }
}

#[test]
fn test_recover() {
    let path = "./files/recover_undo_log";
    let mut store: MyStore<MyLogData> = MyStore::new();
    let result = panic::catch_unwind(move || {
        {
            let mut undo_log = UndoLog::new(path, store.clone()).unwrap();
            undo_log.start().unwrap();
            undo_log.write(20, "Hello".to_string()).unwrap();
            undo_log.commit().unwrap();

            store.set_flush_err(true);

            undo_log.start().unwrap();
            undo_log.write(20, "World".to_string()).unwrap();
            undo_log.write(30, "Hello".to_string()).unwrap();
            assert!(undo_log.commit().is_err());
        }

        // Create a new undo log which should automatically recover data.
        let undo_log = UndoLog::new(path, store.clone()).unwrap();
        assert_eq!(*undo_log.tid.read().unwrap(), 2);

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
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)
            .unwrap();
        let mut iter = WalIterator::new(&mut file).unwrap();
        while let Ok(data) = read_serializable::<UndoLogEntry<MyLogData>>(&mut iter) {
            assert_eq!(data, expected_entries.next().unwrap());
        }

        assert_eq!(store.get(&20), Some("Hello".to_string()));
        assert_eq!(store.get(&30), None);
    });
    fs::remove_file(path).unwrap();
    if let Err(e) = result {
        panic!(e);
    }
}
