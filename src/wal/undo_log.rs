use std::collections::{VecDeque, HashSet};
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Read, Write};
use std::path::Path;
use std::result;
use std::sync::{Arc, Mutex, RwLock};

use wal::{LogData, read_serializable_backwards, Serializable, split_bytes_into_records};
use wal::entries::{ChangeEntry, InsertEntry, Transaction};
use wal::iterator::WalIterator;
use wal::writer::Writer;

#[derive(Debug)]
pub enum UndoLogError {
    IoError(io::Error),
    LockError,
}

impl From<io::Error> for UndoLogError {
    fn from(err: io::Error) -> UndoLogError {
        UndoLogError::IoError(err)
    }
}

pub type Result<T> = result::Result<T, UndoLogError>;

#[derive(Debug, PartialEq)]
pub enum UndoLogEntry<Data: LogData> {
    InsertEntry(InsertEntry<Data>),
    ChangeEntry(ChangeEntry<Data>),
    Transaction(Transaction),
}

impl<Data> Serializable for UndoLogEntry<Data>
    where Data: LogData
{
    fn serialize<W: Write>(&self, bytes: &mut W) -> io::Result<()> {
        match *self {
            UndoLogEntry::InsertEntry(ref entry) => {
                bytes.write(&[0])?;
                entry.serialize(bytes)
            }
            UndoLogEntry::ChangeEntry(ref entry) => {
                bytes.write(&[1])?;
                entry.serialize(bytes)
            }
            UndoLogEntry::Transaction(ref entry) => {
                bytes.write(&[2])?;
                entry.serialize(bytes)
            }
        }
    }

    fn deserialize<R: Read>(bytes: &mut R) -> io::Result<UndoLogEntry<Data>> {
        let mut entry_type = [0; 1];
        bytes.read(&mut entry_type)?;

        match entry_type[0] {
            0 => Ok(UndoLogEntry::InsertEntry(InsertEntry::deserialize(bytes)?)),
            1 => Ok(UndoLogEntry::ChangeEntry(ChangeEntry::deserialize(bytes)?)),
            2 => Ok(UndoLogEntry::Transaction(Transaction::deserialize(bytes)?)),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid entry type")),
        }
    }
}

const MAX_RECORD_SIZE: usize = 1024;

pub trait UndoLogStore<Data: LogData> {
    fn get(&self, key: &Data::Key) -> Option<Data::Value>;
    fn remove(&mut self, key: &Data::Key);
    fn update(&mut self, key: Data::Key, val: Data::Value);
    fn flush(&mut self) -> io::Result<()>;
}

pub struct UndoLog<Data: LogData, Store: UndoLogStore<Data>> {
    file: Arc<Mutex<File>>,
    mem_log: Arc<Mutex<VecDeque<UndoLogEntry<Data>>>>,
    tid: Arc<RwLock<u64>>,
    store: Store,
}

impl<Data, Store> UndoLog<Data, Store>
    where Data: LogData,
          Store: UndoLogStore<Data>
{
    pub fn new<P: AsRef<Path> + ?Sized>(path: &P, store: Store) -> Result<UndoLog<Data, Store>> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)?;
        let mut tid = 0;
        let mut recover = false;

        // Do a backwards pass over the file.
        // If last record is not a COMMIT or ABORT, then start recovery.
        {
            let mut iter = WalIterator::new(&mut file)?;
            if let Ok(data) = read_serializable_backwards::<UndoLogEntry<Data>>(&mut iter) {
                match data {
                    UndoLogEntry::Transaction(Transaction::Commit(id)) => tid = id,
                    UndoLogEntry::Transaction(Transaction::Abort(id)) => tid = id,
                    _ => recover = true,
                }
            }
        }

        let mut log = UndoLog {
            file: Arc::new(Mutex::new(file)),
            mem_log: Arc::new(Mutex::new(VecDeque::new())),
            tid: Arc::new(RwLock::new(tid)),
            store: store,
        };

        if recover {
            log.recover()?;
        }
        Ok(log)
    }

    pub fn recover(&mut self) -> Result<()> {
        let mut finished_transactions = HashSet::new();
        let mut unfinished_transactions = HashSet::new();

        {
            let mut file = self.file.lock().map_err(|_| UndoLogError::LockError)?;
            let mut iter = WalIterator::new(&mut file)?;

            while let Ok(data) = read_serializable_backwards::<UndoLogEntry<Data>>(&mut iter) {
                match data {
                    UndoLogEntry::Transaction(Transaction::Commit(id)) => {
                        finished_transactions.insert(id);
                    }
                    UndoLogEntry::Transaction(Transaction::Abort(id)) => {
                        finished_transactions.insert(id);
                    }
                    UndoLogEntry::InsertEntry(entry) => {
                        if !finished_transactions.contains(&entry.tid) {
                            self.store.remove(&entry.key);
                            unfinished_transactions.insert(entry.tid);
                        }
                    }
                    UndoLogEntry::ChangeEntry(entry) => {
                        if !finished_transactions.contains(&entry.tid) {
                            self.store.update(entry.key, entry.old);
                            unfinished_transactions.insert(entry.tid);
                        }
                    }
                    _ => {}
                }
            }
        }

        {
            let mut log = self.mem_log.lock().map_err(|_| UndoLogError::LockError)?;
            let mut max_tid = None;
            for unfinished_tid in unfinished_transactions {
                log.push_back(UndoLogEntry::Transaction(Transaction::Abort(unfinished_tid)));

                match max_tid {
                    Some(tid) if unfinished_tid > tid => max_tid = Some(unfinished_tid),
                    None => max_tid = Some(unfinished_tid),
                    _ => {}
                }
            }

            // Set the tid to the largest aborted tid.
            if let Some(max_tid) = max_tid {
                let mut tid = self.tid.write().map_err(|_| UndoLogError::LockError)?;
                *tid = max_tid;
            }
        }

        self.flush()?;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        let mut file = self.file.lock().map_err(|_| UndoLogError::LockError)?;
        let mut writer = Writer::new(&mut file);
        let mut log = self.mem_log.lock().map_err(|_| UndoLogError::LockError)?;
        for entry in log.iter_mut() {
            let mut bytes = Vec::new();
            entry.serialize(&mut bytes)?;

            let records = split_bytes_into_records(bytes, MAX_RECORD_SIZE)?;
            for record in records.iter() {
                writer.append(record)?;
            }
        }
        log.clear();
        Ok(())
    }

    pub fn start(&mut self) -> Result<()> {
        let tid = self.tid.read().map_err(|_| UndoLogError::LockError)?;
        let entry = UndoLogEntry::Transaction(Transaction::Start(*tid + 1));
        let mut log = self.mem_log.lock().map_err(|_| UndoLogError::LockError)?;
        log.push_back(entry);
        Ok(())
    }

    pub fn write(&mut self, key: Data::Key, val: Data::Value) -> Result<()> {
        let tid = self.tid.read().map_err(|_| UndoLogError::LockError)?;
        let entry = if let Some(old_value) = self.store.get(&key) {
            UndoLogEntry::ChangeEntry(ChangeEntry {
                tid: *tid + 1,
                key: key.clone(),
                old: old_value,
            })
        } else {
            UndoLogEntry::InsertEntry(InsertEntry {
                tid: *tid + 1,
                key: key.clone(),
                value: val.clone(),
            })
        };
        self.store.update(key, val);

        let mut log = self.mem_log.lock().map_err(|_| UndoLogError::LockError)?;
        log.push_back(entry);

        Ok(())
    }

    pub fn commit(&mut self) -> Result<()> {
        self.flush()?;
        self.store.flush()?;

        {
            let tid = self.tid.read().map_err(|_| UndoLogError::LockError)?;
            let entry = UndoLogEntry::Transaction(Transaction::Commit(*tid + 1));
            let mut log = self.mem_log.lock().map_err(|_| UndoLogError::LockError)?;
            log.push_back(entry);
        }

        self.flush()?;
        let mut tid = self.tid.write().map_err(|_| UndoLogError::LockError)?;
        *tid += 1;
        Ok(())
    }

    pub fn abort(&mut self) -> Result<()> {
        {
            let tid = self.tid.read().map_err(|_| UndoLogError::LockError)?;
            let entry = UndoLogEntry::Transaction(Transaction::Abort(*tid + 1));
            let mut log = self.mem_log.lock().map_err(|_| UndoLogError::LockError)?;
            log.push_back(entry);
        }

        self.flush()?;
        let mut tid = self.tid.write().map_err(|_| UndoLogError::LockError)?;
        *tid += 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::fs::OpenOptions;
    use std::io;
    use std::panic;
    use std::sync::{Arc, RwLock};
    use super::*;
    use wal::entries::{ChangeEntry, InsertEntry, Transaction};
    use wal::iterator::WalIterator;
    use wal::{LogData, read_serializable};

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
                       UndoLogEntry::InsertEntry(InsertEntry {
                           tid: 1,
                           key: 20,
                           value: "Hello".to_string(),
                       }));

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

            let mut expected_entries = vec![UndoLogEntry::Transaction(Transaction::Start(1)),
                                            UndoLogEntry::InsertEntry(InsertEntry {
                                                tid: 1,
                                                key: 20,
                                                value: "Hello".to_string(),
                                            }),
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

            let mut expected_entries = vec![UndoLogEntry::Transaction(Transaction::Start(1)),
                                            UndoLogEntry::InsertEntry(InsertEntry {
                                                tid: 1,
                                                key: 20,
                                                value: "Hello".to_string(),
                                            }),
                                            UndoLogEntry::Transaction(Transaction::Commit(1)),
                                            UndoLogEntry::Transaction(Transaction::Start(2)),
                                            UndoLogEntry::ChangeEntry(ChangeEntry {
                                                tid: 2,
                                                key: 20,
                                                old: "Hello".to_string(),
                                            }),
                                            UndoLogEntry::InsertEntry(InsertEntry {
                                                tid: 2,
                                                key: 30,
                                                value: "Hello".to_string(),
                                            }),
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
}
