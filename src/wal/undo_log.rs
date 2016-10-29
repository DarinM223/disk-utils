use std::collections::{VecDeque, HashSet};
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Read, Write};
use std::path::Path;
use std::result;
use std::sync::{Arc, Mutex, RwLock};

use wal::{LogData, read_serializable_backwards, Serializable, split_bytes_into_records};
use wal::entries::{ChangeEntry, Transaction};
use wal::iterator::WalIterator;
use wal::writer::Writer;

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

pub enum UndoLogEntry<Data: LogData> {
    ChangeEntry(ChangeEntry<Data>),
    Transaction(Transaction),
}

impl<Data> Serializable for UndoLogEntry<Data>
    where Data: LogData
{
    fn serialize<W: Write>(&self, bytes: &mut W) -> io::Result<()> {
        match *self {
            UndoLogEntry::ChangeEntry(ref entry) => {
                bytes.write(&[0])?;
                entry.serialize(bytes)
            }
            UndoLogEntry::Transaction(ref entry) => {
                bytes.write(&[1])?;
                entry.serialize(bytes)
            }
        }
    }

    fn deserialize<R: Read>(bytes: &mut R) -> io::Result<UndoLogEntry<Data>> {
        let mut entry_type = [0; 1];
        bytes.read(&mut entry_type)?;

        match entry_type[0] {
            0 => Ok(UndoLogEntry::ChangeEntry(ChangeEntry::deserialize(bytes)?)),
            1 => Ok(UndoLogEntry::Transaction(Transaction::deserialize(bytes)?)),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid entry type")),
        }
    }
}

const MAX_RECORD_SIZE: usize = 1024;

pub trait UndoLogStore<Data: LogData> {
    fn get(&self, key: &Data::Key) -> Option<Data::Value>;
    fn update(&mut self, key: &Data::Key, val: Data::Value);
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
    pub fn new<P: AsRef<Path>>(path: &P, store: Store) -> Result<UndoLog<Data, Store>> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)?;
        let mut tid = 0;
        let mut recover = false;

        // Do a backwards pass over the file.
        // If last record is not a COMMIT or ABORT, then start recovery.
        // Otherwise, get last tid from that record or 0 if file is empty.
        // TODO(DarinM223): get last tid in recovery mode.
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
                    UndoLogEntry::ChangeEntry(entry) => {
                        if !finished_transactions.contains(&entry.tid) {
                            self.store.update(&entry.key, entry.old);
                            unfinished_transactions.insert(entry.tid);
                        }
                    }
                    _ => {}
                }
            }
        }

        {
            let mut log = self.mem_log.lock().map_err(|_| UndoLogError::LockError)?;
            for tid in unfinished_transactions {
                log.push_back(UndoLogEntry::Transaction(Transaction::Abort(tid)));
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
        if let Some(old_value) = self.store.get(&key) {
            self.store.update(&key, val);

            let tid = self.tid.read().map_err(|_| UndoLogError::LockError)?;
            let entry = ChangeEntry {
                tid: *tid + 1,
                key: key,
                old: old_value,
            };

            let mut log = self.mem_log.lock().map_err(|_| UndoLogError::LockError)?;
            log.push_back(UndoLogEntry::ChangeEntry(entry));
        }

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
