use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Read, Write};
use std::path::Path;
use std::result;
use std::sync::{Arc, Mutex, RwLock};

use wal::{Serializable, split_bytes_into_records};
use wal::entries::{ChangeEntry, Transaction};
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

#[derive(Clone)]
pub enum UndoLogEntry<A, B> {
    ChangeEntry(ChangeEntry<A, B>),
    Transaction(Transaction),
}

impl<A, B> Serializable for UndoLogEntry<A, B>
    where A: Serializable,
          B: Serializable
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

    fn deserialize<R: Read>(bytes: &mut R) -> io::Result<UndoLogEntry<A, B>> {
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

pub struct UndoLog<A, B> {
    file: Arc<Mutex<File>>,
    mem_log: Arc<Mutex<VecDeque<UndoLogEntry<A, B>>>>,
    tid: Arc<RwLock<u64>>,
}

impl<A, B> UndoLog<A, B>
    where A: Serializable,
          B: Serializable
{
    pub fn new<P: AsRef<Path>>(path: &P) -> Result<UndoLog<A, B>> {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)?;

        // TODO(DarinM223): do a backwards pass over the file.
        // If last record is not a COMMIT or ABORT, then start recovery.
        // Otherwise, get last tid from that record or 0 if file is empty.

        Ok(UndoLog {
            file: Arc::new(Mutex::new(file)),
            mem_log: Arc::new(Mutex::new(VecDeque::new())),
            tid: Arc::new(RwLock::new(0)), // TODO(DarinM223): handle tid.
        })
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

    pub fn write(&mut self, key: A, val: B) -> Result<()> {
        let tid = self.tid.read().map_err(|_| UndoLogError::LockError)?;
        let entry = UndoLogEntry::ChangeEntry(ChangeEntry {
            tid: *tid + 1,
            key: key,
            old: val,
        });

        let mut log = self.mem_log.lock().map_err(|_| UndoLogError::LockError)?;
        log.push_back(entry);
        Ok(())
    }

    pub fn commit(&mut self) -> Result<()> {
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
