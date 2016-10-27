use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Read, Write};
use std::path::Path;
use std::result;
use std::sync::{Arc, Mutex};

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
        // TODO(DarinM223): write number to distinguish between ChangeEntry and Transaction.
        match *self {
            UndoLogEntry::ChangeEntry(ref entry) => entry.serialize(bytes),
            UndoLogEntry::Transaction(ref entry) => entry.serialize(bytes),
        }
    }

    fn deserialize<R: Read>(bytes: &mut R) -> io::Result<UndoLogEntry<A, B>> {
        // TODO(DarinM223): read number to distinguish between ChangeEntry and Transaction.
        unimplemented!()
    }
}

const MAX_RECORD_SIZE: usize = 1024;

pub struct UndoLog<A, B> {
    file: Arc<Mutex<File>>,
    mem_log: Arc<Mutex<VecDeque<UndoLogEntry<A, B>>>>,
}

impl<A, B> UndoLog<A, B>
    where A: Serializable + Clone,
          B: Serializable + Clone
{
    pub fn new<P: AsRef<Path>>(path: &P) -> Result<UndoLog<A, B>> {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)?;

        Ok(UndoLog {
            file: Arc::new(Mutex::new(file)),
            mem_log: Arc::new(Mutex::new(VecDeque::new())),
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
        // TODO(DarinM223): handle tid.
        let entry = UndoLogEntry::Transaction(Transaction::Start(0));
        let mut log = self.mem_log.lock().map_err(|_| UndoLogError::LockError)?;
        log.push_back(entry);
        Ok(())
    }

    pub fn write(&mut self, key: A, val: B) -> Result<()> {
        let entry = UndoLogEntry::ChangeEntry(ChangeEntry {
            tid: 0, // TODO(DarinM223): handle tid.
            key: key,
            old: val,
        });

        let mut log = self.mem_log.lock().map_err(|_| UndoLogError::LockError)?;
        log.push_back(entry);
        Ok(())
    }

    pub fn commit(&mut self) -> Result<()> {
        {
            // TODO(DarinM223): handle tid.
            let entry = UndoLogEntry::Transaction(Transaction::Commit(0));
            let mut log = self.mem_log.lock().map_err(|_| UndoLogError::LockError)?;
            log.push_back(entry);
        }

        self.flush()?;
        Ok(())
    }

    pub fn abort(&mut self) -> Result<()> {
        {
            // TODO(DarinM223): handle tid.
            let entry = UndoLogEntry::Transaction(Transaction::Abort(0));
            let mut log = self.mem_log.lock().map_err(|_| UndoLogError::LockError)?;
            log.push_back(entry);
        }

        self.flush()?;
        Ok(())
    }
}
