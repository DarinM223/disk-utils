use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;
use std::result;
use std::sync::{Arc, Mutex};

use wal::Serializable;
use wal::record::Record;
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

pub struct ChangeEntry<A: Serializable, B: Serializable> {
    tid: u64,
    key: A,
    old: B,
}

impl<A, B> Serializable for ChangeEntry<A, B>
    where A: Serializable,
          B: Serializable
{
    fn serialize(&self) -> io::Result<Vec<Record>> {
        unimplemented!()
    }

    fn deserialize(&mut self, records: Vec<Record>) -> io::Result<()> {
        unimplemented!()
    }
}

pub struct UndoLog {
    file: File,
    mutex: Arc<Mutex<u8>>,
}

impl UndoLog {
    pub fn new<P: AsRef<Path>>(path: &P) -> Result<UndoLog> {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)?;

        Ok(UndoLog {
            file: file,
            mutex: Arc::new(Mutex::new(0)),
        })
    }

    pub fn write_change<S: Serializable>(&mut self, key: S, val: S) -> Result<()> {
        let lock = self.mutex.lock().map_err(|_| UndoLogError::LockError)?;
        let mut writer = Writer::new(&mut self.file);
        let entry = ChangeEntry {
            tid: 0,
            key: key,
            old: val,
        };
        let records = entry.serialize()?;
        for record in records.iter() {
            writer.append(record)?;
        }
        Ok(())
    }
}
