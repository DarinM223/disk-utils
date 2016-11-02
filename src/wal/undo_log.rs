use std::collections::{VecDeque, HashSet};
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Read, Write};
use std::path::Path;

use wal::{LogData, read_serializable_backwards, Serializable, split_bytes_into_records};
use wal::entries::{ChangeEntry, InsertEntry, Transaction};
use wal::iterator::WalIterator;
use wal::writer::Writer;

#[derive(Clone, Debug, PartialEq)]
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
    mem_log: VecDeque<UndoLogEntry<Data>>,
    last_tid: u64,
    active_tids: HashSet<u64>,
    file: File,
    store: Store,
}

impl<Data, Store> UndoLog<Data, Store>
    where Data: LogData,
          Store: UndoLogStore<Data>
{
    pub fn new<P: AsRef<Path> + ?Sized>(path: &P,
                                        store: Store)
                                        -> io::Result<UndoLog<Data, Store>> {
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
            file: file,
            mem_log: VecDeque::new(),
            last_tid: tid,
            active_tids: HashSet::new(),
            store: store,
        };

        if recover {
            log.recover()?;
        }
        Ok(log)
    }

    pub fn entries(&self) -> Vec<UndoLogEntry<Data>> {
        self.mem_log.clone().into_iter().collect()
    }

    pub fn recover(&mut self) -> io::Result<()> {
        let mut finished_transactions = HashSet::new();
        let mut unfinished_transactions = HashSet::new();

        {
            let mut iter = WalIterator::new(&mut self.file)?;
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

        // Flush undo store changes first before writing aborts to the log.
        self.store.flush()?;

        let mut max_tid = None;
        for unfinished_tid in unfinished_transactions {
            self.mem_log.push_back(UndoLogEntry::Transaction(Transaction::Abort(unfinished_tid)));
            self.active_tids.remove(&unfinished_tid);

            match max_tid {
                Some(tid) if unfinished_tid > tid => max_tid = Some(unfinished_tid),
                None => max_tid = Some(unfinished_tid),
                _ => {}
            }
        }

        // Set the last tid to the largest aborted tid.
        if let Some(max_tid) = max_tid {
            self.last_tid = max_tid;
        }

        self.flush()?;
        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        let mut writer = Writer::new(&mut self.file);
        for entry in self.mem_log.iter_mut() {
            let mut bytes = Vec::new();
            entry.serialize(&mut bytes)?;

            let records = split_bytes_into_records(bytes, MAX_RECORD_SIZE)?;
            for record in records.iter() {
                writer.append(record)?;
            }
        }
        self.mem_log.clear();
        Ok(())
    }

    pub fn start(&mut self) -> u64 {
        self.last_tid += 1;
        let entry = UndoLogEntry::Transaction(Transaction::Start(self.last_tid));
        self.mem_log.push_back(entry);
        self.active_tids.insert(self.last_tid);

        self.last_tid
    }

    pub fn write(&mut self, tid: u64, key: Data::Key, val: Data::Value) {
        if self.active_tids.contains(&tid) {
            let entry = if let Some(old_value) = self.store.get(&key) {
                UndoLogEntry::ChangeEntry(ChangeEntry {
                    tid: tid,
                    key: key.clone(),
                    old: old_value,
                })
            } else {
                UndoLogEntry::InsertEntry(InsertEntry {
                    tid: tid,
                    key: key.clone(),
                })
            };
            self.store.update(key, val);
            self.mem_log.push_back(entry);
        }
    }

    pub fn commit(&mut self, tid: u64) -> io::Result<()> {
        if self.active_tids.contains(&tid) {
            self.flush()?;
            self.store.flush()?;

            let entry = UndoLogEntry::Transaction(Transaction::Commit(tid));
            self.mem_log.push_back(entry);
            self.active_tids.remove(&tid);
            self.flush()?;
        }

        Ok(())
    }
}
