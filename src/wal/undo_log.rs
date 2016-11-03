use std::cmp;
use std::collections::{VecDeque, HashSet};
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Read, Write};
use std::path::Path;

use wal::{LogData, LogStore, read_serializable_backwards, Serializable, split_bytes_into_records};
use wal::entries::{ChangeEntry, Checkpoint, InsertEntry, Transaction};
use wal::iterator::WalIterator;
use wal::writer::Writer;

#[derive(Clone, Debug, PartialEq)]
pub enum UndoLogEntry<Data: LogData> {
    InsertEntry(InsertEntry<Data>),
    ChangeEntry(ChangeEntry<Data>),
    Transaction(Transaction),
    Checkpoint(Checkpoint),
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
            UndoLogEntry::Checkpoint(ref entry) => {
                bytes.write(&[3])?;
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
            3 => Ok(UndoLogEntry::Checkpoint(Checkpoint::deserialize(bytes)?)),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid entry type")),
        }
    }
}

const MAX_RECORD_SIZE: usize = 1024;

#[derive(PartialEq)]
enum RecoverState {
    /// No checkpoint entry found, read until end of log.
    None,
    /// Begin checkpoint entry found, read until the start entry
    /// of every transaction in the checkpoint is read.
    Begin(HashSet<u64>),
    /// End checkpoint entry found, read until a begin
    /// checkpoint entry is found.
    End,
}

pub struct UndoLog<Data: LogData, Store: LogStore<Data>> {
    mem_log: VecDeque<UndoLogEntry<Data>>,
    last_tid: u64,
    checkpoint_tids: Option<Vec<u64>>,
    active_tids: HashSet<u64>,
    file: File,
    store: Store,
}

impl<Data, Store> UndoLog<Data, Store>
    where Data: LogData,
          Store: LogStore<Data>
{
    pub fn new<P: AsRef<Path> + ?Sized>(path: &P,
                                        store: Store)
                                        -> io::Result<UndoLog<Data, Store>> {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)?;
        let mut log = UndoLog {
            file: file,
            mem_log: VecDeque::new(),
            last_tid: 0,
            checkpoint_tids: None,
            active_tids: HashSet::new(),
            store: store,
        };
        log.recover()?;
        Ok(log)
    }

    pub fn entries(&self) -> Vec<UndoLogEntry<Data>> {
        self.mem_log.clone().into_iter().collect()
    }

    fn recover(&mut self) -> io::Result<()> {
        let mut finished_transactions = HashSet::new();
        let mut unfinished_transactions = HashSet::new();
        let mut state = RecoverState::None;

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
                    UndoLogEntry::Transaction(Transaction::Start(id)) => {
                        if let RecoverState::Begin(ref mut transactions) = state {
                            transactions.remove(&id);
                            if transactions.is_empty() {
                                break;
                            }
                        }
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
                    UndoLogEntry::Checkpoint(Checkpoint::Begin(transactions)) => {
                        match state {
                            RecoverState::None => {
                                if transactions.is_empty() {
                                    break;
                                }
                                state = RecoverState::Begin(transactions.into_iter().collect());
                            }
                            RecoverState::End => break,
                            _ => {}
                        }
                    }
                    UndoLogEntry::Checkpoint(Checkpoint::End) => {
                        if state == RecoverState::None {
                            state = RecoverState::End;
                        }
                    }
                }
            }
        }

        // Flush undo store changes first before writing aborts to the log.
        self.store.flush()?;
        for unfinished_tid in unfinished_transactions.iter() {
            self.mem_log.push_back(UndoLogEntry::Transaction(Transaction::Abort(*unfinished_tid)));
        }

        // Set the last tid to the largest tid.
        let max_unfinished = unfinished_transactions.into_iter().max().unwrap_or(0);
        let max_finished = finished_transactions.into_iter().max().unwrap_or(0);
        self.last_tid = cmp::max(max_unfinished, max_finished);

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

    pub fn checkpoint(&mut self) -> io::Result<()> {
        if self.checkpoint_tids.is_none() {
            let transactions: Vec<_> = self.active_tids.clone().into_iter().collect();
            let entry = UndoLogEntry::Checkpoint(Checkpoint::Begin(transactions.clone()));
            self.mem_log.push_back(entry);
            self.flush()?;
            self.checkpoint_tids = Some(transactions);
        }

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

            // Add end checkpoint to log if all checkpoint transactions have finished.
            if let Some(tids) = self.checkpoint_tids.take() {
                let mut transactions_completed = true;
                for tid in tids.iter() {
                    if self.active_tids.contains(tid) {
                        transactions_completed = false;
                        break;
                    }
                }

                if transactions_completed {
                    let entry = UndoLogEntry::Checkpoint(Checkpoint::End);
                    self.mem_log.push_back(entry);
                    self.checkpoint_tids = None;
                } else {
                    self.checkpoint_tids = Some(tids);
                }
            }
            self.flush()?;
        }

        Ok(())
    }
}
