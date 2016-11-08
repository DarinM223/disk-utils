use std::cmp;
use std::collections::{VecDeque, HashSet};
use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

use wal::{append_to_file, LogData, LogStore, read_serializable_backwards, RecoverState,
          Serializable, split_bytes_into_records};
use wal::entries::{ChangeEntry, Checkpoint, InsertEntry, SingleLogEntry, Transaction};
use wal::iterator::WalIterator;

const MAX_RECORD_SIZE: usize = 1024;

pub struct UndoLog<Data: LogData, Store: LogStore<Data>> {
    file: File,
    mem_log: VecDeque<SingleLogEntry<Data>>,
    last_tid: u64,
    checkpoint_tids: Option<Vec<u64>>,
    active_tids: HashSet<u64>,
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

    pub fn entries(&self) -> Vec<SingleLogEntry<Data>> {
        self.mem_log.clone().into_iter().collect()
    }

    pub fn checkpoint(&mut self) -> io::Result<()> {
        if self.checkpoint_tids.is_none() {
            let transactions: Vec<_> = self.active_tids.clone().into_iter().collect();
            let entry = SingleLogEntry::Checkpoint(Checkpoint::Begin(transactions.clone()));
            self.mem_log.push_back(entry);
            self.flush()?;
            self.checkpoint_tids = Some(transactions);
        }

        Ok(())
    }

    pub fn start(&mut self) -> u64 {
        self.last_tid += 1;
        let entry = SingleLogEntry::Transaction(Transaction::Start(self.last_tid));
        self.mem_log.push_back(entry);
        self.active_tids.insert(self.last_tid);

        self.last_tid
    }

    pub fn write(&mut self, tid: u64, key: Data::Key, val: Data::Value) {
        if self.active_tids.contains(&tid) {
            let entry = if let Some(old_value) = self.store.get(&key) {
                SingleLogEntry::ChangeEntry(ChangeEntry {
                    tid: tid,
                    key: key.clone(),
                    value: old_value,
                })
            } else {
                SingleLogEntry::InsertEntry(InsertEntry {
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

            let entry = SingleLogEntry::Transaction(Transaction::Commit(tid));
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
                    let entry = SingleLogEntry::Checkpoint(Checkpoint::End);
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

    fn flush(&mut self) -> io::Result<()> {
        for entry in self.mem_log.iter_mut() {
            let mut bytes = Vec::new();
            entry.serialize(&mut bytes)?;

            let records = split_bytes_into_records(bytes, MAX_RECORD_SIZE)?;
            for record in records.iter() {
                append_to_file(&mut self.file, record)?;
            }
        }
        self.mem_log.clear();
        Ok(())
    }

    fn recover(&mut self) -> io::Result<()> {
        let mut finished = HashSet::new();
        let mut unfinished = HashSet::new();
        let mut state = RecoverState::None;

        {
            let mut iter = WalIterator::new(&mut self.file)?;
            while let Ok(data) = read_serializable_backwards::<SingleLogEntry<Data>>(&mut iter) {
                match data {
                    SingleLogEntry::Transaction(Transaction::Commit(id)) => {
                        finished.insert(id);
                    }
                    SingleLogEntry::Transaction(Transaction::Abort(id)) => {
                        finished.insert(id);
                    }
                    SingleLogEntry::Transaction(Transaction::Start(id)) => {
                        if let RecoverState::Begin(ref mut transactions) = state {
                            transactions.remove(&id);
                            if transactions.is_empty() {
                                break;
                            }
                        }
                    }
                    SingleLogEntry::InsertEntry(entry) => {
                        if !finished.contains(&entry.tid) {
                            self.store.remove(&entry.key);
                            unfinished.insert(entry.tid);
                        }
                    }
                    SingleLogEntry::ChangeEntry(entry) => {
                        if !finished.contains(&entry.tid) {
                            self.store.update(entry.key, entry.value);
                            unfinished.insert(entry.tid);
                        }
                    }
                    SingleLogEntry::Checkpoint(Checkpoint::Begin(transactions)) => {
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
                    SingleLogEntry::Checkpoint(Checkpoint::End) => {
                        if state == RecoverState::None {
                            state = RecoverState::End;
                        }
                    }
                }
            }
        }

        // Flush undo store changes first before writing aborts to the log.
        self.store.flush()?;
        for tid in unfinished.iter() {
            self.mem_log.push_back(SingleLogEntry::Transaction(Transaction::Abort(*tid)));
        }

        // Set the last tid to the largest tid.
        let max_unfinished = unfinished.into_iter().max().unwrap_or(0);
        let max_finished = finished.into_iter().max().unwrap_or(0);
        self.last_tid = cmp::max(max_unfinished, max_finished);

        self.flush()?;
        Ok(())
    }
}
