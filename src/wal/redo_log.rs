use std::collections::{VecDeque, HashSet};
use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

use wal::{append_to_file, LogData, LogStore, read_serializable, Serializable,
          split_bytes_into_records};
use wal::entries::{ChangeEntry, Checkpoint, InsertEntry, SingleLogEntry, Transaction};
use wal::iterator::WalIterator;

const MAX_RECORD_SIZE: usize = 1024;

pub struct RedoLog<Data: LogData, Store: LogStore<Data>> {
    file: File,
    mem_log: VecDeque<SingleLogEntry<Data>>,
    last_tid: u64,
    active_tids: HashSet<u64>,
    store: Store,
}

impl<Data, Store> RedoLog<Data, Store>
    where Data: LogData,
          Store: LogStore<Data>
{
    pub fn new<P: AsRef<Path> + ?Sized>(path: &P,
                                        store: Store)
                                        -> io::Result<RedoLog<Data, Store>> {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)?;
        let mut log = RedoLog {
            file: file,
            mem_log: VecDeque::new(),
            last_tid: 0,
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
        let transactions: Vec<_> = self.active_tids.clone().into_iter().collect();
        let entry = SingleLogEntry::Checkpoint(Checkpoint::Begin(transactions.clone()));

        // Add begin checkpoint into the log.
        self.mem_log.push_back(entry);

        // Ensure that all changes committed before the begin checkpoint are flushed to disk.
        // TODO(DarinM223): verify that this is correct.
        self.store.flush()?;

        // Add end checkpoint to log and flush the log.
        self.mem_log.push_back(SingleLogEntry::Checkpoint(Checkpoint::End));
        self.flush()?;

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
            let entry = if let Some(_) = self.store.get(&key) {
                SingleLogEntry::ChangeEntry(ChangeEntry {
                    tid: tid,
                    key: key.clone(),
                    value: val.clone(),
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
        unimplemented!()
    }
}
