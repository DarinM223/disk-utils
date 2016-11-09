use std::collections::{VecDeque, HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::path::Path;

use wal::{append_to_file, LogData, LogStore, read_serializable, read_serializable_backwards,
          RecoverState, Result, Serializable, split_bytes_into_records};
use wal::entries::{ChangeEntry, Checkpoint, SingleLogEntry, Transaction};
use wal::iterator::{ReadDirection, WalIterator};

const MAX_RECORD_SIZE: usize = 1024;

pub struct RedoLog<Data: LogData, Store: LogStore<Data>> {
    file: File,
    mem_log: VecDeque<SingleLogEntry<Data>>,
    last_tid: u64,
    changes: Changes<Data>,
    active_tids: HashSet<u64>,
    store: Store,
}

impl<Data, Store> RedoLog<Data, Store>
    where Data: LogData,
          Store: LogStore<Data>
{
    pub fn new<P: AsRef<Path> + ?Sized>(path: &P, store: Store) -> Result<RedoLog<Data, Store>> {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)?;
        let mut log = RedoLog {
            file: file,
            mem_log: VecDeque::new(),
            last_tid: 0,
            changes: Changes::new(),
            active_tids: HashSet::new(),
            store: store,
        };
        log.recover()?;
        Ok(log)
    }

    pub fn entries(&self) -> Vec<SingleLogEntry<Data>> {
        self.mem_log.clone().into_iter().collect()
    }

    pub fn checkpoint(&mut self) -> Result<()> {
        let transactions: Vec<_> = self.active_tids.clone().into_iter().collect();
        let entry = SingleLogEntry::Checkpoint(Checkpoint::Begin(transactions.clone()));

        // Add begin checkpoint into the log.
        self.mem_log.push_back(entry);
        self.flush()?;

        // Ensure that all changes committed before the begin checkpoint are flushed to disk.
        // TODO(DarinM223): verify that this is correct.
        for (key, val) in self.changes.flush_changes() {
            self.store.flush_change(key, val)?;
        }

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
            let entry = SingleLogEntry::ChangeEntry(ChangeEntry {
                tid: tid,
                key: key.clone(),
                value: val.clone(),
            });

            self.changes.write(tid, key.clone(), val.clone());
            self.store.update(key, val);
            self.mem_log.push_back(entry);
        }
    }

    pub fn commit(&mut self, tid: u64) -> Result<()> {
        if self.active_tids.contains(&tid) {
            let entry = SingleLogEntry::Transaction(Transaction::Commit(tid));
            self.mem_log.push_back(entry);

            self.flush()?;
            self.store.flush()?;

            self.active_tids.remove(&tid);
            self.changes.commit(tid);
        }

        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
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

    fn recover(&mut self) -> Result<()> {
        let mut committed = HashSet::new();
        let mut uncommitted = HashSet::new();
        let mut aborted = HashSet::new();
        let mut state = RecoverState::None;

        {
            let mut iter = WalIterator::new(&mut self.file, ReadDirection::Backward)?;

            // First pass:
            while let Ok(data) = read_serializable_backwards::<SingleLogEntry<Data>>(&mut iter) {
                match data {
                    SingleLogEntry::Transaction(Transaction::Commit(id)) => {
                        committed.insert(id);
                    }
                    SingleLogEntry::Transaction(Transaction::Abort(id)) => {
                        aborted.insert(id);
                    }
                    SingleLogEntry::Transaction(Transaction::Start(id)) => {
                        if let RecoverState::Begin(ref mut transactions) = state {
                            transactions.remove(&id);
                            if transactions.is_empty() {
                                break;
                            }
                        }
                    }
                    SingleLogEntry::ChangeEntry(ref entry) => {
                        if !committed.contains(&entry.tid) && !aborted.contains(&entry.tid) {
                            uncommitted.insert(entry.tid);
                        }
                    }
                    SingleLogEntry::Checkpoint(Checkpoint::Begin(transactions)) => {
                        if state == RecoverState::End {
                            if transactions.is_empty() {
                                break;
                            }
                            state = RecoverState::Begin(transactions.into_iter().collect());
                        }
                    }
                    SingleLogEntry::Checkpoint(Checkpoint::End) if state == RecoverState::None => {
                        state = RecoverState::End;
                    }
                    _ => {}
                }
            }

            // TODO(DarinM223): check if iterator is in correct position right here (might need to
            // move the iterator up one).

            // Second pass:
            while let Ok(data) = read_serializable::<SingleLogEntry<Data>>(&mut iter) {
                if let SingleLogEntry::ChangeEntry(entry) = data {
                    if committed.contains(&entry.tid) {
                        self.store.update(entry.key, entry.value);
                    }
                }
            }
        }

        // Flush redo store changes first before writing aborts to the log.
        self.store.flush()?;
        for tid in uncommitted.iter() {
            self.mem_log.push_back(SingleLogEntry::Transaction(Transaction::Abort(*tid)));
        }

        // Set the last tid to the largest tid.
        let max_committed = committed.into_iter().max().unwrap_or(0);
        let max_uncommitted = uncommitted.into_iter().max().unwrap_or(0);
        let max_aborted = aborted.into_iter().max().unwrap_or(0);
        let max_tids = vec![max_committed, max_uncommitted, max_aborted];
        self.last_tid = max_tids.into_iter().max().unwrap();

        self.flush()?;
        Ok(())
    }
}


struct Changes<Data: LogData> {
    committed_tids: HashSet<u64>,
    transaction_changes: Vec<(u64, Data::Key, Data::Value)>,
}

impl<Data> Changes<Data>
    where Data: LogData
{
    fn new() -> Changes<Data> {
        Changes {
            committed_tids: HashSet::new(),
            transaction_changes: Vec::new(),
        }
    }

    fn write(&mut self, tid: u64, key: Data::Key, val: Data::Value) {
        self.transaction_changes.push((tid, key, val));
    }

    fn commit(&mut self, tid: u64) {
        self.committed_tids.insert(tid);
    }

    fn flush_changes(&self) -> HashMap<Data::Key, Data::Value> {
        let mut map = HashMap::new();
        for &(tid, ref key, ref value) in self.transaction_changes.iter() {
            if self.committed_tids.contains(&tid) {
                map.insert(key.clone(), value.clone());
            }
        }

        map
    }
}

#[test]
fn test_changes() {
    #[derive(Clone, PartialEq, Debug)]
    struct MyLogData;
    impl LogData for MyLogData {
        type Key = i32;
        type Value = String;
    }

    let mut changes: Changes<MyLogData> = Changes::new();
    changes.write(1, 2, "Hello".to_string());
    changes.write(2, 3, "World".to_string());
    changes.commit(1);

    let flush_changes = changes.flush_changes();
    assert_eq!(flush_changes.len(), 1);
    assert_eq!(flush_changes.get(&2), Some(&"Hello".to_string()));

    let mut changes: Changes<MyLogData> = Changes::new();
    changes.write(1, 2, "Hello".to_string());
    changes.write(2, 2, "World".to_string());
    changes.write(1, 3, "Blah".to_string());
    changes.write(3, 3, "Foo".to_string());

    changes.commit(3);
    changes.commit(1);

    let flush_changes = changes.flush_changes();
    assert_eq!(flush_changes.len(), 2);
    assert_eq!(flush_changes.get(&2), Some(&"Hello".to_string()));
    assert_eq!(flush_changes.get(&3), Some(&"Foo".to_string()));
}
