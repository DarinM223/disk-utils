use std::io;
use std::io::{Read, Write};

use super::super::Serializable;

use crate::wal::LogData;

#[derive(Clone, Debug, PartialEq)]
pub enum Transaction {
    Start(u64),
    Commit(u64),
    Abort(u64),
}

impl Serializable for Transaction {
    fn serialize<W: Write>(&self, bytes: &mut W) -> io::Result<()> {
        match *self {
            Transaction::Start(_) => bytes.write(&[0])?,
            Transaction::Commit(_) => bytes.write(&[1])?,
            Transaction::Abort(_) => bytes.write(&[2])?,
        };

        let tid = match *self {
            Transaction::Start(tid) => tid,
            Transaction::Commit(tid) => tid,
            Transaction::Abort(tid) => tid,
        };

        tid.serialize(bytes)?;
        Ok(())
    }

    fn deserialize<R: Read>(bytes: &mut R) -> io::Result<Transaction> {
        let mut transaction_type = [0; 1];
        bytes.read_exact(&mut transaction_type)?;

        let tid = u64::deserialize(bytes)?;
        match transaction_type[0] {
            0 => Ok(Transaction::Start(tid)),
            1 => Ok(Transaction::Commit(tid)),
            2 => Ok(Transaction::Abort(tid)),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid transaction type",
            )),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Checkpoint {
    Begin(Vec<u64>),
    End,
}

impl Serializable for Checkpoint {
    fn serialize<W: Write>(&self, bytes: &mut W) -> io::Result<()> {
        match *self {
            Checkpoint::Begin(ref transactions) => {
                bytes.write_all(&[0])?;
                (transactions.len() as i32).serialize(bytes)?;
                for tid in transactions.iter() {
                    tid.serialize(bytes)?;
                }
            }
            Checkpoint::End => {
                bytes.write_all(&[1])?;
            }
        }

        Ok(())
    }

    fn deserialize<R: Read>(bytes: &mut R) -> io::Result<Checkpoint> {
        let mut checkpoint_type = [0; 1];
        bytes.read_exact(&mut checkpoint_type)?;

        match checkpoint_type[0] {
            0 => {
                let len = i32::deserialize(bytes)?;
                let mut transactions = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    transactions.push(u64::deserialize(bytes)?);
                }

                Ok(Checkpoint::Begin(transactions))
            }
            1 => Ok(Checkpoint::End),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid checkpoint type",
            )),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct InsertEntry<Data: LogData> {
    pub tid: u64,
    pub key: Data::Key,
}

impl<Data> Serializable for InsertEntry<Data>
where
    Data: LogData,
{
    fn serialize<W: Write>(&self, bytes: &mut W) -> io::Result<()> {
        self.tid.serialize(bytes)?;
        self.key.serialize(bytes)?;

        Ok(())
    }

    fn deserialize<R: Read>(bytes: &mut R) -> io::Result<InsertEntry<Data>> {
        let tid = u64::deserialize(bytes)?;
        let key = Data::Key::deserialize(bytes)?;

        Ok(InsertEntry { tid, key })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ChangeEntry<Data: LogData> {
    pub tid: u64,
    pub key: Data::Key,
    pub value: Data::Value,
}

impl<Data> Serializable for ChangeEntry<Data>
where
    Data: LogData,
{
    fn serialize<W: Write>(&self, bytes: &mut W) -> io::Result<()> {
        self.tid.serialize(bytes)?;
        self.key.serialize(bytes)?;
        self.value.serialize(bytes)?;

        Ok(())
    }

    fn deserialize<R: Read>(bytes: &mut R) -> io::Result<ChangeEntry<Data>> {
        let tid = u64::deserialize(bytes)?;
        let (key, value) = (
            Data::Key::deserialize(bytes)?,
            Data::Value::deserialize(bytes)?,
        );

        Ok(ChangeEntry { tid, key, value })
    }
}

/// Main log entry for undo logs and redo logs.
/// This entry type is not used by undo/redo logs.
#[derive(Clone, Debug, PartialEq)]
pub enum SingleLogEntry<Data: LogData> {
    InsertEntry(InsertEntry<Data>),
    ChangeEntry(ChangeEntry<Data>),
    Transaction(Transaction),
    Checkpoint(Checkpoint),
}

impl<Data> Serializable for SingleLogEntry<Data>
where
    Data: LogData,
{
    fn serialize<W: Write>(&self, bytes: &mut W) -> io::Result<()> {
        match *self {
            SingleLogEntry::InsertEntry(ref entry) => {
                bytes.write_all(&[0])?;
                entry.serialize(bytes)
            }
            SingleLogEntry::ChangeEntry(ref entry) => {
                bytes.write_all(&[1])?;
                entry.serialize(bytes)
            }
            SingleLogEntry::Transaction(ref entry) => {
                bytes.write_all(&[2])?;
                entry.serialize(bytes)
            }
            SingleLogEntry::Checkpoint(ref entry) => {
                bytes.write_all(&[3])?;
                entry.serialize(bytes)
            }
        }
    }

    fn deserialize<R: Read>(bytes: &mut R) -> io::Result<SingleLogEntry<Data>> {
        let mut entry_type = [0; 1];
        bytes.read_exact(&mut entry_type)?;

        match entry_type[0] {
            0 => Ok(SingleLogEntry::InsertEntry(InsertEntry::deserialize(
                bytes,
            )?)),
            1 => Ok(SingleLogEntry::ChangeEntry(ChangeEntry::deserialize(
                bytes,
            )?)),
            2 => Ok(SingleLogEntry::Transaction(Transaction::deserialize(
                bytes,
            )?)),
            3 => Ok(SingleLogEntry::Checkpoint(Checkpoint::deserialize(bytes)?)),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid entry type",
            )),
        }
    }
}
