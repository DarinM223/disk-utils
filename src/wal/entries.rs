use std::io;
use std::io::{Read, Write};

use wal::{LogData, Serializable};

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
        bytes.read(&mut transaction_type)?;

        let tid = u64::deserialize(bytes)?;
        match transaction_type[0] {
            0 => Ok(Transaction::Start(tid)),
            1 => Ok(Transaction::Commit(tid)),
            2 => Ok(Transaction::Abort(tid)),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid transaction type")),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Checkpoint {
    Begin(Vec<Transaction>),
    End,
}

impl Serializable for Checkpoint {
    fn serialize<W: Write>(&self, bytes: &mut W) -> io::Result<()> {
        match *self {
            Checkpoint::Begin(ref transactions) => {
                bytes.write(&[0])?;
                (transactions.len() as i32).serialize(bytes)?;
                for transaction in transactions.iter() {
                    transaction.serialize(bytes)?;
                }
            }
            Checkpoint::End => {
                bytes.write(&[1])?;
            }
        }

        Ok(())
    }

    fn deserialize<R: Read>(bytes: &mut R) -> io::Result<Checkpoint> {
        let mut checkpoint_type = [0; 1];
        bytes.read(&mut checkpoint_type)?;

        match checkpoint_type[0] {
            0 => {
                let len = i32::deserialize(bytes)?;
                let mut transactions = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    transactions.push(Transaction::deserialize(bytes)?);
                }

                Ok(Checkpoint::Begin(transactions))
            }
            1 => Ok(Checkpoint::End),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid checkpoint type")),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct InsertEntry<Data: LogData> {
    pub tid: u64,
    pub key: Data::Key,
}

impl<Data> Serializable for InsertEntry<Data>
    where Data: LogData
{
    fn serialize<W: Write>(&self, bytes: &mut W) -> io::Result<()> {
        self.tid.serialize(bytes)?;
        self.key.serialize(bytes)?;

        Ok(())
    }

    fn deserialize<R: Read>(bytes: &mut R) -> io::Result<InsertEntry<Data>> {
        let tid = u64::deserialize(bytes)?;
        let key = Data::Key::deserialize(bytes)?;

        Ok(InsertEntry {
            tid: tid,
            key: key,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ChangeEntry<Data: LogData> {
    pub tid: u64,
    pub key: Data::Key,
    pub old: Data::Value,
}

impl<Data> Serializable for ChangeEntry<Data>
    where Data: LogData
{
    fn serialize<W: Write>(&self, bytes: &mut W) -> io::Result<()> {
        self.tid.serialize(bytes)?;
        self.key.serialize(bytes)?;
        self.old.serialize(bytes)?;

        Ok(())
    }

    fn deserialize<R: Read>(bytes: &mut R) -> io::Result<ChangeEntry<Data>> {
        let tid = u64::deserialize(bytes)?;
        let (key, old) = (Data::Key::deserialize(bytes)?, Data::Value::deserialize(bytes)?);

        Ok(ChangeEntry {
            tid: tid,
            key: key,
            old: old,
        })
    }
}
