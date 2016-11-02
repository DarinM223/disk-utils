use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};

use std::io;
use std::io::{Cursor, Read, Write};

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

        let mut tid_bytes = Vec::new();
        tid_bytes.write_u64::<BigEndian>(tid)?;
        bytes.write(&tid_bytes)?;
        Ok(())
    }

    fn deserialize<R: Read>(bytes: &mut R) -> io::Result<Transaction> {
        let mut transaction_type = [0; 1];
        bytes.read(&mut transaction_type)?;

        let mut buf = [0; 8];
        bytes.read(&mut buf)?;

        let mut rdr = Cursor::new(buf[..].to_vec());
        let tid = rdr.read_u64::<BigEndian>()?;

        match transaction_type[0] {
            0 => Ok(Transaction::Start(tid)),
            1 => Ok(Transaction::Commit(tid)),
            2 => Ok(Transaction::Abort(tid)),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid transaction type")),
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
        let mut wtr = Vec::new();
        wtr.write_u64::<BigEndian>(self.tid)?;
        bytes.write(&wtr)?;
        self.key.serialize(bytes)?;

        Ok(())
    }

    fn deserialize<R: Read>(bytes: &mut R) -> io::Result<InsertEntry<Data>> {
        let mut buf = [0; 8];
        bytes.read(&mut buf)?;
        let mut rdr = Cursor::new(buf[..].to_vec());
        let tid = rdr.read_u64::<BigEndian>()?;
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
        let mut wtr = Vec::new();
        wtr.write_u64::<BigEndian>(self.tid)?;
        bytes.write(&wtr)?;
        self.key.serialize(bytes)?;
        self.old.serialize(bytes)?;

        Ok(())
    }

    fn deserialize<R: Read>(bytes: &mut R) -> io::Result<ChangeEntry<Data>> {
        let mut buf = [0; 8];
        bytes.read(&mut buf)?;
        let mut rdr = Cursor::new(buf[..].to_vec());
        let tid = rdr.read_u64::<BigEndian>()?;
        let (key, old) = (Data::Key::deserialize(bytes)?, Data::Value::deserialize(bytes)?);

        Ok(ChangeEntry {
            tid: tid,
            key: key,
            old: old,
        })
    }
}
