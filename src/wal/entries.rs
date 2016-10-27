use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};

use std::io;
use std::io::{Cursor, Read, Write};

use wal::Serializable;

#[derive(Clone)]
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

#[derive(Clone)]
pub struct ChangeEntry<A, B> {
    pub tid: u64,
    pub key: A,
    pub old: B,
}

impl<A, B> Serializable for ChangeEntry<A, B>
    where A: Serializable,
          B: Serializable
{
    fn serialize<W: Write>(&self, bytes: &mut W) -> io::Result<()> {
        let mut wtr = Vec::new();
        wtr.write_u64::<BigEndian>(self.tid)?;
        bytes.write(&wtr)?;
        self.key.serialize(bytes)?;
        self.old.serialize(bytes)?;

        Ok(())
    }

    fn deserialize<R: Read>(bytes: &mut R) -> io::Result<ChangeEntry<A, B>> {
        let mut buf = [0; 8];
        bytes.read(&mut buf)?;
        let mut rdr = Cursor::new(buf[..].to_vec());
        let tid = rdr.read_u64::<BigEndian>()?;
        let (key, old) = (A::deserialize(bytes)?, B::deserialize(bytes)?);

        Ok(ChangeEntry {
            tid: tid,
            key: key,
            old: old,
        })
    }
}
