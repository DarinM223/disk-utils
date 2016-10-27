use std::io;
use std::io::{Cursor, Read, Write};
use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};
use wal::Serializable;

#[derive(Clone)]
pub enum Transaction {
    Start(u64),
    Commit(u64),
    Abort(u64),
}

impl Serializable for Transaction {
    fn serialize<W: Write>(&self, bytes: &mut W) -> io::Result<()> {
        unimplemented!()
    }

    fn deserialize<R: Read>(bytes: &mut R) -> io::Result<Transaction> {
        unimplemented!()
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
        bytes.write(&[wtr[0], wtr[1], wtr[2], wtr[3], wtr[4], wtr[5], wtr[6], wtr[7]])?;
        self.key.serialize(bytes)?;
        self.old.serialize(bytes)?;

        Ok(())
    }

    fn deserialize<R: Read>(bytes: &mut R) -> io::Result<ChangeEntry<A, B>> {
        let mut buf = [0; 8];
        bytes.read(&mut buf)?;
        let mut rdr = Cursor::new(vec![buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6],
                                       buf[7]]);
        let tid = rdr.read_u64::<BigEndian>()?;
        let (key, old) = (A::deserialize(bytes)?, B::deserialize(bytes)?);

        Ok(ChangeEntry {
            tid: tid,
            key: key,
            old: old,
        })
    }
}
