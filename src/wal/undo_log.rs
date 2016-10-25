use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};

use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Cursor, Read, Write};
use std::path::Path;
use std::result;
use std::sync::{Arc, Mutex};

use wal::Serializable;
use wal::record::Record;
use wal::writer::Writer;

pub enum UndoLogError {
    IoError(io::Error),
    LockError,
}

impl From<io::Error> for UndoLogError {
    fn from(err: io::Error) -> UndoLogError {
        UndoLogError::IoError(err)
    }
}

pub type Result<T> = result::Result<T, UndoLogError>;

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

pub struct UndoLog {
    file: File,
    mutex: Arc<Mutex<u8>>,
}

impl UndoLog {
    pub fn new<P: AsRef<Path>>(path: &P) -> Result<UndoLog> {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)?;

        Ok(UndoLog {
            file: file,
            mutex: Arc::new(Mutex::new(0)),
        })
    }

    pub fn write_change<A: Serializable, B: Serializable>(&mut self, key: A, val: B) -> Result<()> {
        let lock = self.mutex.lock().map_err(|_| UndoLogError::LockError)?;
        let mut writer = Writer::new(&mut self.file);
        let entry = ChangeEntry {
            tid: 0, // TODO(DarinM223): handle tid.
            key: key,
            old: val,
        };
        let mut bytes = Vec::new();
        entry.serialize(&mut bytes)?;
        let records = split_bytes_into_records(bytes)?;
        for record in records.iter() {
            writer.append(record)?;
        }
        Ok(())
    }
}

fn split_bytes_into_records(bytes: Vec<u8>) -> io::Result<Vec<Record>> {
    unimplemented!()
}
