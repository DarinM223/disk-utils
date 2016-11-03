pub mod entries;
pub mod iterator;
pub mod record;
pub mod undo_log;
pub mod writer;

use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};

use self::iterator::WalIterator;
use self::record::{Record, RecordType};

use std::fmt::Debug;
use std::hash::Hash;
use std::io;
use std::io::{Cursor, Read, Write};

pub trait Serializable: Sized {
    fn serialize<W: Write>(&self, bytes: &mut W) -> io::Result<()>;
    fn deserialize<R: Read>(bytes: &mut R) -> io::Result<Self>;
}

impl Serializable for String {
    fn serialize<W: Write>(&self, bytes: &mut W) -> io::Result<()> {
        let mut len_bytes = Vec::new();
        len_bytes.write_u32::<BigEndian>(self.len() as u32)?;

        bytes.write(&len_bytes)?;
        bytes.write(self.as_bytes())?;
        Ok(())
    }

    fn deserialize<R: Read>(bytes: &mut R) -> io::Result<String> {
        let mut len_buf = [0; 4];
        bytes.read(&mut len_buf)?;

        let mut rdr = Cursor::new(len_buf[..].to_vec());
        let len = rdr.read_u32::<BigEndian>()?;

        let mut str_bytes = vec![0; len as usize];
        bytes.read(&mut str_bytes)?;

        // TODO(DarinM223): change from io::Result to custom result supporting Utf8Error.
        Ok(String::from_utf8(str_bytes).expect("Error converting bytes"))
    }
}

impl Serializable for i32 {
    fn serialize<W: Write>(&self, bytes: &mut W) -> io::Result<()> {
        let mut wtr = Vec::new();
        wtr.write_i32::<BigEndian>(*self)?;
        bytes.write(&wtr)?;
        Ok(())
    }

    fn deserialize<R: Read>(bytes: &mut R) -> io::Result<i32> {
        let mut buf = [0; 4];
        bytes.read(&mut buf)?;

        let mut rdr = Cursor::new(buf[..].to_vec());
        Ok(rdr.read_i32::<BigEndian>()?)
    }
}

impl Serializable for u64 {
    fn serialize<W: Write>(&self, bytes: &mut W) -> io::Result<()> {
        let mut num_bytes = Vec::new();
        num_bytes.write_u64::<BigEndian>(*self)?;
        bytes.write(&num_bytes)?;
        Ok(())
    }

    fn deserialize<R: Read>(bytes: &mut R) -> io::Result<u64> {
        let mut buf = [0; 8];
        bytes.read(&mut buf)?;

        let mut num_reader = Cursor::new(buf[..].to_vec());
        num_reader.read_u64::<BigEndian>()
    }
}

pub trait LogData: Clone + PartialEq + Debug {
    type Key: Clone + PartialEq + Eq + Debug + Hash + Serializable;
    type Value: Clone + PartialEq + Debug + Serializable;
}

pub trait LogStore<Data: LogData> {
    fn get(&self, key: &Data::Key) -> Option<Data::Value>;
    fn remove(&mut self, key: &Data::Key);
    fn update(&mut self, key: Data::Key, val: Data::Value);
    fn flush(&mut self) -> io::Result<()>;
}

#[derive(PartialEq)]
enum SerializableState {
    None,
    First,
    Middle,
}

pub fn read_serializable<S: Serializable>(iter: &mut WalIterator) -> io::Result<S> {
    let mut buf = Vec::new();
    let mut state = SerializableState::None;
    while let Some(mut record) = iter.next() {
        match record.record_type {
            RecordType::Zero | RecordType::Full => {
                return S::deserialize(&mut &record.payload[..]);
            }
            RecordType::First => {
                if state != SerializableState::None {
                    return Err(io::Error::new(io::ErrorKind::InvalidData,
                                              "Invalid transfer to first state"));
                }
                state = SerializableState::First;
                buf.append(&mut record.payload);
            }
            RecordType::Middle => {
                if state != SerializableState::First && state != SerializableState::Middle {
                    return Err(io::Error::new(io::ErrorKind::InvalidData,
                                              "Invalid transfer to middle state"));
                }
                state = SerializableState::Middle;
                buf.append(&mut record.payload);
            }
            RecordType::Last => {
                if state != SerializableState::Middle {
                    return Err(io::Error::new(io::ErrorKind::InvalidData,
                                              "Invalid transfer to last state"));
                }
                buf.append(&mut record.payload);
                return S::deserialize(&mut &buf[..]);
            }
        }
    }

    Err(io::Error::new(io::ErrorKind::UnexpectedEof,
                       "Ran out of records before attempting to deserialize"))
}

pub fn read_serializable_backwards<S: Serializable>(iter: &mut WalIterator) -> io::Result<S> {
    let mut buf = Vec::new();
    let mut state = SerializableState::None;
    while let Some(mut record) = iter.next_back() {
        match record.record_type {
            RecordType::Zero | RecordType::Full => {
                return S::deserialize(&mut &record.payload[..]);
            }
            RecordType::First => {
                if state != SerializableState::Middle {
                    return Err(io::Error::new(io::ErrorKind::InvalidData,
                                              "Invalid transfer to last state"));
                }
                record.payload.reverse();
                buf.append(&mut record.payload);
                buf.reverse();
                return S::deserialize(&mut &buf[..]);
            }
            RecordType::Middle => {
                if state != SerializableState::First && state != SerializableState::Middle {
                    return Err(io::Error::new(io::ErrorKind::InvalidData,
                                              "Invalid transfer to middle state"));
                }
                state = SerializableState::Middle;
                record.payload.reverse();
                buf.append(&mut record.payload);
            }
            RecordType::Last => {
                if state != SerializableState::None {
                    return Err(io::Error::new(io::ErrorKind::InvalidData,
                                              "Invalid transfer to first state"));
                }
                state = SerializableState::First;
                record.payload.reverse();
                buf.append(&mut record.payload);
            }
        }
    }

    Err(io::Error::new(io::ErrorKind::UnexpectedEof,
                       "Ran out of records before attempting to deserialize"))
}

pub fn split_bytes_into_records(bytes: Vec<u8>, max_record_size: usize) -> io::Result<Vec<Record>> {
    let mut records: Vec<_> = bytes.chunks(max_record_size)
        .map(|bytes| Record::new(RecordType::Middle, bytes.to_vec()))
        .collect();
    if records.len() == 1 {
        records.first_mut().unwrap().record_type = RecordType::Full;
    } else if records.len() > 1 {
        records.first_mut().unwrap().record_type = RecordType::First;
        records.last_mut().unwrap().record_type = RecordType::Last;
    } else {
        records.push(Record::new(RecordType::Zero, vec![]));
    }

    Ok(records)
}
