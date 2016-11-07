pub mod entries;
pub mod iterator;
pub mod record;
pub mod redo_log;
pub mod serializable;
pub mod undo_log;

use self::iterator::WalIterator;
use self::record::{BLOCK_SIZE, Record, RecordType};

use std::fmt::Debug;
use std::fs::File;
use std::hash::Hash;
use std::io;
use std::io::{Read, Write};

pub trait Serializable: Sized {
    fn serialize<W: Write>(&self, bytes: &mut W) -> io::Result<()>;
    fn deserialize<R: Read>(bytes: &mut R) -> io::Result<Self>;
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

pub fn append_to_file(file: &mut File, record: &Record) -> io::Result<()> {
    let file_len = file.metadata()?.len();
    let curr_block_len = file_len - (file_len / BLOCK_SIZE as u64) * BLOCK_SIZE as u64;
    if curr_block_len + record.payload.len() as u64 > BLOCK_SIZE as u64 {
        let padding_len = BLOCK_SIZE as u64 - curr_block_len;
        let padding = vec![0; padding_len as usize];
        file.write(&padding[..])?;
    }

    record.write(file)?;
    Ok(())
}
