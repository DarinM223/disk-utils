pub mod entries;
pub mod iterator;
pub mod record;
pub mod redo_log;
pub mod serializable;
pub mod undo_log;

use self::iterator::{BlockError, WalIterator};
use self::record::{BLOCK_SIZE, Record, RecordType};

use std::collections::HashSet;
use std::fmt::Debug;
use std::fs::File;
use std::hash::Hash;
use std::io;
use std::io::Write;
use std::result;

use super::Serializable;

pub trait LogData: Clone + PartialEq + Debug {
    type Key: Clone + PartialEq + Eq + Debug + Hash + Serializable;
    type Value: Clone + PartialEq + Debug + Serializable;
}

pub trait LogStore<Data: LogData> {
    fn get(&self, key: &Data::Key) -> Option<Data::Value>;
    fn remove(&mut self, key: &Data::Key);
    fn update(&mut self, key: Data::Key, val: Data::Value);
    fn flush(&mut self) -> io::Result<()>;
    fn flush_change(&mut self, key: Data::Key, val: Data::Value) -> io::Result<()>;
}

#[derive(Debug)]
pub enum LogError {
    IoError(io::Error),
    BlockError(BlockError),
    SerializeError(SerializeError),
}

impl From<io::Error> for LogError {
    fn from(err: io::Error) -> LogError {
        LogError::IoError(err)
    }
}

impl From<BlockError> for LogError {
    fn from(err: BlockError) -> LogError {
        LogError::BlockError(err)
    }
}

impl From<SerializeError> for LogError {
    fn from(err: SerializeError) -> LogError {
        LogError::SerializeError(err)
    }
}

pub type Result<T> = result::Result<T, LogError>;

#[derive(PartialEq)]
enum RecoverState {
    /// No checkpoint entry found, read until end of log.
    None,
    /// Begin checkpoint entry found, read until the start entry
    /// of every transaction in the checkpoint is read.
    Begin(HashSet<u64>),
    /// End checkpoint entry found, read until a begin
    /// checkpoint entry is found.
    End,
}

#[derive(Debug)]
pub enum SerializeError {
    IoError(io::Error),
    InvalidTransfer(RecordType),
    OutOfRecords,
}

impl From<io::Error> for SerializeError {
    fn from(err: io::Error) -> SerializeError {
        SerializeError::IoError(err)
    }
}

#[derive(PartialEq)]
enum SerializeState {
    None,
    First,
    Middle,
}

pub type SerializeResult<T> = result::Result<T, SerializeError>;

pub fn read_serializable<S: Serializable>(iter: &mut WalIterator) -> SerializeResult<S> {
    let mut buf = Vec::new();
    let mut state = SerializeState::None;
    while let Some(mut record) = iter.next() {
        match record.record_type {
            RecordType::Zero | RecordType::Full => {
                return Ok(S::deserialize(&mut &record.payload[..])?);
            }
            RecordType::First => {
                if state != SerializeState::None {
                    return Err(SerializeError::InvalidTransfer(RecordType::First));
                }
                state = SerializeState::First;
                buf.append(&mut record.payload);
            }
            RecordType::Middle => {
                if state != SerializeState::First && state != SerializeState::Middle {
                    return Err(SerializeError::InvalidTransfer(RecordType::Middle));
                }
                state = SerializeState::Middle;
                buf.append(&mut record.payload);
            }
            RecordType::Last => {
                if state != SerializeState::Middle {
                    return Err(SerializeError::InvalidTransfer(RecordType::Last));
                }
                buf.append(&mut record.payload);
                return Ok(S::deserialize(&mut &buf[..])?);
            }
        }
    }

    Err(SerializeError::OutOfRecords)
}

pub fn read_serializable_backwards<S: Serializable>(iter: &mut WalIterator) -> SerializeResult<S> {
    let mut buf = Vec::new();
    let mut state = SerializeState::None;
    while let Some(mut record) = iter.next_back() {
        match record.record_type {
            RecordType::Zero | RecordType::Full => {
                return Ok(S::deserialize(&mut &record.payload[..])?);
            }
            RecordType::First => {
                if state != SerializeState::Middle {
                    return Err(SerializeError::InvalidTransfer(RecordType::First));
                }
                record.payload.reverse();
                buf.append(&mut record.payload);
                buf.reverse();
                return Ok(S::deserialize(&mut &buf[..])?);
            }
            RecordType::Middle => {
                if state != SerializeState::First && state != SerializeState::Middle {
                    return Err(SerializeError::InvalidTransfer(RecordType::Middle));
                }
                state = SerializeState::Middle;
                record.payload.reverse();
                buf.append(&mut record.payload);
            }
            RecordType::Last => {
                if state != SerializeState::None {
                    return Err(SerializeError::InvalidTransfer(RecordType::Last));
                }
                state = SerializeState::First;
                record.payload.reverse();
                buf.append(&mut record.payload);
            }
        }
    }

    Err(SerializeError::OutOfRecords)
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
