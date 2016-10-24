pub mod iterator;
pub mod record;
pub mod undo_log;
pub mod writer;

use self::iterator::WalIterator;
use self::record::{Record, RecordType};

use std::io;

pub trait Serializable {
    fn serialize(&self) -> io::Result<Vec<Record>>;
    fn deserialize(&mut self, records: Vec<Record>) -> io::Result<()>;
}

#[derive(PartialEq)]
enum SerializableState {
    None,
    First,
    Middle,
}

pub fn read_serializable<S: Serializable>(iter: &mut WalIterator,
                                          serializable: &mut S)
                                          -> io::Result<()> {
    let mut records = Vec::new();
    let mut state = SerializableState::None;
    while let Some(record) = iter.next() {
        match record.record_type {
            RecordType::Zero | RecordType::Full => {
                serializable.deserialize(vec![record])?;
                break;
            }
            RecordType::First => {
                if state != SerializableState::None {
                    return Err(io::Error::new(io::ErrorKind::InvalidData,
                                              "Invalid transfer to first state"));
                }
                state = SerializableState::First;
                records.push(record);
            }
            RecordType::Middle => {
                if state != SerializableState::First || state != SerializableState::Middle {
                    return Err(io::Error::new(io::ErrorKind::InvalidData,
                                              "Invalid transfer to middle state"));
                }
                state = SerializableState::Middle;
                records.push(record);
            }
            RecordType::Last => {
                if state != SerializableState::Middle {
                    return Err(io::Error::new(io::ErrorKind::InvalidData,
                                              "Invalid transfer to last state"));
                }
                records.push(record);
                serializable.deserialize(records)?;
                break;
            }
        }
    }

    Ok(())
}
