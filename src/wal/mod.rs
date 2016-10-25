pub mod iterator;
pub mod record;
pub mod undo_log;
pub mod writer;

use self::iterator::WalIterator;
use self::record::RecordType;

use std::io;
use std::io::{Read, Write};

pub trait Serializable: Sized {
    fn serialize<W: Write>(&self, bytes: &mut W) -> io::Result<()>;
    fn deserialize<R: Read>(bytes: &mut R) -> io::Result<Self>;
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
                return S::deserialize(&mut &buf[..]);
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
                if state != SerializableState::First || state != SerializableState::Middle {
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
