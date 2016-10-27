pub mod entries;
pub mod iterator;
pub mod record;
pub mod undo_log;
pub mod writer;

use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};

use self::iterator::WalIterator;
use self::record::{Record, RecordType};

use std::io;
use std::io::{Cursor, Read, Write};

pub trait Serializable: Sized {
    fn serialize<W: Write>(&self, bytes: &mut W) -> io::Result<()>;
    fn deserialize<R: Read>(bytes: &mut R) -> io::Result<Self>;
}

impl Serializable for String {
    fn serialize<W: Write>(&self, bytes: &mut W) -> io::Result<()> {
        let mut wtr = Vec::new();
        wtr.write_u32::<BigEndian>(self.len() as u32)?;
        let (l1, l2, l3, l4) = (wtr[0], wtr[1], wtr[2], wtr[3]);

        bytes.write(&[l1, l2, l3, l4])?;
        bytes.write(self.as_bytes())?;
        Ok(())
    }

    fn deserialize<R: Read>(bytes: &mut R) -> io::Result<String> {
        let mut len_buf = [0; 4];
        bytes.read(&mut len_buf)?;

        let mut rdr = Cursor::new(vec![len_buf[0], len_buf[1], len_buf[2], len_buf[3]]);
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
        bytes.write(&[wtr[0], wtr[1], wtr[2], wtr[3]])?;
        Ok(())
    }

    fn deserialize<R: Read>(bytes: &mut R) -> io::Result<i32> {
        let mut buf = [0; 4];
        bytes.read(&mut buf)?;

        let mut rdr = Cursor::new(vec![buf[0], buf[1], buf[2], buf[3]]);
        Ok(rdr.read_i32::<BigEndian>()?)
    }
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

pub fn split_bytes_into_records(bytes: Vec<u8>, max_record_size: usize) -> io::Result<Vec<Record>> {
    let mut records: Vec<_> = bytes.chunks(max_record_size)
        .map(|bytes| {
            Record {
                crc: 0,
                size: bytes.len() as u16,
                record_type: RecordType::Middle,
                payload: bytes.to_vec(),
            }
        })
        .collect();
    if records.len() == 1 {
        records.first_mut().unwrap().record_type = RecordType::Full;
    } else if records.len() > 1 {
        records.first_mut().unwrap().record_type = RecordType::First;
        records.last_mut().unwrap().record_type = RecordType::Last;
    } else {
        records.push(Record {
            crc: 0,
            size: 0,
            record_type: RecordType::Zero,
            payload: vec![],
        });
    }

    Ok(records)
}
