#[macro_use]
extern crate enum_primitive;
extern crate byteorder;
extern crate crc;

pub mod testing;
pub mod wal;

use std::io;
use std::io::{Read, Write};

pub trait Serializable: Sized {
    fn serialize<W: Write>(&self, bytes: &mut W) -> io::Result<()>;
    fn deserialize<R: Read>(bytes: &mut R) -> io::Result<Self>;
}
