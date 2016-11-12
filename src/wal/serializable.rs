use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};

use std::io;
use std::io::{Cursor, Read, Write};

use super::super::Serializable;

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

        String::from_utf8(str_bytes).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "Error converting bytes to UTF8")
        })
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
