use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::CodecError;
use crate::table::{FieldTable, LongString};

/// Cursor over an AMQP payload. Consecutive `bit` fields share octets, as the spec requires.
pub struct Reader {
    buf: Bytes,
    bits: u8,
    bit_pos: u8,
}

impl Reader {
    pub fn new(buf: Bytes) -> Self {
        Self {
            buf,
            bits: 0,
            bit_pos: 0,
        }
    }

    pub fn remaining(&self) -> usize {
        self.buf.remaining()
    }

    fn need(&self, n: usize, what: &'static str) -> Result<(), CodecError> {
        if self.buf.remaining() < n {
            Err(CodecError::Truncated(what))
        } else {
            Ok(())
        }
    }

    pub fn u8(&mut self) -> Result<u8, CodecError> {
        self.bit_pos = 0;
        self.need(1, "octet")?;
        Ok(self.buf.get_u8())
    }

    pub fn u16(&mut self) -> Result<u16, CodecError> {
        self.bit_pos = 0;
        self.need(2, "short")?;
        Ok(self.buf.get_u16())
    }

    pub fn u32(&mut self) -> Result<u32, CodecError> {
        self.bit_pos = 0;
        self.need(4, "long")?;
        Ok(self.buf.get_u32())
    }

    pub fn u64(&mut self) -> Result<u64, CodecError> {
        self.bit_pos = 0;
        self.need(8, "longlong")?;
        Ok(self.buf.get_u64())
    }

    pub fn bit(&mut self) -> Result<bool, CodecError> {
        if self.bit_pos == 0 {
            self.need(1, "bit")?;
            self.bits = self.buf.get_u8();
        }
        let value = self.bits & (1 << self.bit_pos) != 0;
        self.bit_pos = (self.bit_pos + 1) % 8;
        Ok(value)
    }

    pub fn bytes(&mut self, n: usize, what: &'static str) -> Result<Bytes, CodecError> {
        self.bit_pos = 0;
        self.need(n, what)?;
        Ok(self.buf.split_to(n))
    }

    pub fn short_str(&mut self) -> Result<String, CodecError> {
        let len = self.u8()? as usize;
        let raw = self.bytes(len, "shortstr")?;
        String::from_utf8(raw.to_vec()).map_err(|_| CodecError::InvalidUtf8)
    }

    pub fn long_bytes(&mut self) -> Result<Bytes, CodecError> {
        let len = self.u32()? as usize;
        self.bytes(len, "longstr")
    }

    pub fn table(&mut self) -> Result<FieldTable, CodecError> {
        FieldTable::read(self)
    }

    pub fn into_rest(self) -> Bytes {
        self.buf
    }
}

/// Appends AMQP-encoded values to a buffer, packing consecutive bits into octets.
pub struct Writer<'a> {
    out: &'a mut BytesMut,
    bits: u8,
    bit_pos: u8,
}

impl<'a> Writer<'a> {
    pub fn new(out: &'a mut BytesMut) -> Self {
        Self {
            out,
            bits: 0,
            bit_pos: 0,
        }
    }

    pub fn flush_bits(&mut self) {
        if self.bit_pos > 0 {
            self.out.put_u8(self.bits);
            self.bits = 0;
            self.bit_pos = 0;
        }
    }

    pub fn u8(&mut self, v: u8) {
        self.flush_bits();
        self.out.put_u8(v);
    }

    pub fn u16(&mut self, v: u16) {
        self.flush_bits();
        self.out.put_u16(v);
    }

    pub fn u32(&mut self, v: u32) {
        self.flush_bits();
        self.out.put_u32(v);
    }

    pub fn u64(&mut self, v: u64) {
        self.flush_bits();
        self.out.put_u64(v);
    }

    pub fn bit(&mut self, v: bool) {
        if v {
            self.bits |= 1 << self.bit_pos;
        }
        self.bit_pos += 1;
        if self.bit_pos == 8 {
            self.flush_bits();
        }
    }

    pub fn raw(&mut self, data: &[u8]) {
        self.flush_bits();
        self.out.put_slice(data);
    }

    /// Writes a short string, truncating at 255 bytes on a character boundary.
    pub fn short_str(&mut self, s: &str) {
        let mut end = s.len().min(255);
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        self.u8(end as u8);
        self.raw(&s.as_bytes()[..end]);
    }

    pub fn long_bytes(&mut self, b: &[u8]) {
        self.u32(b.len() as u32);
        self.raw(b);
    }

    pub fn table(&mut self, t: &FieldTable) {
        self.flush_bits();
        t.write(self.out);
    }
}

/// Types that can appear as method arguments.
pub trait Field: Sized {
    fn read(r: &mut Reader) -> Result<Self, CodecError>;
    fn write(&self, w: &mut Writer<'_>);
}

impl Field for u8 {
    fn read(r: &mut Reader) -> Result<Self, CodecError> {
        r.u8()
    }
    fn write(&self, w: &mut Writer<'_>) {
        w.u8(*self)
    }
}

impl Field for u16 {
    fn read(r: &mut Reader) -> Result<Self, CodecError> {
        r.u16()
    }
    fn write(&self, w: &mut Writer<'_>) {
        w.u16(*self)
    }
}

impl Field for u32 {
    fn read(r: &mut Reader) -> Result<Self, CodecError> {
        r.u32()
    }
    fn write(&self, w: &mut Writer<'_>) {
        w.u32(*self)
    }
}

impl Field for u64 {
    fn read(r: &mut Reader) -> Result<Self, CodecError> {
        r.u64()
    }
    fn write(&self, w: &mut Writer<'_>) {
        w.u64(*self)
    }
}

impl Field for bool {
    fn read(r: &mut Reader) -> Result<Self, CodecError> {
        r.bit()
    }
    fn write(&self, w: &mut Writer<'_>) {
        w.bit(*self)
    }
}

impl Field for String {
    fn read(r: &mut Reader) -> Result<Self, CodecError> {
        r.short_str()
    }
    fn write(&self, w: &mut Writer<'_>) {
        w.short_str(self)
    }
}

impl Field for LongString {
    fn read(r: &mut Reader) -> Result<Self, CodecError> {
        Ok(LongString(r.long_bytes()?))
    }
    fn write(&self, w: &mut Writer<'_>) {
        w.long_bytes(&self.0)
    }
}

impl Field for FieldTable {
    fn read(r: &mut Reader) -> Result<Self, CodecError> {
        r.table()
    }
    fn write(&self, w: &mut Writer<'_>) {
        w.table(self)
    }
}
