// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

//! Tezos binary data reader.

use failure::ResultExt;
use std::fmt::{self, Display};

use bit_vec::BitVec;
use bytes::Buf;
use failure::Fail;
use serde::de::Error as SerdeError;

use crate::bit_utils::{BitReverse, BitTrim, Bits, ToBytes};
use crate::de;
use crate::encoding::{Encoding, Field, SchemaType};
use crate::types::{self, Value};

use super::error_context::EncodingError;

/// Error produced by a [BinaryReader].
pub type BinaryReaderError = EncodingError<BinaryReaderErrorKind>;

/// Actual size of encoded data
#[derive(Debug, Clone, Copy)]
pub enum ActualSize {
    /// Exact size
    Exact(usize),
    /// Unknown size exceeding the limit
    GreaterThan(usize),
}

impl Display for ActualSize {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ActualSize::Exact(size) => write!(f, "{}", size),
            ActualSize::GreaterThan(size) => write!(f, "greater than {}", size),
        }
    }
}

/// Kind of error for [BinaryReaderError]
#[derive(Debug, Fail, Clone)]
pub enum BinaryReaderErrorKind {
    /// More bytes were expected than there were available in input buffer.
    #[fail(display = "Input underflow, missing {} bytes", bytes)]
    Underflow { bytes: usize },
    /// Writer tried to write too many bytes to output buffer.
    #[fail(display = "Input overflow, excess of {} bytes", bytes)]
    Overflow { bytes: usize },
    /// Generic deserialization error.
    #[fail(display = "Deserializer error: {}", error)]
    DeserializationError { error: crate::de::Error },
    /// No tag with the corresponding id was found. This might not be an error of the binary data but
    /// may simply mean that we have not yet defined tag in encoding.
    #[fail(display = "No tag found for id: 0x{:X}", tag)]
    UnsupportedTag { tag: u16 },
    /// Encoding boundary constraint violation
    #[fail(
        display = "Encoded data {} exceeded its size boundary: {}, actual: {}",
        name, boundary, actual
    )]
    EncodingBoundaryExceeded {
        name: String,
        boundary: usize,
        actual: ActualSize,
    },
    /// Arithmetic overflow
    #[fail(display = "Arithmetic overflow while encoding {:?}", encoding)]
    ArithmeticOverflow { encoding: &'static str },
}

impl From<crate::de::Error> for BinaryReaderError {
    fn from(error: crate::de::Error) -> Self {
        BinaryReaderErrorKind::DeserializationError { error }.into()
    }
}

impl From<std::string::FromUtf8Error> for BinaryReaderError {
    fn from(from: std::string::FromUtf8Error) -> Self {
        BinaryReaderErrorKind::DeserializationError {
            error: de::Error::custom(format!("Error decoding UTF-8 string. Reason: {:?}", from)),
        }
        .into()
    }
}

impl From<crate::bit_utils::BitsError> for BinaryReaderError {
    fn from(source: crate::bit_utils::BitsError) -> Self {
        BinaryReaderErrorKind::DeserializationError {
            error: crate::de::Error::custom(format!("Bits operation error: {:?}", source)),
        }
        .into()
    }
}

/// Safely read from input buffer. If input buffer does not contain enough bytes to construct desired error is returned.
#[macro_export]
macro_rules! safe {
    ($buf:ident, $foo:ident, $sz:ident) => {{
        use std::mem::size_of;
        if $buf.remaining() >= size_of::<$sz>() {
            $buf.$foo()
        } else {
            return Result::Err(BinaryReaderErrorKind::Underflow {
                bytes: (size_of::<$sz>() - $buf.remaining()),
            })?;
        }
    }};
    ($buf:ident, $sz:expr, $exp:expr) => {{
        if $buf.remaining() >= $sz {
            $exp
        } else {
            return Result::Err(BinaryReaderErrorKind::Underflow {
                bytes: ($sz - $buf.remaining()),
            })?;
        }
    }};
}

/// Converts Tezos binary form into rust types.
pub struct BinaryReader;

impl BinaryReader {
    /// Construct new instance of the [BinaryReader].
    pub fn new() -> Self {
        Self
    }

    /// Convert Tezos binary data into [intermadiate form](Value). Input binary is parsed according to [`encoding`](Encoding).
    ///
    /// # Examples:
    ///
    /// ```
    /// use serde::Deserialize;
    /// use tezos_encoding::binary_reader::BinaryReader;
    /// use tezos_encoding::de;
    /// use tezos_encoding::encoding::{Field, Encoding};
    ///
    /// #[derive(Deserialize, Debug, PartialEq)]
    /// struct Version {
    ///    name: String,
    ///    major: u16,
    ///    minor: u16,
    /// }
    ///
    /// let version_schema = Encoding::Obj("Rec", vec![
    ///     Field::new("name", Encoding::String),
    ///     Field::new("major", Encoding::Uint16),
    ///     Field::new("minor", Encoding::Uint16)
    /// ]);
    ///
    /// let reader = BinaryReader::new();
    /// // create intermediate form
    /// let intermediate = reader.read(hex::decode("0000000476312e3000010000").unwrap(), &version_schema).unwrap();
    /// // deserialize from intermediate form
    /// let version = de::from_value::<Version>(&intermediate).unwrap();
    ///
    /// let version_expected = Version { name: "v1.0".into(), major: 1, minor: 0 };
    ///
    /// assert_eq!(version, version_expected);
    /// ```
    pub fn read<Buf: AsRef<[u8]>>(
        &self,
        buf: Buf,
        encoding: &Encoding,
    ) -> Result<Value, BinaryReaderError> {
        let mut buf = buf.as_ref();

        let result = match encoding {
            Encoding::Obj(_, schema) => self.decode_record(&mut buf, schema),
            Encoding::Tup(encodings) => self.decode_tuple(&mut buf, encodings),
            _ => self.decode_value(&mut buf, encoding),
        }?;

        if buf.remaining() == 0 {
            Ok(result)
        } else {
            Err(BinaryReaderErrorKind::Overflow {
                bytes: buf.remaining(),
            })?
        }
    }

    fn decode_record(
        &self,
        buf: &mut &[u8],
        schema: &[Field],
    ) -> Result<Value, BinaryReaderError> {
        let mut values = Vec::with_capacity(schema.len());
        for field in schema {
            let name = field.get_name();
            let encoding = field.get_encoding();
            values.push((
                name.clone(),
                self.decode_value(buf, encoding)
                    .with_context(|e| e.field(field.get_name()))?,
            ))
        }
        Ok(Value::Record(values))
    }

    fn decode_tuple(
        &self,
        buf: &mut &[u8],
        encodings: &[Encoding],
    ) -> Result<Value, BinaryReaderError> {
        let mut values = Vec::with_capacity(encodings.len());
        for encoding in encodings {
            values.push(self.decode_value(buf, encoding)?)
        }
        Ok(Value::Tuple(values))
    }

    fn decode_value(
        &self,
        buf: &mut &[u8],
        encoding: &Encoding,
    ) -> Result<Value, BinaryReaderError> {
        match encoding {
            Encoding::Unit => Ok(Value::Unit),
            Encoding::Int8 => Ok(Value::Int8(safe!(buf, get_i8, i8))),
            Encoding::Uint8 => Ok(Value::Uint8(safe!(buf, get_u8, u8))),
            Encoding::Int16 => Ok(Value::Int16(safe!(buf, get_i16, i16))),
            Encoding::Uint16 => Ok(Value::Uint16(safe!(buf, get_u16, u16))),
            Encoding::Int31 => Ok(Value::Int31(safe!(buf, get_i32, i32))),
            Encoding::Int32 => Ok(Value::Int32(safe!(buf, get_i32, i32))),
            Encoding::Int64 | Encoding::Timestamp => Ok(Value::Int64(safe!(buf, get_i64, i64))),
            Encoding::Float => Ok(Value::Float(safe!(buf, get_f64, f64))),
            Encoding::Bool => {
                let b = safe!(buf, get_u8, u8);
                match b {
                    types::BYTE_VAL_TRUE => Ok(Value::Bool(true)),
                    types::BYTE_VAL_FALSE => Ok(Value::Bool(false)),
                    _ => Err(de::Error::custom(format!(
                        "Vas expecting 0xFF or 0x00 but instead got {:X}",
                        b
                    )))?,
                }
            }
            Encoding::String => {
                let bytes_sz = safe!(buf, get_u32, u32) as usize;
                let mut buf_slice = safe!(buf, bytes_sz, buf.take(bytes_sz));
                let mut str_buf = Vec::with_capacity(bytes_sz);
                while buf_slice.has_remaining() {
                    let chunk = buf_slice.chunk();
                    let len = chunk.len();
                    str_buf.extend_from_slice(chunk);
                    buf_slice.advance(len);
                }
                Ok(Value::String(String::from_utf8(str_buf)?))
            }
            Encoding::BoundedString(bytes_max) => {
                let bytes_sz = safe!(buf, get_u32, u32) as usize;
                if bytes_sz > *bytes_max {
                    Err(BinaryReaderErrorKind::EncodingBoundaryExceeded {
                        name: "Encoding::BoundedString".to_string(),
                        boundary: *bytes_max,
                        actual: ActualSize::Exact(bytes_sz),
                    })?
                } else {
                    let mut buf_slice = safe!(buf, bytes_sz, buf.take(bytes_sz));
                    let mut str_buf = Vec::with_capacity(bytes_sz);
                    while buf_slice.has_remaining() {
                        let chunk = buf_slice.chunk();
                        let len = chunk.len();
                        str_buf.extend_from_slice(chunk);
                        buf_slice.advance(len);
                    }
                    Ok(Value::String(String::from_utf8(str_buf)?))
                }
            }
            Encoding::Enum => Ok(Value::Enum(None, Some(u32::from(safe!(buf, get_u8, u8))))),
            Encoding::Dynamic(dynamic_encoding) => {
                let bytes_sz = safe!(buf, get_u32, u32) as usize;
                let mut buf_slice = safe!(buf, bytes_sz, { let (a, b) = buf.split_at(bytes_sz); *buf = b; a });
                self.decode_value(&mut buf_slice, dynamic_encoding)
            }
            Encoding::BoundedDynamic(max, dynamic_encoding) => {
                let bytes_sz = safe!(buf, get_u32, u32) as usize;
                if bytes_sz > *max {
                    Err(BinaryReaderErrorKind::EncodingBoundaryExceeded {
                        name: "Encoding::BoundedDynamic".to_string(),
                        boundary: *max,
                        actual: ActualSize::Exact(bytes_sz),
                    })?
                } else {
                    let mut buf_slice = safe!(buf, bytes_sz, { let (a, b) = buf.split_at(bytes_sz); *buf = b; a });
                    self.decode_value(&mut buf_slice, dynamic_encoding)
                }
            }
            Encoding::Sized(sized_size, sized_encoding) => {
                let mut buf_slice = safe!(buf, *sized_size, { let (a, b) = buf.split_at(*sized_size); *buf = b; a });
                self.decode_value(&mut buf_slice, sized_encoding)
            }
            Encoding::Bounded(max, inner_encoding) => {
                let upper = std::cmp::min(*max, buf.remaining());
                let mut buf_slice = safe!(buf, upper, { let (a, b) = buf.split_at(upper); *buf = b; a });
                let res = self.decode_value(&mut buf_slice, inner_encoding);
                match res {
                    // if underlying encoding requires more data than we have,
                    // and it is limited to maximal possible size, that means
                    // that this is bounded constraint violation.
                    Err(e) => match e.kind() {
                        BinaryReaderErrorKind::Underflow { bytes } if upper == *max => {
                            let act_size = bytes.checked_add(*max).ok_or_else(|| {
                                BinaryReaderErrorKind::ArithmeticOverflow {
                                    encoding: "Encoding::Bounded",
                                }
                            })?;
                            Err(BinaryReaderErrorKind::EncodingBoundaryExceeded {
                                name: "Encoding::Bounded".to_string(),
                                boundary: *max,
                                actual: ActualSize::Exact(act_size),
                            })?
                        }
                        _ => Err(e),
                    },
                    r => r,
                }
            }
            Encoding::Greedy(un_sized_encoding) => {
                let bytes_sz = buf.remaining();
                let mut buf_slice = safe!(buf, bytes_sz, { let (a, b) = buf.split_at(bytes_sz); *buf = b; a });
                self.decode_value(&mut buf_slice, un_sized_encoding)
            }
            Encoding::Tags(tag_sz, ref tag_map) => {
                let tag_id = match tag_sz {
                    /*u8*/ 1 => Ok(u16::from(safe!(buf, get_u8, u8))),
                    /*u16*/ 2 => Ok(safe!(buf, get_u16, u16)),
                    _ => Err(de::Error::custom(format!(
                        "Unsupported tag size {}",
                        tag_sz
                    ))),
                }?;

                match tag_map.find_by_id(tag_id) {
                    Some(tag) => {
                        let tag_value = self.decode_value(buf, tag.get_encoding())?;
                        Ok(Value::Tag(
                            tag.get_variant().to_string(),
                            Box::new(tag_value),
                        ))
                    }
                    None => Err(BinaryReaderErrorKind::UnsupportedTag { tag: tag_id })?,
                }
            }
            Encoding::List(encoding_inner) => {
                let bytes_sz = buf.remaining();

                let mut buf_slice = safe!(buf, bytes_sz, { let (a, b) = buf.split_at(bytes_sz); *buf = b; a });

                let mut values = vec![];
                while buf_slice.remaining() > 0 {
                    values.push(
                        self.decode_value(&mut buf_slice, encoding_inner)
                            .with_context(|e| e.element_of())?,
                    );
                }

                Ok(Value::List(values))
            }
            Encoding::BoundedList(max, encoding_inner) => {
                let bytes_sz = buf.remaining();

                let mut buf_slice = safe!(buf, bytes_sz, { let (a, b) = buf.split_at(bytes_sz); *buf = b; a });

                let mut values = vec![];
                while buf_slice.remaining() > 0 {
                    if values.len() >= *max {
                        return Err(BinaryReaderErrorKind::EncodingBoundaryExceeded {
                            name: "Encoding::List".to_string(),
                            boundary: *max,
                            actual: ActualSize::GreaterThan(values.len()),
                        })?;
                    }
                    values.push(
                        self.decode_value(&mut buf_slice, encoding_inner)
                            .with_context(|e| e.element_of())?,
                    );
                }

                Ok(Value::List(values))
            }
            Encoding::Option(inner_encoding) => {
                let is_present_byte = safe!(buf, get_u8, u8);
                match is_present_byte {
                    types::BYTE_VAL_SOME => {
                        let v = self.decode_value(buf, inner_encoding)?;
                        Ok(Value::Option(Some(Box::new(v))))
                    }
                    types::BYTE_VAL_NONE => Ok(Value::Option(None)),
                    _ => Err(de::Error::custom(format!(
                        "Unexpected option value {:X}",
                        is_present_byte
                    )))?,
                }
            }
            Encoding::OptionalField(inner_encoding) => {
                let is_present_byte = safe!(buf, get_u8, u8);
                match is_present_byte {
                    types::BYTE_FIELD_SOME => {
                        let v = self.decode_value(buf, inner_encoding)?;
                        Ok(Value::Option(Some(Box::new(v))))
                    }
                    types::BYTE_FIELD_NONE => Ok(Value::Option(None)),
                    _ => Err(de::Error::custom(format!(
                        "Unexpected option value {:X}",
                        is_present_byte
                    )))?,
                }
            }
            Encoding::Obj(_, schema_inner) => Ok(self.decode_record(buf, schema_inner)?),
            Encoding::Tup(encodings_inner) => Ok(self.decode_tuple(buf, encodings_inner)?),
            Encoding::Z => {
                // read first byte
                let byte = safe!(buf, get_u8, u8);
                let negative = byte.get(6)?;
                if byte <= 0x3F {
                    let mut num = i32::from(byte);
                    if negative {
                        num *= -1;
                    }
                    Ok(Value::String(format!("{:x}", num)))
                } else {
                    let mut bits = BitVec::new();
                    for bit_idx in 0..6 {
                        bits.push(byte.get(bit_idx)?);
                    }

                    let mut has_next_byte = true;
                    while has_next_byte {
                        let byte = safe!(buf, get_u8, u8);
                        for bit_idx in 0..7 {
                            bits.push(byte.get(bit_idx)?)
                        }

                        has_next_byte = byte.get(7)?;
                    }

                    let bytes = bits.reverse().trim_left().to_byte_vec();

                    let mut str_num = bytes
                        .iter()
                        .enumerate()
                        .map(|(idx, b)| match idx {
                            0 => format!("{:x}", *b),
                            _ => format!("{:02x}", *b),
                        })
                        .fold(String::new(), |mut str_num, val| {
                            str_num.push_str(&val);
                            str_num
                        });
                    if negative {
                        str_num = String::from("-") + &str_num;
                    }

                    Ok(Value::String(str_num))
                }
            }
            Encoding::Mutez => {
                let mut bits = BitVec::new();

                let mut has_next_byte = true;
                while has_next_byte {
                    let byte = safe!(buf, get_u8, u8);
                    for bit_idx in 0..7 {
                        bits.push(byte.get(bit_idx)?)
                    }

                    has_next_byte = byte.get(7)?;
                }

                let bytes = bits.reverse().trim_left().to_byte_vec();

                let str_num = bytes
                    .iter()
                    .enumerate()
                    .map(|(idx, b)| match idx {
                        0 => format!("{:x}", *b),
                        _ => format!("{:02x}", *b),
                    })
                    .fold(String::new(), |mut str_num, val| {
                        str_num.push_str(&val);
                        str_num
                    });

                Ok(Value::String(str_num))
            }
            Encoding::Bytes => {
                let bytes_sz = buf.remaining();
                let mut buf_slice = vec![0u8; bytes_sz].into_boxed_slice();
                buf.copy_to_slice(&mut buf_slice);
                Ok(Value::List(
                    buf_slice
                        .into_vec()
                        .iter()
                        .map(|&byte| Value::Uint8(byte))
                        .collect(),
                ))
            }
            Encoding::Hash(hash_type) => {
                let bytes_sz = hash_type.size();
                let mut buf_slice = vec![0u8; bytes_sz].into_boxed_slice();
                safe!(buf, bytes_sz, buf.copy_to_slice(&mut buf_slice));
                Ok(Value::List(
                    buf_slice
                        .into_vec()
                        .iter()
                        .map(|&byte| Value::Uint8(byte))
                        .collect(),
                ))
            }
            Encoding::Split(inner_encoding) => {
                let inner_encoding = inner_encoding(SchemaType::Binary);
                self.decode_value(buf, &inner_encoding)
            }
            Encoding::Lazy(fn_encoding) => {
                let inner_encoding = fn_encoding();
                self.decode_value(buf, &inner_encoding)
            }
            Encoding::Custom(codec) => codec.decode(buf, encoding),
            Encoding::Uint32 | Encoding::RangedInt | Encoding::RangedFloat => Err(
                de::Error::custom(format!("Unsupported encoding {:?}", encoding)),
            )?,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::mem::size_of;

    use serde::{Deserialize, Serialize};

    use crate::encoding::{Tag, TagMap};
    use crate::ser::Serializer;
    use crate::types::BigInt;
    use crate::{binary_writer, de};

    use super::*;

    #[test]
    fn can_deserialize_mutez_from_binary() {
        #[derive(Deserialize, Debug)]
        struct Record {
            a: BigInt,
        }
        let record_schema = vec![Field::new("a", Encoding::Mutez)];

        let record_buf = hex::decode("9e9ed49d01").unwrap();
        let reader = BinaryReader::new();
        let value = reader
            .read(record_buf, &Encoding::Obj("", record_schema))
            .unwrap();
        assert_eq!(
            Value::Record(vec![(
                "a".to_string(),
                Value::String("13b50f1e".to_string())
            )]),
            value
        )
    }

    #[test]
    fn can_deserialize_z_from_binary() {
        #[derive(Deserialize, Debug)]
        struct Record {
            a: BigInt,
        }
        let record_schema = vec![Field::new("a", Encoding::Z)];

        let record_buf = hex::decode("9e9ed49d01").unwrap();
        let reader = BinaryReader::new();
        let value = reader
            .read(record_buf, &Encoding::Obj("", record_schema))
            .unwrap();
        assert_eq!(
            Value::Record(vec![(
                "a".to_string(),
                Value::String("9da879e".to_string())
            )]),
            value
        )
    }

    #[test]
    fn can_deserialize_tag_from_binary() {
        #[derive(Deserialize, Debug, PartialEq)]
        struct GetHeadRecord {
            chain_id: Vec<u8>,
        }

        let get_head_record_schema = vec![Field::new(
            "chain_id",
            Encoding::Sized(4, Box::new(Encoding::Bytes)),
        )];

        #[derive(Deserialize, Debug, PartialEq)]
        enum Message {
            GetHead(GetHeadRecord),
        }

        #[derive(Deserialize, Debug, PartialEq)]
        struct Response {
            messages: Vec<Message>,
        }

        let response_schema = vec![Field::new(
            "messages",
            Encoding::dynamic(Encoding::list(Encoding::Tags(
                size_of::<u16>(),
                TagMap::new(vec![Tag::new(
                    0x10,
                    "GetHead",
                    Encoding::Obj("GetHead", get_head_record_schema),
                )]),
            ))),
        )];

        // deserialize to value
        let record_buf = hex::decode("0000000600108eceda2f").unwrap();
        let reader = BinaryReader::new();
        let value = reader
            .read(record_buf, &Encoding::Obj("", response_schema))
            .unwrap();
        // convert value to actual data structure
        let value: Response = de::from_value(&value).unwrap();
        let expected_value = Response {
            messages: vec![Message::GetHead(GetHeadRecord {
                chain_id: hex::decode("8eceda2f").unwrap(),
            })],
        };
        assert_eq!(expected_value, value)
    }

    #[test]
    fn can_deserialize_z_range() {
        #[derive(Serialize, Deserialize, Debug)]
        struct Record {
            a: BigInt,
        }
        let record_schema = vec![Field::new("a", Encoding::Z)];
        let record_encoding = Encoding::Obj("", record_schema);

        for num in -100..=100 {
            let num_mul = num * 1000;
            let record = Record {
                a: num_bigint::BigInt::from(num_mul).into(),
            };

            let mut serializer = Serializer::default();

            let value_serialized = record.serialize(&mut serializer).unwrap();
            let record_bytes = binary_writer::write(&record, &record_encoding).unwrap();

            let reader = BinaryReader::new();
            let value_deserialized = reader.read(record_bytes, &record_encoding).unwrap();

            assert_eq!(value_serialized, value_deserialized)
        }
    }

    #[test]
    fn can_deserialize_connection_message() {
        #[derive(Deserialize, Debug, PartialEq)]
        struct Version {
            name: String,
            major: u16,
            minor: u16,
        }

        #[derive(Deserialize, Debug, PartialEq)]
        struct ConnectionMessage {
            port: u16,
            versions: Vec<Version>,
            public_key: Vec<u8>,
            proof_of_work_stamp: Vec<u8>,
            message_nonce: Vec<u8>,
        }

        let version_schema = vec![
            Field::new("name", Encoding::String),
            Field::new("major", Encoding::Uint16),
            Field::new("minor", Encoding::Uint16),
        ];

        let connection_message_schema = vec![
            Field::new("port", Encoding::Uint16),
            Field::new("public_key", Encoding::sized(32, Encoding::Bytes)),
            Field::new("proof_of_work_stamp", Encoding::sized(24, Encoding::Bytes)),
            Field::new("message_nonce", Encoding::sized(24, Encoding::Bytes)),
            Field::new(
                "versions",
                Encoding::list(Encoding::Obj("", version_schema)),
            ),
        ];
        let connection_message_encoding = Encoding::Obj("", connection_message_schema);

        let connection_message = ConnectionMessage {
            port: 3001,
            versions: vec![
                Version {
                    name: "A".to_string(),
                    major: 1,
                    minor: 1,
                },
                Version {
                    name: "B".to_string(),
                    major: 2,
                    minor: 0,
                },
            ],
            public_key: hex::decode(
                "eaef40186db19fd6f56ed5b1af57f9d9c8a1eed85c29f8e4daaa7367869c0f0b",
            )
            .unwrap(),
            proof_of_work_stamp: hex::decode("000000000000000000000000000000000000000000000000")
                .unwrap(),
            message_nonce: hex::decode("000000000000000000000000000000000000000000000000").unwrap(),
        };

        let connection_message_buf = hex::decode("0bb9eaef40186db19fd6f56ed5b1af57f9d9c8a1eed85c29f8e4daaa7367869c0f0b000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000014100010001000000014200020000").unwrap();
        let reader = BinaryReader::new();
        let value = reader
            .read(connection_message_buf, &connection_message_encoding)
            .unwrap();

        let connection_message_deserialized: ConnectionMessage = de::from_value(&value).unwrap();
        assert_eq!(connection_message, connection_message_deserialized);
    }

    #[test]
    fn can_deserialize_option_some() {
        #[derive(Deserialize, Debug, PartialEq)]
        struct Record {
            pub forking_block_hash: Vec<u8>,
        }

        let record_schema = vec![Field::new(
            "forking_block_hash",
            Encoding::list(Encoding::Uint8),
        )];
        let record_encoding = Encoding::Obj("", record_schema);

        let record = Some(Record {
            forking_block_hash: hex::decode(
                "2253698f0c94788689fb95ca35eb1535ec3a8b7c613a97e6683f8007d7959e4b",
            )
            .unwrap(),
        });

        let message_buf =
            hex::decode("012253698f0c94788689fb95ca35eb1535ec3a8b7c613a97e6683f8007d7959e4b")
                .unwrap();
        let reader = BinaryReader::new();
        let value = reader
            .read(message_buf, &Encoding::option(record_encoding))
            .unwrap();

        let record_deserialized: Option<Record> = de::from_value(&value).unwrap();
        assert_eq!(record, record_deserialized);
    }

    #[test]
    fn can_deserialize_option_none() {
        #[derive(Deserialize, Debug, PartialEq)]
        struct Record {
            pub forking_block_hash: Vec<u8>,
        }

        let record_schema = vec![Field::new(
            "forking_block_hash",
            Encoding::list(Encoding::Uint8),
        )];
        let record_encoding = Encoding::Obj("", record_schema);

        let record: Option<Record> = None;

        let message_buf = hex::decode("00").unwrap();
        let reader = BinaryReader::new();
        let value = reader
            .read(message_buf, &Encoding::option(record_encoding))
            .unwrap();

        let record_deserialized: Option<Record> = de::from_value(&value).unwrap();
        assert_eq!(record, record_deserialized);
    }

    #[test]
    fn deserialize_bounds_error_location_string() {
        let schema = Encoding::Obj("", vec![Field::new("xxx", Encoding::BoundedString(1))]);
        let data = hex::decode("000000020000").unwrap();
        let err = BinaryReader::new()
            .read(data, &schema)
            .expect_err("Error is expected");
        let kind = err.kind();
        assert!(matches!(
            kind,
            BinaryReaderErrorKind::EncodingBoundaryExceeded {
                name: _,
                boundary: 1,
                actual: ActualSize::Exact(2)
            }
        ));
        let location = err.location();
        assert!(location.contains("field `xxx`"));
    }

    #[test]
    fn deserialize_bounds_error_location_list() {
        let schema = Encoding::Obj(
            "",
            vec![Field::new(
                "xxx",
                Encoding::bounded_list(1, Encoding::Uint8),
            )],
        );
        let data = hex::decode("0000").unwrap();
        let err = BinaryReader::new()
            .read(data, &schema)
            .expect_err("Error is expected");
        let kind = err.kind();
        assert!(matches!(
            kind,
            BinaryReaderErrorKind::EncodingBoundaryExceeded {
                name: _,
                boundary: 1,
                actual: ActualSize::GreaterThan(1)
            }
        ));
        let location = err.location();
        assert!(location.contains("field `xxx`"));
    }

    #[test]
    fn deserialize_bounds_error_location_element_of() {
        let schema = Encoding::Obj(
            "",
            vec![Field::new(
                "xxx",
                Encoding::list(Encoding::BoundedString(1)),
            )],
        );
        let data = hex::decode("000000020000").unwrap();
        let err = BinaryReader::new()
            .read(data, &schema)
            .expect_err("Error is expected");
        let kind = err.kind();
        assert!(matches!(
            kind,
            BinaryReaderErrorKind::EncodingBoundaryExceeded {
                name: _,
                boundary: 1,
                actual: ActualSize::Exact(2)
            }
        ));
        let location = err.location();
        assert!(location.contains("field `xxx`"));
        assert!(location.contains("list element"));
    }

    #[test]
    fn underflow_in_bounded() {
        let encoded = hex::decode("000000ff00112233445566778899AABBCCDDEEFF").unwrap(); // dynamic block states 255 bytes, got only 16
        let encoding = Encoding::bounded(1000, Encoding::dynamic(Encoding::list(Encoding::Uint8)));
        let err = BinaryReader::new()
            .read(encoded, &encoding)
            .expect_err("Error is expected");
        assert!(
            matches!(err.kind(), BinaryReaderErrorKind::Underflow { .. }),
            "Underflow error expected, got '{:?}'",
            err
        );
    }
}
