// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

//! Tezos binary data writer.

use std::cmp;
use std::mem::size_of;

use bit_vec::BitVec;
use byteorder::{BigEndian, WriteBytesExt};
use bytes::BufMut;
use serde::ser::{Error as SerdeError, Serialize};

use crate::bit_utils::{Bits, BitTrim};
use crate::encoding::{Encoding, Field, SchemaType};
use crate::ser::{Error, Serializer};
use crate::types::{self, Value};

/// Converts rust types into Tezos binary form.

/// Convert rust type into Tezos binary form. Binary form is defined by [`encoding`](Encoding).
///
/// # Examples:
///
/// ```
/// use serde::Serialize;
/// use tezos_encoding::binary_writer;
/// use tezos_encoding::encoding::{Field, Encoding};
///
/// #[derive(Serialize, Debug)]
/// struct Version {
///    name: String,
///    major: u16,
///    minor: u16,
/// }
/// let version = Version { name: "v1.0".into(), major: 1, minor: 0 };
///
/// let version_schema = Encoding::Obj(vec![
///     Field::new("name", Encoding::String),
///     Field::new("major", Encoding::Uint16),
///     Field::new("minor", Encoding::Uint16)
/// ]);
///
/// let binary = binary_writer::write(&version, &version_schema).unwrap();
///
/// assert_eq!(binary, hex::decode("0000000476312e3000010000").unwrap());
/// ```
pub fn write<T>(data: &T, encoding: &Encoding) -> Result<Vec<u8>, Error>
    where
        T: ?Sized + Serialize
{
    let mut serializer = Serializer::default();
    let value = data.serialize(&mut serializer)?;

    let mut data = Vec::with_capacity(512);

    encode_any(&mut data, &value, encoding)?;

    Ok(data)
}

fn encode_any(data: &mut Vec<u8>, value: &Value, encoding: &Encoding) -> Result<usize, Error> {
    if let Encoding::Obj(ref schema) = encoding {
        encode_record(data, value, schema)
    } else if let Encoding::Tup(ref encodings) = encoding {
        encode_tuple(data, value, encodings)
    } else {
        encode_value(data, value, encoding)
    }
}

fn encode_record(data: &mut Vec<u8>, value: &Value, schema: &[Field]) -> Result<usize, Error> {
    match value {
        Value::Record(ref values) => {
            let mut bytes_sz: usize = 0;
            for field in schema {
                let name = field.get_name();
                let value = find_value_in_record_values(name, values).unwrap_or_else(|| panic!("No values found for {}", name));
                let encoding = field.get_encoding();

                bytes_sz += encode_any(data, value, encoding)?;
            }

            Ok(bytes_sz)
        }
        _ => Err(Error::encoding_mismatch(&Encoding::Obj(schema.to_vec()), value))
    }
}

fn encode_tuple(data: &mut Vec<u8>, value: &Value, encodings: &[Encoding]) -> Result<usize, Error> {
    if let Value::Tuple(ref values) = value {
        let mut bytes_sz: usize = 0;
        for (index, encoding) in encodings.into_iter().enumerate() {
            if let Some(value) = values.get(index) {
                bytes_sz += encode_any(data, value, encoding)?;
            } else {
                return Err(Error::encoding_mismatch(&Encoding::Tup(encodings.to_vec()), value));
            }
        }
        Ok(bytes_sz)
    } else {
        Err(Error::encoding_mismatch(&Encoding::Tup(encodings.to_vec()), value))
    }
}

fn encode_value(data: &mut Vec<u8>, value: &Value, encoding: &Encoding) -> Result<usize, Error> {
    match encoding {
        Encoding::Unit => {
            Ok(0)
        }
        Encoding::Int8 => {
            match value {
                Value::Int8(v) => {
                    data.put_i8(*v);
                    Ok(size_of::<i8>())
                }
                _ => Err(Error::encoding_mismatch(encoding, value))
            }
        }
        Encoding::Uint8 => {
            match value {
                Value::Uint8(v) => {
                    data.put_u8(*v);
                    Ok(size_of::<u8>())
                }
                _ => Err(Error::encoding_mismatch(encoding, value))
            }
        }
        Encoding::Int16 => {
            match value {
                Value::Int16(v) => {
                    data.put_i16(*v);
                    Ok(size_of::<i16>())
                }
                _ => Err(Error::encoding_mismatch(encoding, value))
            }
        }
        Encoding::Uint16 => {
            match value {
                Value::Uint16(v) => {
                    data.put_u16(*v);
                    Ok(size_of::<u16>())
                }
                _ => Err(Error::encoding_mismatch(encoding, value))
            }
        }
        Encoding::Int32 => {
            match value {
                Value::Int32(v) => {
                    data.put_i32(*v);
                    Ok(size_of::<i32>())
                }
                _ => Err(Error::encoding_mismatch(encoding, value))
            }
        }
        Encoding::Int31 => {
            match value {
                Value::Int32(v) => {
                    if (*v & 0x7FFF_FFFF) == *v {
                        data.put_i32(*v);
                        Ok(size_of::<i32>())
                    } else {
                        Err(Error::custom("Value is outside of Int31 range"))
                    }
                }
                _ => Err(Error::encoding_mismatch(encoding, value))
            }
        }
        Encoding::Uint32 => {
            match value {
                Value::Int32(v) => {
                    if *v >= 0 {
                        data.put_i32(*v);
                        Ok(size_of::<i32>())
                    } else {
                        Err(Error::custom("Value is outside of Uint32 range"))
                    }
                }
                _ => Err(Error::encoding_mismatch(encoding, value))
            }
        }
        Encoding::RangedInt => {
            unimplemented!("Encoding::RangedInt is not implemented")
        }
        Encoding::RangedFloat => {
            unimplemented!("Encoding::RangedFloat is not implemented")
        }
        Encoding::Int64 |
        Encoding::Timestamp => {
            match value {
                Value::Int64(v) => {
                    data.put_i64(*v);
                    Ok(size_of::<i64>())
                }
                _ => Err(Error::encoding_mismatch(encoding, value))
            }
        }
        Encoding::Float => {
            match value {
                Value::Float(v) => {
                    data.put_f64(*v);
                    Ok(size_of::<f64>())
                }
                _ => Err(Error::encoding_mismatch(encoding, value))
            }
        }
        Encoding::Bool => {
            match value {
                Value::Bool(v) => {
                    if *v { data.put_u8(types::BYTE_VAL_TRUE) } else { data.put_u8(types::BYTE_VAL_FALSE) };
                    Ok(size_of::<u8>())
                }
                _ => Err(Error::encoding_mismatch(encoding, value))
            }
        }
        Encoding::Z | Encoding::Mutez => {
            match value {
                Value::String(v) => encode_z(data, v),
                _ => Err(Error::encoding_mismatch(encoding, value))
            }
        }
        Encoding::String => {
            match value {
                Value::String(v) => {
                    data.put_u32(v.len() as u32);
                    data.put_slice(v.as_bytes());
                    Ok(size_of::<u32>() + v.len())
                }
                _ => Err(Error::encoding_mismatch(encoding, value))
            }
        }
        Encoding::Enum => {
            match value {
                Value::Enum(_, ordinal) => {
                    // TODO: handle enum of different sizes
                    data.put_u8(ordinal.expect("Was expecting enum ordinal value") as u8);
                    Ok(size_of::<u8>())
                }
                _ => Err(Error::encoding_mismatch(encoding, value))
            }
        }
        Encoding::List(list_inner_encoding) => {
            match value {
                Value::List(values) => {
                    let data_len_before_write = data.len();
                    // write data
                    for value in values {
                        encode_value(data, value, list_inner_encoding)?;
                    }
                    Ok(data.len() - data_len_before_write)
                }
                _ => Err(Error::encoding_mismatch(encoding, value))
            }
        }
        Encoding::Bytes => {
            match value {
                Value::List(values) => {
                    let data_len_before_write = data.len();
                    for value in values {
                        match value {
                            Value::Uint8(u8_val) => data.put_u8(*u8_val),
                            _ => return Err(Error::custom(format!("Encoding::Bytes could be applied only to &[u8] value but found: {:?}", value)))
                        }
                    }
                    Ok(data.len() - data_len_before_write)
                }
                _ => Err(Error::encoding_mismatch(encoding, value))
            }
        }
        Encoding::Hash(hash_type) => {
            match value {
                Value::List(ref values) => {
                    let data_len_before_write = data.len();
                    for value in values {
                        match value {
                            Value::Uint8(u8_val) => data.put_u8(*u8_val),
                            _ => return Err(Error::custom(format!("Encoding::Hash could be applied only to &[u8] value but found: {:?}", value)))
                        }
                    }

                    // count of bytes written
                    let bytes_sz = data.len() - data_len_before_write;

                    // check if writen bytes is equal to expected hash size
                    if bytes_sz == hash_type.size() {
                        Ok(bytes_sz)
                    } else {
                        Err(Error::custom(format!("Was expecting {} bytes but got {}", hash_type.size(), bytes_sz)))
                    }
                }
                _ => Err(Error::encoding_mismatch(encoding, value))
            }
        }
        Encoding::Option(option_encoding) => {
            match value {
                Value::Option(ref wrapped_value) => {
                    match wrapped_value {
                        Some(option_value) => {
                            data.put_u8(types::BYTE_VAL_SOME);
                            let bytes_sz = encode_value(data, option_value, option_encoding)?;
                            Ok(size_of::<u8>() + bytes_sz)
                        }
                        None => {
                            data.put_u8(types::BYTE_VAL_NONE);
                            Ok(size_of::<u8>())
                        }
                    }
                }
                _ => Err(Error::encoding_mismatch(encoding, value))
            }
        }
        Encoding::OptionalField(option_encoding) => {
            match value {
                Value::Option(ref wrapped_value) => {
                    match wrapped_value {
                        Some(option_value) => {
                            data.put_u8(types::BYTE_FIELD_SOME);
                            let bytes_sz = encode_value(data, option_value, option_encoding)?;
                            Ok(size_of::<u8>() + bytes_sz)
                        }
                        None => {
                            data.put_u8(types::BYTE_FIELD_NONE);
                            Ok(size_of::<u8>())
                        }
                    }
                }
                _ => Err(Error::encoding_mismatch(encoding, value))
            }
        }
        Encoding::Dynamic(dynamic_encoding) => {
            let data_len_before_write = data.len();
            // put 0 as a placeholder
            data.put_u32(0);
            // we will use this info to create slice of buffer where inner record size will be stored
            let data_len_after_size_placeholder = data.len();

            // write data
            let bytes_sz = encode_value(data, value, dynamic_encoding)?;

            // capture slice of buffer where List length was stored
            let mut bytes_sz_slice = &mut data[data_len_before_write..data_len_after_size_placeholder];
            // update size
            bytes_sz_slice.write_u32::<BigEndian>(bytes_sz as u32)?;

            Ok(data.len() - data_len_before_write)
        }
        Encoding::Sized(sized_size, sized_encoding) => {
            // write data
            let bytes_sz = encode_value(data, value, sized_encoding)?;

            if bytes_sz == *sized_size {
                Ok(bytes_sz)
            } else {
                Err(Error::custom(format!("Was expecting {} bytes but got {}", bytes_sz, sized_size)))
            }
        }
        Encoding::Greedy(un_sized_encoding) => {
            encode_value(data, value, un_sized_encoding)
        }
        Encoding::Tags(tag_sz, tag_map) => {
            match value {
                Value::Tag(ref tag_variant, ref tag_value) => {
                    match tag_map.find_by_variant(tag_variant) {
                        Some(tag) => {
                            let data_len_before_write = data.len();
                            // write tag id
                            write_tag_id(data, *tag_sz, tag.get_id())?;
                            // encode value
                            encode_value(data, tag_value, tag.get_encoding())?;

                            Ok(data.len() - data_len_before_write)
                        }
                        None => Err(Error::custom(format!("No tag found for variant: {}", tag_variant)))
                    }
                }
                Value::Enum(ref tag_variant, _) => {
                    let tag_variant = tag_variant.as_ref().expect("Was expecting variant name");
                    match tag_map.find_by_variant(tag_variant) {
                        Some(tag) => {
                            let data_len_before_write = data.len();
                            // write tag id
                            write_tag_id(data, *tag_sz, tag.get_id())?;

                            Ok(data.len() - data_len_before_write)
                        }
                        None => Err(Error::custom(format!("No tag found for variant: {}", tag_variant)))
                    }
                }
                _ => Err(Error::encoding_mismatch(encoding, value))
            }
        }
        Encoding::Split(fn_encoding) => {
            let inner_encoding = fn_encoding(SchemaType::Binary);
            encode_value(data, value, &inner_encoding)
        }
        Encoding::Lazy(fn_encoding) => {
            let inner_encoding = fn_encoding();
            encode_value(data, value, &inner_encoding)
        }
        Encoding::Obj(obj_schema) => {
            encode_record(data, value, obj_schema)
        }
        Encoding::Tup(tup_encodings) => {
            encode_tuple(data, value, tup_encodings)
        }
    }
}

fn write_tag_id(data: &mut Vec<u8>, tag_sz: usize, tag_id: u16) -> Result<(), Error> {
    match tag_sz {
        1 => Ok(data.put_u8(tag_id as u8)),
        2 => Ok(data.put_u16(tag_id)),
        _ => Err(Error::custom(format!("Unsupported tag size {}", tag_sz)))
    }
}

fn encode_z(data: &mut Vec<u8>, value: &str) -> Result<usize, Error> {
    let (decode_offset, negative) = {
        if let Some(sign) = value.chars().next() {
            if sign.is_alphanumeric() {
                (0, false)
            } else if sign == '-' { (1, true) } else { (1, false) }
        } else {
            return Err(Error::custom("Cannot process empty value"));
        }
    };

    let mut hex_value = value[decode_offset..].to_string();
    if (hex_value.len() % 2) == 1 {
        hex_value = "0".to_string() + &hex_value;
    }

    let bytes = hex::decode(&hex_value)?;

    if (bytes.len() == 1) && (bytes[0] <= 0x3F) {
        // 0x3F == 0b111111 --> encoded value will fit into 1 byte (2b "header" + 6b value)
        let mut byte = bytes[0];
        if negative {
            byte |= 0x40;
        }
        data.put_u8(byte);
        Ok(size_of::<u8>())
    } else {
        let data_len_before_write: usize = data.len();

        // At the beginning we have to process first 6 bits because we have to indicate
        // continuation of bit chunks and to set one bit to indicate numeric sign (0-positive, 1-negative).
        // Then the algorithm continues by processing 7 bit chunks.
        let mut bits = BitVec::from_bytes(&bytes);
        bits = bits.trim_left();

        let mut n: u8 = if negative { 0xC0 } else { 0x80 };
        for bit_idx in 0..6 {
            n.set(bit_idx, bits.pop().unwrap());
        }
        data.put_u8(n);

        let chunk_size = 7;
        let last_chunk_idx = (bits.len() - 1) / chunk_size;

        for chunk_idx in 0..=last_chunk_idx {
            let mut n = 0u8;
            let bit_count = cmp::min(chunk_size, bits.len()) as u8;
            for bit_idx in 0..bit_count {
                n.set(bit_idx, bits.pop().unwrap());
            }
            // set continuation bit if there are other chunks to be processed
            if chunk_idx != last_chunk_idx {
                n.set(7, true);
            }
            data.put_u8(n)
        }

        Ok(data.len() - data_len_before_write)
    }
}

fn find_value_in_record_values<'a>(name: &'a str, values: &'a [(String, Value)]) -> Option<&'a Value> {
    values.iter()
        .find(|&(v_name, _)| { v_name == name })
        .map(|(_, value)| value)
}

#[cfg(test)]
mod tests {
    use serde::Serialize;

    use crate::encoding::{Tag, TagMap};
    use crate::types::BigInt;

    use super::*;

    #[test]
    fn can_serialize_z_positive_to_binary() {
        #[derive(Serialize, Debug)]
        struct Record {
            a: BigInt
        }
        let record_schema = vec![
            Field::new("a", Encoding::Z)
        ];
        let record_encoding = Encoding::Obj(record_schema);

        {
            let record = Record {
                a: num_bigint::BigInt::from(165_316_510).into()
            };
            let writer_result = write(&record, &record_encoding).unwrap();
            let expected_writer_result = hex::decode("9e9ed49d01").unwrap();
            assert_eq!(expected_writer_result, writer_result);
        }

        {
            let record = Record {
                a: num_bigint::BigInt::from(3000).into()
            };
            let writer_result = write(&record, &record_encoding).unwrap();
            let expected_writer_result = hex::decode("b82e").unwrap();
            assert_eq!(expected_writer_result, writer_result);
        }
    }

    #[test]
    fn can_serialize_mutez_to_binary() {
        #[derive(Serialize, Debug)]
        struct Record {
            a: BigInt
        }
        let record_schema = vec![
            Field::new("a", Encoding::Mutez)
        ];
        let record_encoding = Encoding::Obj(record_schema);

        {
            let record = Record {
                a: num_bigint::BigInt::from(165_316_510).into()
            };
            let writer_result = write(&record, &record_encoding).unwrap();
            let expected_writer_result = hex::decode("9e9ed49d01").unwrap();
            assert_eq!(expected_writer_result, writer_result);
        }

        {
            let record = Record {
                a: num_bigint::BigInt::from(3000).into()
            };
            let writer_result = write(&record, &record_encoding).unwrap();
            let expected_writer_result = hex::decode("b82e").unwrap();
            assert_eq!(expected_writer_result, writer_result);
        }
    }

    #[test]
    fn can_serialize_z_negative_to_binary() {
        #[derive(Serialize, Debug)]
        struct Record {
            a: BigInt
        }
        let record_schema = vec![
            Field::new("a", Encoding::Z)
        ];
        let record_encoding = Encoding::Obj(record_schema);

        let record = Record {
            a: num_bigint::BigInt::from(-100_000).into()
        };
        let writer_result = write(&record, &record_encoding).unwrap();
        let expected_writer_result = hex::decode("e09a0c").unwrap();
        assert_eq!(expected_writer_result, writer_result);
    }

    #[test]
    fn can_serialize_z_small_number_to_binary() {
        #[derive(Serialize, Debug)]
        struct Record {
            a: BigInt
        }
        let record_schema = vec![
            Field::new("a", Encoding::Z)
        ];
        let record_encoding = Encoding::Obj(record_schema);

        let record = Record {
            a: num_bigint::BigInt::from(63).into()
        };
        let writer_result = write(&record, &record_encoding).unwrap();
        let expected_writer_result = hex::decode("3f").unwrap();
        assert_eq!(expected_writer_result, writer_result);
    }

    #[test]
    fn can_serialize_z_negative_small_number_to_binary() {
        #[derive(Serialize, Debug)]
        struct Record {
            a: BigInt
        }
        let record_schema = vec![
            Field::new("a", Encoding::Z)
        ];
        let record_encoding = Encoding::Obj(record_schema);

        let record = Record {
            a: num_bigint::BigInt::from(-23).into()
        };
        let writer_result = write(&record, &record_encoding).unwrap();
        let expected_writer_result = hex::decode("57").unwrap();
        assert_eq!(expected_writer_result, writer_result);
    }

    #[test]
    fn can_serialize_tag_to_binary() {
        #[derive(Serialize, Debug)]
        struct GetHeadRecord {
            chain_id: Vec<u8>,
        }

        let get_head_record_schema = vec![
            Field::new("chain_id", Encoding::Sized(4, Box::new(Encoding::Bytes)))
        ];

        #[derive(Serialize, Debug)]
        enum Message {
            GetHead(GetHeadRecord)
        }

        #[derive(Serialize, Debug)]
        struct Response {
            messages: Vec<Message>,
        }

        let response_schema = vec![
            Field::new("messages", Encoding::dynamic(Encoding::list(
                Encoding::Tags(
                    size_of::<u16>(),
                    TagMap::new(&[Tag::new(0x10, "GetHead", Encoding::Obj(get_head_record_schema))]),
                )
            )))
        ];
        let response_encoding = Encoding::Obj(response_schema);

        let response = Response {
            messages: vec![Message::GetHead(GetHeadRecord { chain_id: hex::decode("8eceda2f").unwrap() })]
        };

        let writer_result = write(&response, &response_encoding);
        if let Err(e) = writer_result {
            panic!("Writer error: {:?}", e);
        }

        let expected_writer_result = hex::decode("0000000600108eceda2f").expect("Failed to decode");
        assert_eq!(expected_writer_result, writer_result.unwrap());
    }

    #[test]
    fn can_serialize_complex_schema_to_binary() {
        #[derive(Serialize, Debug)]
        #[allow(dead_code)]
        enum EnumType {
            Accepted,
            Running,
            Disconnected,
        }

        #[derive(Serialize, Debug)]
        struct Version {
            name: String,
            major: u16,
            minor: u16,
        }

        #[derive(Serialize, Debug)]
        struct SubRecord {
            x: i32,
            y: i32,
            v: Vec<i32>,
        }

        #[derive(Serialize, Debug)]
        struct Record {
            a: i32,
            b: bool,
            c: Option<BigInt>,
            d: f64,
            e: EnumType,
            f: Vec<Version>,
            s: SubRecord,
        }

        let record = Record {
            a: 32,
            b: true,
            c: Some(num_bigint::BigInt::from(1_548_569_249).into()),
            d: 12.34,
            e: EnumType::Disconnected,
            f: vec![Version { name: "A".to_string(), major: 1, minor: 1 }, Version { name: "B".to_string(), major: 2, minor: 0 }],
            s: SubRecord {
                x: 5,
                y: 32,
                v: vec![12, 34],
            },
        };

        let version_schema = vec![
            Field::new("name", Encoding::String),
            Field::new("major", Encoding::Uint16),
            Field::new("minor", Encoding::Uint16)
        ];

        let sub_record_schema = vec![
            Field::new("x", Encoding::Int31),
            Field::new("y", Encoding::Int31),
            Field::new("v", Encoding::dynamic(Encoding::list(Encoding::Int31)))
        ];

        let record_schema = vec![
            Field::new("a", Encoding::Int31),
            Field::new("b", Encoding::Bool),
            Field::new("s", Encoding::Obj(sub_record_schema)),
            Field::new("c", Encoding::Option(Box::new(Encoding::Z))),
            Field::new("d", Encoding::Float),
            Field::new("e", Encoding::Enum),
            Field::new("f", Encoding::dynamic(Encoding::list(Encoding::Obj(version_schema))))
        ];
        let record_encoding = Encoding::Obj(record_schema);

        let writer_result = write(&record, &record_encoding);
        assert!(writer_result.is_ok());

        let expected_writer_result = hex::decode("00000020ff0000000500000020000000080000000c0000002201a1aaeac40b4028ae147ae147ae0200000012000000014100010001000000014200020000").expect("Failed to decode");
        assert_eq!(expected_writer_result, writer_result.unwrap());
    }

    #[test]
    fn can_serialize_connection_message() {
        #[derive(Serialize, Debug)]
        struct Version {
            name: String,
            major: u16,
            minor: u16,
        }

        #[derive(Serialize, Debug)]
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
            Field::new("minor", Encoding::Uint16)
        ];

        let connection_message_schema = vec![
            Field::new("port", Encoding::Uint16),
            Field::new("public_key", Encoding::sized(32, Encoding::Bytes)),
            Field::new("proof_of_work_stamp", Encoding::sized(24, Encoding::Bytes)),
            Field::new("message_nonce", Encoding::sized(24, Encoding::Bytes)),
            Field::new("versions", Encoding::list(Encoding::Obj(version_schema)))
        ];
        let connection_message_encoding = Encoding::Obj(connection_message_schema);

        let connection_message = ConnectionMessage {
            port: 3001,
            versions: vec![Version { name: "A".to_string(), major: 1, minor: 1 }, Version { name: "B".to_string(), major: 2, minor: 0 }],
            public_key: hex::decode("eaef40186db19fd6f56ed5b1af57f9d9c8a1eed85c29f8e4daaa7367869c0f0b").unwrap(),
            proof_of_work_stamp: hex::decode("000000000000000000000000000000000000000000000000").unwrap(),
            message_nonce: hex::decode("000000000000000000000000000000000000000000000000").unwrap(),
        };

        let writer_result = write(&connection_message, &connection_message_encoding).expect("Writer failed");

        let expected_writer_result = hex::decode("0bb9eaef40186db19fd6f56ed5b1af57f9d9c8a1eed85c29f8e4daaa7367869c0f0b000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000014100010001000000014200020000").expect("Failed to decode");
        assert_eq!(expected_writer_result, writer_result);
    }

    #[test]
    fn can_serialize_option_some() {
        #[derive(Serialize, Debug)]
        struct Record {
            pub forking_block_hash: Vec<u8>,
        }

        let record_schema = vec![
            Field::new("forking_block_hash", Encoding::list(Encoding::Uint8)),
        ];
        let record_encoding = Encoding::Obj(record_schema);

        let record = Some(
            Record {
                forking_block_hash: hex::decode("2253698f0c94788689fb95ca35eb1535ec3a8b7c613a97e6683f8007d7959e4b").unwrap(),
            }
        );
        let writer_result = write(&record, &Encoding::option(record_encoding)).unwrap();
        let expected_writer_result = hex::decode("012253698f0c94788689fb95ca35eb1535ec3a8b7c613a97e6683f8007d7959e4b").unwrap();
        assert_eq!(expected_writer_result, writer_result);
    }

    #[test]
    fn can_serialize_option_none() {
        #[derive(Serialize, Debug)]
        struct Record {
            pub forking_block_hash: Vec<u8>,
        }

        let record_schema = vec![
            Field::new("forking_block_hash", Encoding::list(Encoding::Uint8)),
        ];
        let record_encoding = Encoding::Obj(record_schema);

        let record: Option<Record> = None;
        let writer_result = write(&record, &Encoding::option(record_encoding)).unwrap();
        let expected_writer_result = hex::decode("00").unwrap();
        assert_eq!(expected_writer_result, writer_result);
    }

    #[test]
    fn can_serialize_optional_field_some() {
        #[derive(Serialize, Debug)]
        struct Record {
            pub arg: Option<String>,
        }

        let record_schema = vec![
            Field::new("arg", Encoding::option_field(Encoding::String)),
        ];
        let record_encoding = Encoding::Obj(record_schema);

        let record = Record {
            arg: Some("arg".to_string()),
        };
        let writer_result = write(&record, &record_encoding).unwrap();
        let expected_writer_result = hex::decode("ff00000003617267").unwrap();
        assert_eq!(expected_writer_result, writer_result);
    }

    #[test]
    fn can_serialize_optional_field_none() {
        #[derive(Serialize, Debug)]
        struct Record {
            pub arg: Option<String>,
        }

        let record_schema = vec![
            Field::new("arg", Encoding::option_field(Encoding::String)),
        ];
        let record_encoding = Encoding::Obj(record_schema);

        let record = Record {
            arg: None,
        };
        let writer_result = write(&record, &record_encoding).unwrap();
        let expected_writer_result = hex::decode("00").unwrap();
        assert_eq!(expected_writer_result, writer_result);
    }
}

