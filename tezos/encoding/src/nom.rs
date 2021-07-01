// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use bit_vec::BitVec;
use crypto::hash::HashTrait;
use nom::{
    branch::*,
    bytes::complete::*,
    combinator::*,
    error::ErrorKind,
    multi::*,
    number::{complete::*, Endianness},
    sequence::*,
    Err, InputLength, Parser, Slice,
};
use num_bigint::{BigInt, Sign};
pub use tezos_encoding_derive::NomReader;

use crate::{
    bit_utils::{BitReverse, Bits, BitsError},
    types::{Mutez, Zarith},
};

use self::error::{BoundedEncodingKind, DecodeError, DecodeErrorKind};

pub mod error {
    use std::{fmt::Write, str::Utf8Error};

    use nom::{
        error::{ErrorKind, FromExternalError},
        Offset,
    };

    use crate::bit_utils::BitsError;

    use super::NomInput;

    /// Decoding error
    #[derive(Debug, PartialEq)]
    pub struct DecodeError<I> {
        /// Input causing the error.
        pub(crate) input: I,
        /// Kind of the error.
        pub(crate) kind: DecodeErrorKind,
        /// Subsequent error, if any.
        pub(crate) other: Option<Box<DecodeError<I>>>,
    }

    /// Decoding error kind.
    #[derive(Debug, PartialEq)]
    pub enum DecodeErrorKind {
        /// Nom-specific error.
        Nom(ErrorKind),
        /// Error converting bytes to a UTF-8 string.
        Utf8(ErrorKind, Utf8Error),
        /// Boundary violation.
        Boundary(BoundedEncodingKind),
        /// Bits error
        Bits(BitsError),
        /// Field name
        Field(&'static str),
        /// Field name
        Variant(&'static str),
        /// Unknown/unsupported tag
        UnknownTag(String),
        /// Invalid tag
        InvalidTag(String),
    }

    /// Specific bounded encoding kind.
    #[derive(Debug, PartialEq, Clone)]
    pub enum BoundedEncodingKind {
        String,
        List,
        Dynamic,
        Bounded,
    }

    impl<'a> DecodeError<NomInput<'a>> {
        pub(crate) fn add_field(self, name: &'static str) -> Self {
            Self {
                input: <&[u8]>::clone(&self.input),
                kind: DecodeErrorKind::Field(name),
                other: Some(Box::new(self)),
            }
        }

        pub(crate) fn add_variant(self, name: &'static str) -> Self {
            Self {
                input: <&[u8]>::clone(&self.input),
                kind: DecodeErrorKind::Variant(name),
                other: Some(Box::new(self)),
            }
        }

        pub(crate) fn limit(input: NomInput<'a>, kind: BoundedEncodingKind) -> Self {
            Self {
                input,
                kind: DecodeErrorKind::Boundary(kind),
                other: None,
            }
        }

        pub fn unknown_tag(input: NomInput<'a>, tag: String) -> Self {
            Self {
                input,
                kind: DecodeErrorKind::UnknownTag(tag),
                other: None,
            }
        }

        pub fn invalid_tag(input: NomInput<'a>, tag: String) -> Self {
            Self {
                input,
                kind: DecodeErrorKind::InvalidTag(tag),
                other: None,
            }
        }

        pub fn get_unknown_tag(&self) -> Option<&String> {
            match self.kind {
                DecodeErrorKind::UnknownTag(ref tag) => Some(tag),
                _ => None,
            }
        }
    }

    impl<I> nom::error::ParseError<I> for DecodeError<I> {
        fn from_error_kind(input: I, kind: ErrorKind) -> Self {
            Self {
                input,
                kind: DecodeErrorKind::Nom(kind),
                other: None,
            }
        }

        fn append(input: I, kind: ErrorKind, other: Self) -> Self {
            Self {
                input,
                kind: DecodeErrorKind::Nom(kind),
                other: Some(Box::new(other)),
            }
        }
    }

    impl<I> FromExternalError<I, Utf8Error> for DecodeError<I> {
        fn from_external_error(input: I, kind: ErrorKind, e: Utf8Error) -> Self {
            Self {
                input,
                kind: DecodeErrorKind::Utf8(kind, e),
                other: None,
            }
        }
    }

    pub fn convert_error(input: NomInput, error: DecodeError<NomInput>) -> String {
        let mut res = String::new();
        let start = input.offset(error.input);
        let end = start + error.input.len();
        let _ = write!(res, "Error decoding bytes [{}..{}]", start, end);
        let _ = match error.kind {
            DecodeErrorKind::Nom(kind) => write!(res, " by nom parser `{:?}`", kind),
            DecodeErrorKind::Utf8(kind, e) => write!(res, " by nom parser `{:?}`: {}", kind, e),
            DecodeErrorKind::Boundary(kind) => {
                write!(
                    res,
                    " caused by boundary violation of encoding `{:?}`",
                    kind
                )
            }
            DecodeErrorKind::Field(name) => {
                write!(res, " while decoding field `{}`", name)
            }
            DecodeErrorKind::Variant(name) => {
                write!(res, " while decoding variant `{}`", name)
            }
            DecodeErrorKind::Bits(e) => write!(res, " while performing bits operation: {}", e),
            DecodeErrorKind::UnknownTag(tag) => write!(res, " caused by unsupported tag `{}`", tag),
            DecodeErrorKind::InvalidTag(tag) => write!(res, " caused by invalid tag `{}`", tag),
        };

        if let Some(other) = error.other {
            let _ = write!(res, "\n\nNext error:\n{}", convert_error(input, *other));
        }

        res
    }
}

/// Input for decoding.
pub type NomInput<'a> = &'a [u8];

/// Error type used to parameterize `nom`.
pub type NomError<'a> = error::DecodeError<NomInput<'a>>;

/// Nom result used in Tezedge (`&[u8]` as input, [NomError] as error type).
pub type NomResult<'a, T> = nom::IResult<NomInput<'a>, T, NomError<'a>>;

/// Traits defining message decoding using `nom` primitives.
pub trait NomReader: Sized {
    fn nom_read(bytes: &[u8]) -> NomResult<Self>;
}

macro_rules! hash_nom_reader {
    ($hash_name:ident) => {
        impl NomReader for crypto::hash::$hash_name {
            #[inline(always)]
            fn nom_read(bytes: &[u8]) -> NomResult<Self> {
                map(take(Self::hash_size()), |bytes| {
                    Self::try_from_bytes(bytes).unwrap()
                })(bytes)
            }
        }
    };
}

hash_nom_reader!(ChainId);
hash_nom_reader!(BlockHash);
hash_nom_reader!(BlockMetadataHash);
hash_nom_reader!(OperationHash);
hash_nom_reader!(OperationListListHash);
hash_nom_reader!(OperationMetadataHash);
hash_nom_reader!(OperationMetadataListListHash);
hash_nom_reader!(ContextHash);
hash_nom_reader!(ProtocolHash);
hash_nom_reader!(ContractKt1Hash);
hash_nom_reader!(ContractTz1Hash);
hash_nom_reader!(ContractTz2Hash);
hash_nom_reader!(ContractTz3Hash);
hash_nom_reader!(CryptoboxPublicKeyHash);
hash_nom_reader!(PublicKeyEd25519);
hash_nom_reader!(PublicKeySecp256k1);
hash_nom_reader!(PublicKeyP256);

impl NomReader for Zarith {
    fn nom_read(bytes: &[u8]) -> NomResult<Self> {
        map(zarith, |big_int| big_int.into())(bytes)
    }
}

impl NomReader for Mutez {
    fn nom_read(bytes: &[u8]) -> NomResult<Self> {
        map(mutez, |big_int| big_int.into())(bytes)
    }
}

/// Reads a boolean value.
#[inline(always)]
pub fn boolean(input: NomInput) -> NomResult<bool> {
    alt((
        map(tag(&[crate::types::BYTE_VAL_TRUE][..]), |_| true),
        map(tag(&[crate::types::BYTE_VAL_FALSE][..]), |_| false),
    ))(input)
}

/// Reads all available bytes into a [Vec]. Used in conjunction with [sized].
#[inline(always)]
pub fn bytes(input: NomInput) -> NomResult<Vec<u8>> {
    map(rest, Vec::from)(input)
}

/// Reads size encoded as 4-bytes big-endian unsigned.
#[inline(always)]
pub fn size(input: NomInput) -> NomResult<u32> {
    u32(Endianness::Big)(input)
}

/// Reads size encoded as 4-bytes big-endian unsigned, checking that it does not exceed the `max` value.
#[inline(always)]
fn bounded_size<'a>(
    kind: BoundedEncodingKind,
    max: usize,
) -> impl FnMut(NomInput) -> NomResult<u32> {
    move |input| {
        let i = <&[u8]>::clone(&input);
        let (input, size) = size(input)?;
        if size as usize <= max {
            Ok((input, size))
        } else {
            Err(Err::Error(DecodeError::limit(i, kind.clone())))
        }
    }
}

/// Reads Tesoz string encoded as a 32-bit length followed by the string bytes.
#[inline(always)]
pub fn string(input: NomInput) -> NomResult<String> {
    map_res(complete(length_data(size)), |bytes| {
        std::str::from_utf8(bytes).map(str::to_string)
    })(input)
}

/// Returns parser that reads Tesoz string encoded as a 32-bit length followed by the string bytes,
/// checking that the lengh of the string does not exceed `max`.
#[inline(always)]
pub fn bounded_string<'a>(max: usize) -> impl FnMut(NomInput<'a>) -> NomResult<'a, String> {
    map_res(
        complete(length_data(bounded_size(BoundedEncodingKind::String, max))),
        |bytes| std::str::from_utf8(bytes).map(str::to_string),
    )
}

/// Parser that applies specified parser to the fixed length slice of input.
#[inline(always)]
pub fn sized<'a, O, F>(size: usize, f: F) -> impl FnMut(NomInput<'a>) -> NomResult<'a, O>
where
    F: FnMut(NomInput<'a>) -> NomResult<'a, O>,
{
    map_parser(take(size), f)
}

/// Parses optional field. Byte `0x00` indicates absence of the field,
/// byte `0xff` preceedes encoding of the existing field.
#[inline(always)]
pub fn optional_field<'a, O, F>(parser: F) -> impl FnMut(NomInput<'a>) -> NomResult<'a, Option<O>>
where
    F: FnMut(NomInput<'a>) -> NomResult<'a, O>,
    O: Clone,
{
    alt((
        preceded(tag(0x00u8.to_be_bytes()), success(None)),
        preceded(tag(0xffu8.to_be_bytes()), map(parser, Some)),
    ))
}

/// Parses input by applying parser `f` to it.
#[inline(always)]
pub fn list<'a, O, F>(f: F) -> impl FnMut(NomInput<'a>) -> NomResult<'a, Vec<O>>
where
    F: FnMut(NomInput<'a>) -> NomResult<'a, O>,
    O: Clone,
{
    fold_many0(f, Vec::new(), |mut list, item| {
        list.push(item);
        list
    })
}

/// Parses input by applying parser `f` to it no more than `max` times.
#[inline(always)]
pub fn bounded_list<'a, O, F>(
    max: usize,
    mut f: F,
) -> impl FnMut(NomInput<'a>) -> NomResult<'a, Vec<O>>
where
    F: FnMut(NomInput<'a>) -> NomResult<'a, O>,
    O: Clone,
{
    move |input| {
        let (input, list) = fold_many_m_n(
            0,
            max,
            |i| f.parse(i),
            Vec::new(),
            |mut list, item| {
                list.push(item);
                list
            },
        )(input)?;
        if input.input_len() > 0 {
            Err(Err::Error(DecodeError {
                input,
                kind: DecodeErrorKind::Boundary(BoundedEncodingKind::List),
                other: None,
            }))
        } else {
            Ok((input, list))
        }
    }
}

/// Parses dynamic block by reading 4-bytes size and applying the parser `f` to the following sequence of bytes of that size.
#[inline(always)]
pub fn dynamic<'a, O, F>(f: F) -> impl FnMut(NomInput<'a>) -> NomResult<'a, O>
where
    F: FnMut(NomInput<'a>) -> NomResult<'a, O>,
    O: Clone,
{
    length_value(size, all_consuming(f))
}

/// Parses dynamic block by reading 4-bytes size and applying the parser `f`
/// to the following sequence of bytes of that size. It also checks that the size
/// does not exceed the `max` value.
#[inline(always)]
pub fn bounded_dynamic<'a, O, F>(max: usize, f: F) -> impl FnMut(NomInput<'a>) -> NomResult<'a, O>
where
    F: FnMut(NomInput<'a>) -> NomResult<'a, O>,
    O: Clone,
{
    length_value(
        bounded_size(BoundedEncodingKind::Dynamic, max),
        all_consuming(f),
    )
}

/// Applies the parser `f` to the input, limiting it to `max` bytes at most.
#[inline(always)]
pub fn bounded<'a, O, F>(max: usize, mut f: F) -> impl FnMut(NomInput<'a>) -> NomResult<'a, O>
where
    F: FnMut(NomInput<'a>) -> NomResult<'a, O>,
    O: Clone,
{
    move |input: NomInput| {
        let max = std::cmp::min(max, input.input_len());
        let bounded = input.slice(std::ops::RangeTo { end: max });
        match f.parse(bounded) {
            Ok((rest, parsed)) => Ok((
                input.slice(std::ops::RangeFrom {
                    start: max - rest.input_len(),
                }),
                parsed,
            )),
            Err(Err::Error(DecodeError {
                input,
                kind: error::DecodeErrorKind::Nom(ErrorKind::Eof),
                other,
            })) => Err(Err::Error(DecodeError {
                input,
                kind: error::DecodeErrorKind::Boundary(BoundedEncodingKind::Bounded),
                other,
            })),
            e => e,
        }
    }
}

/// Applies the `parser` to the input, addin field context to the error.
#[inline(always)]
pub fn field<'a, O, F>(
    name: &'static str,
    mut parser: F,
) -> impl FnMut(NomInput<'a>) -> NomResult<'a, O>
where
    F: FnMut(NomInput<'a>) -> NomResult<'a, O>,
{
    move |input| parser(input).map_err(|e| e.map(|e| e.add_field(name)))
}

/// Applies the `parser` to the input, addin enum variant context to the error.
#[inline(always)]
pub fn variant<'a, O, F>(
    name: &'static str,
    mut parser: F,
) -> impl FnMut(NomInput<'a>) -> NomResult<'a, O>
where
    F: FnMut(NomInput<'a>) -> NomResult<'a, O>,
{
    move |input| parser(input).map_err(|e| e.map(|e| e.add_variant(name)))
}

fn map_bits_err(input: NomInput, error: BitsError) -> NomError {
    DecodeError {
        input,
        kind: DecodeErrorKind::Bits(error),
        other: None,
    }
}

pub fn zarith(input: NomInput) -> NomResult<BigInt> {
    let map_err = |e| Err::Error(map_bits_err(<&[u8]>::clone(&input), e));

    let (input, mut first) = u8(input)?;
    let mut has_next = first.take(7).map_err(map_err)?;
    let negative = first.take(6).map_err(map_err)?;

    let (input, big_int) = if !has_next {
        let num = if negative {
            -(first as i8)
        } else {
            first as i8
        };
        (input, num_bigint::BigInt::from(num))
    } else {
        // In Z encoding bit chunks go from least significant to most significant.
        // That means that we should collect bits from least to most significant
        // and then reverse the `BitVec` to get proper BE order.

        let mut bits = BitVec::new();
        for i in 0..6 {
            bits.push(first.get(i).map_err(map_err)?);
        }
        let mut input = input;
        while has_next {
            let i = <&[u8]>::clone(&input);
            let map_err = |e| Err::Error(map_bits_err(i, e));
            let (i, byte) = u8(input)?;
            input = i;
            for i in 0..7 {
                bits.push(byte.get(i).map_err(map_err)?);
            }
            has_next = byte.get(7).map_err(map_err)?;
        }

        // `BitVec::to_bytes` considers the rightmost bit as the 7th bit of the
        // first byte, so it should be padded with zeroes that will become most
        // significant bits after reverse.
        let pad = bits.len() % 8;
        if pad != 0 {
            bits.append(&mut BitVec::from_elem(8 - pad, false));
        }

        let sign = if negative { Sign::Minus } else { Sign::Plus };
        let big_int = num_bigint::BigInt::from_bytes_be(sign, bits.reverse().to_bytes().as_slice());
        (input, big_int)
    };

    Ok((input, big_int))
}

pub fn mutez(mut input: NomInput) -> NomResult<BigInt> {
    let mut bits = BitVec::new();
    let mut has_next = true;
    while has_next {
        let i = <&[u8]>::clone(&input);
        let map_err = |e| Err::Error(map_bits_err(i, e));
        let (i, byte) = u8(input)?;
        input = i;
        for i in 0..7 {
            bits.push(byte.get(i).map_err(map_err)?);
        }
        has_next = byte.get(7).map_err(map_err)?;
    }

    // `BitVec::to_bytes` considers the rightmost bit as the 7th bit of the
    // first byte, so it should be padded with zeroes that will become most
    // significant bits after reverse.
    let pad = bits.len() % 8;
    if pad != 0 {
        bits.append(&mut BitVec::from_elem(8 - pad, false));
    }

    let big_int =
        num_bigint::BigInt::from_bytes_be(Sign::Plus, bits.reverse().to_bytes().as_slice());

    Ok((input, big_int.into()))
}

#[cfg(test)]
mod test {
    use num_bigint::BigInt;
    use num_traits::FromPrimitive;

    use super::error::*;
    use super::*;

    #[test]
    fn test_boolean() {
        let res: NomResult<bool> = boolean(&[0xff]);
        assert_eq!(res, Ok((&[][..], true)));

        let res: NomResult<bool> = boolean(&[0x00]);
        assert_eq!(res, Ok((&[][..], false)));

        let res: NomResult<bool> = boolean(&[0x01]);
        res.expect_err("Error is expected");
    }

    #[test]
    fn test_size() {
        let input = &[0xff, 0xff, 0xff, 0xff];
        let res: NomResult<u32> = size(input);
        assert_eq!(res, Ok((&[][..], 0xffffffff)))
    }

    #[test]
    fn test_bounded_size() {
        let input = &[0x00, 0x00, 0x00, 0x10];

        let res: NomResult<u32> = bounded_size(BoundedEncodingKind::String, 100)(input);
        assert_eq!(res, Ok((&[][..], 0x10)));

        let res: NomResult<u32> = bounded_size(BoundedEncodingKind::String, 0x10)(input);
        assert_eq!(res, Ok((&[][..], 0x10)));

        let res: NomResult<u32> = bounded_size(BoundedEncodingKind::String, 0xf)(input);
        let err = res.expect_err("Error is expected");
        assert_eq!(err, limit_error(input, BoundedEncodingKind::String));
    }

    #[test]
    fn test_bytes() {
        let input = &[0, 1, 2, 3];
        let res: NomResult<Vec<u8>> = bytes(input);
        assert_eq!(res, Ok((&[][..], vec![0, 1, 2, 3])))
    }

    #[test]
    fn test_optional_field() {
        let res: NomResult<Option<u8>> = optional_field(u8)(&[0x00, 0x01][..]);
        assert_eq!(res, Ok((&[0x01][..], None)));

        let res: NomResult<Option<u8>> = optional_field(u8)(&[0xff, 0x01][..]);
        assert_eq!(res, Ok((&[][..], Some(0x01))));

        let res: NomResult<Option<u8>> = optional_field(u8)(&[0x01, 0x01][..]);
        res.expect_err("Error is expected");
    }

    #[test]
    fn test_string() {
        let input = &[0, 0, 0, 3, 0x78, 0x78, 0x78, 0xff];
        let res: NomResult<String> = string(input);
        assert_eq!(res, Ok((&[0xffu8][..], "xxx".to_string())))
    }

    #[test]
    fn test_bounded_string() {
        let input = &[0, 0, 0, 3, 0x78, 0x78, 0x78, 0xff];

        let res: NomResult<String> = bounded_string(3)(input);
        assert_eq!(res, Ok((&[0xffu8][..], "xxx".to_string())));

        let res: NomResult<String> = bounded_string(4)(input);
        assert_eq!(res, Ok((&[0xffu8][..], "xxx".to_string())));

        let res: NomResult<String> = bounded_string(2)(input);
        let err = res.expect_err("Error is expected");
        assert_eq!(err, limit_error(input, BoundedEncodingKind::String));
    }

    #[test]
    fn test_sized_bytes() {
        let input = &[0, 1, 2, 3, 4, 5, 6];
        let res: NomResult<Vec<u8>> = sized(4, bytes)(input);
        assert_eq!(res, Ok((&[4, 5, 6][..], vec![0, 1, 2, 3])))
    }

    #[test]
    fn test_list() {
        let input = &[0, 1, 2, 3, 4, 5];
        let res: NomResult<Vec<u16>> = list(u16(Endianness::Big))(input);
        assert_eq!(res, Ok((&[][..], vec![0x0001, 0x0203, 0x0405])));
    }

    #[test]
    fn test_bounded_list() {
        let input = &[0, 1, 2, 3, 4, 5];

        let res: NomResult<Vec<u16>> = bounded_list(4, u16(Endianness::Big))(input);
        assert_eq!(res, Ok((&[][..], vec![0x0001, 0x0203, 0x0405])));

        let res: NomResult<Vec<u16>> = bounded_list(3, u16(Endianness::Big))(input);
        assert_eq!(res, Ok((&[][..], vec![0x0001, 0x0203, 0x0405])));

        let res: NomResult<Vec<u16>> = bounded_list(2, u16(Endianness::Big))(input);
        let err = res.expect_err("Error is expected");
        assert_eq!(err, limit_error(&input[4..], BoundedEncodingKind::List));
    }

    #[test]
    fn test_dynamic() {
        let input = &[0, 0, 0, 3, 0x78, 0x78, 0x78, 0xff];

        let res: NomResult<Vec<u8>> = dynamic(bytes)(input);
        assert_eq!(res, Ok((&[0xffu8][..], vec![0x78; 3])));

        let res: NomResult<u8> = dynamic(u8)(input);
        res.expect_err("Error is expected");
    }

    #[test]
    fn test_bounded_dynamic() {
        let input = &[0, 0, 0, 3, 0x78, 0x78, 0x78, 0xff];

        let res: NomResult<Vec<u8>> = bounded_dynamic(4, bytes)(input);
        assert_eq!(res, Ok((&[0xffu8][..], vec![0x78; 3])));

        let res: NomResult<Vec<u8>> = bounded_dynamic(3, bytes)(input);
        assert_eq!(res, Ok((&[0xffu8][..], vec![0x78; 3])));

        let res: NomResult<Vec<u8>> = bounded_dynamic(2, bytes)(input);
        let err = res.expect_err("Error is expected");
        assert_eq!(err, limit_error(input, BoundedEncodingKind::Dynamic));
    }

    #[test]
    fn test_bounded() {
        let input = &[1, 2, 3, 4, 5];

        let res: NomResult<Vec<u8>> = bounded(4, bytes)(input);
        assert_eq!(res, Ok((&[5][..], vec![1, 2, 3, 4])));

        let res: NomResult<Vec<u8>> = bounded(3, bytes)(input);
        assert_eq!(res, Ok((&[4, 5][..], vec![1, 2, 3])));

        let res: NomResult<Vec<u8>> = bounded(10, bytes)(input);
        assert_eq!(res, Ok((&[][..], vec![1, 2, 3, 4, 5])));

        let res: NomResult<u32> = bounded(3, u32(Endianness::Big))(input);
        let err = res.expect_err("Error is expected");
        assert_eq!(err, limit_error(&input[..3], BoundedEncodingKind::Bounded));
    }

    #[test]
    fn test_zarith() {
        let input = hex::decode("9e9ed49d01").unwrap();
        let res: NomResult<BigInt> = zarith(&input);
        assert_eq!(res, Ok((&[][..], hex_to_bigint("9da879e"),)));

        let input = hex::decode("41").unwrap();
        let res: NomResult<BigInt> = zarith(&input);
        assert_eq!(res, Ok((&[][..], i64_to_bigint(-1),)));

        let input = hex::decode("57").unwrap();
        let res: NomResult<BigInt> = zarith(&input);
        assert_eq!(res, Ok((&[][..], i64_to_bigint(-23),)));
    }

    #[test]
    fn test_mutez() {
        let input = hex::decode("9e9ed49d01").unwrap();
        let res: NomResult<BigInt> = mutez(&input);
        assert_eq!(res, Ok((&[][..], hex_to_bigint("13b50f1e"),)));
    }

    fn i64_to_bigint(n: i64) -> BigInt {
        num_bigint::BigInt::from_i64(n).unwrap()
    }

    fn hex_to_bigint(s: &str) -> BigInt {
        num_bigint::BigInt::from_i64(i64::from_str_radix(s, 16).unwrap()).unwrap()
    }

    fn limit_error<'a>(input: NomInput<'a>, kind: BoundedEncodingKind) -> Err<NomError<'a>> {
        Err::Error(DecodeError {
            input,
            kind: DecodeErrorKind::Boundary(kind),
            other: None,
        })
    }
}
