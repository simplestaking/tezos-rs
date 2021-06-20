// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::fmt::{Binary, Debug, Display, LowerHex, UpperHex};
use std::mem::size_of;
use std::ops::{BitAnd, BitAndAssign, BitOrAssign, Not, Shl, ShlAssign, Shr, ShrAssign};

use bit_vec::BitVec;
use thiserror::Error;

/// An error triggered when working with [Bits].
#[derive(Debug, Error, PartialEq)]
pub enum BitsError {
    /// Index is out of range
    #[error("index out of range")]
    IndexOutOfRange,
}

/// A trait for types that can be used as direct storage of bits.
///
/// This trait must only be implemented on unsigned integer primitives.
///
/// The dependency on `Sealed`, a crate-private trait, ensures that this trait
/// can only ever be implemented locally, and no downstream crates are able to
/// implement it on new types.
pub trait Bits:
    Binary
    + BitAnd<Self, Output=Self>
    + BitAndAssign<Self>
    + BitOrAssign<Self>
    //  Permit indexing into a generic array
    + Copy
    + Debug
    + Display
    //  Permit testing a value against 1 in `get()`.
    + Eq
    //  Rust treats numeric literals in code as vaguely typed and does not make
    //  them concrete until long after trait expansion, so this enables building
    //  a concrete Self value from a numeric literal.
    + From<u8>
    + LowerHex
    + Not<Output=Self>
    + Shl<u8, Output=Self>
    + ShlAssign<u8>
    + Shr<u8, Output=Self>
    + ShrAssign<u8>
    //  Allow direct access to a concrete implementor type.
    + Sized
    + UpperHex
{
    /// The width in bits of this type.
    const WIDTH: u8 = size_of::<Self>() as u8 * 8;

    /// The number of bits required to *index* the type. This is always
    /// log<sub>2</sub> of the type width.
    ///
    /// Incidentally, this can be computed as `size_of().trailing_zeros()` once
    /// that becomes a valid constexpr.
    const BITS: u8; // = size_of::<Self>().trailing_zeros() as u8;

    /// The bitmask to turn an arbitrary usize into the bit index. Bit indices
    /// are always stored in the lowest bits of an index value.
    const MASK: u8 = Self::WIDTH - 1;

    /// The maximum number of this type that can be held in a `BitVec`.
    const MAX_ELT: usize = core::usize::MAX >> Self::BITS;

    /// Set a specific bit in an element to a given value.
    fn set(&mut self, place: u8, value: bool) -> Result<(), BitsError> {
        if place > Self::MASK {
            return Err(BitsError::IndexOutOfRange);
        }
        //  Blank the selected bit
        *self &= !(Self::from(1u8) << place);
        //  Set the selected bit
        *self |= Self::from(value as u8) << place;
        Ok(())
    }

    /// Get a specific bit in an element.
    fn get(&self, place: u8) -> Result<bool, BitsError> {
        if place > Self::MASK {
            return Err(BitsError::IndexOutOfRange);
        }
        //  Shift down so the targeted bit is LSb, then blank all other bits.
        Ok((*self >> place) & Self::from(1) == Self::from(1))
    }

    /// Get a specific bit in an element and resets it.
    fn take(&mut self, place: u8) -> Result<bool, BitsError> {
        if place > Self::MASK {
            return Err(BitsError::IndexOutOfRange);
        }
        let mask: Self = Self::from(1).shl(place);
        let neg_mask = !mask;
        let mask = mask & *self;
        *self &= neg_mask;
        Ok(mask != 0.into())
    }

    /// Rust doesn’t (as far as I know) have a way to render a typename at
    /// runtime, so this constant holds the typename of the primitive for
    /// printing by Debug.
    #[doc(hidden)]
    const TY: &'static str;
}

impl Bits for u8 {
    const BITS: u8 = 3;

    const TY: &'static str = "u8";
}

impl Bits for u16 {
    const BITS: u8 = 4;

    const TY: &'static str = "u16";
}

impl Bits for u32 {
    const BITS: u8 = 5;

    const TY: &'static str = "u32";
}

pub trait BitReverse {
    fn reverse(&self) -> Self;
}

impl BitReverse for BitVec {
    #[inline]
    fn reverse(&self) -> BitVec {
        let mut reversed = BitVec::new();
        for bit in self.iter().rev() {
            reversed.push(bit)
        }
        reversed
    }
}

pub trait BitTrim {
    fn trim_left(&self) -> Self;
}

impl BitTrim for BitVec {
    fn trim_left(&self) -> BitVec {
        let mut trimmed: BitVec = BitVec::new();

        let mut notrim = false;
        for bit in self.iter() {
            if bit {
                trimmed.push(bit);
                notrim = true;
            } else if notrim {
                trimmed.push(bit);
            }
        }
        trimmed
    }
}

pub trait ToBytes {
    fn to_byte_vec(&self) -> Vec<u8>;
}

impl ToBytes for BitVec {
    fn to_byte_vec(&self) -> Vec<u8> {
        let mut bytes = vec![];
        let mut byte = 0;
        let mut offset = 0;
        for (idx_bit, bit) in self.iter().rev().enumerate() {
            let idx_byte = (idx_bit % 8) as u8;
            byte.set(idx_byte, bit)
                .unwrap_or_else(|_| unreachable!("Byte should have 8-bit width"));
            if idx_byte == 7 {
                bytes.push(byte);
                byte = 0;
            }
            offset = idx_byte;
        }
        if offset != 7 {
            bytes.push(byte);
        }
        bytes.reverse();
        bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reverse() {
        let mut bits = BitVec::new();
        bits.push(false); // 0
        bits.push(false); // 1
        bits.push(false); // 2
        bits.push(true); // 3
        bits.push(true); // 4
        bits.push(true); // 5
        bits.push(false); // 6
        bits.push(true); // 7
        bits.push(false); // 8
        assert_eq!(vec![0, 184], bits.reverse().to_byte_vec());
    }

    #[test]
    fn trim_left() {
        let mut bits = BitVec::new();
        bits.push(false); // 0
        bits.push(false); // 1
        bits.push(false); // 2
        bits.push(true); // 3
        bits.push(true); // 4
        bits.push(true); // 5
        bits.push(false); // 6
        bits.push(true); // 7
        assert_eq!(vec![29], bits.trim_left().to_byte_vec());
    }

    #[test]
    fn to_byte_vec() {
        let mut bits = BitVec::new();
        bits.push(true); // 0
        bits.push(false); // 1
        bits.push(false); // 2
        bits.push(true); // 3
        bits.push(true); // 4
        bits.push(true); // 5
        bits.push(false); // 6
        bits.push(true); // 7
        assert_eq!(vec![157], bits.to_byte_vec());

        bits.push(false); // 8
        assert_eq!(vec![1, 58], bits.to_byte_vec());
    }

    #[test]
    fn set_out_of_range() {
        let (mut b8, mut b16, mut b32) = (0_u8, 0_u16, 0_u32);
        assert!(matches!(b8.set(8, true), Err(BitsError::IndexOutOfRange)));
        assert!(matches!(b16.set(16, true), Err(BitsError::IndexOutOfRange)));
        assert!(matches!(b32.set(32, true), Err(BitsError::IndexOutOfRange)));
    }

    #[test]
    fn get_out_of_range() {
        let (b8, b16, b32) = (0_u8, 0_u16, 0_u32);
        assert!(matches!(b8.get(8), Err(BitsError::IndexOutOfRange)));
        assert!(matches!(b16.get(16), Err(BitsError::IndexOutOfRange)));
        assert!(matches!(b32.get(32), Err(BitsError::IndexOutOfRange)));
    }

    #[test]
    fn test_take() {
        let mut b8 = 0u8;
        b8.set(4, true).unwrap();
        assert!(matches!(b8.take(4), Ok(true)));
        assert!(matches!(b8.get(4), Ok(false)));

        let mut b8 = 0xffu8;
        b8.set(4, false).unwrap();
        assert!(matches!(b8.take(4), Ok(false)));
        assert!(matches!(b8.get(4), Ok(false)));
        assert_eq!(b8, 0xefu8);
    }
}
