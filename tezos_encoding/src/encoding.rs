use std::fmt;
use std::rc::Rc;
use std::collections::HashMap;

use super::hash::HashEncoding;

#[derive(Debug, Clone)]
pub struct Field {
    name: String,
    encoding: Encoding,
}

impl Field {
    pub fn new(name: &str, encoding: Encoding) -> Field {
        Field { name: String::from(name), encoding }
    }

    pub fn get_name(&self) -> &String {
        &self.name
    }

    pub fn get_encoding(&self) -> &Encoding {
        &self.encoding
    }
}

pub type Schema = Vec<Field>;

#[derive(Debug, Clone)]
pub struct Tag {
    id: u16,
    variant: String,
    encoding: Encoding,
}

impl Tag {
    pub fn new(id: u16, variant: &str, encoding: Encoding) -> Tag {
        Tag { id, variant: String::from(variant), encoding }
    }

    pub fn get_id(&self) -> u16 {
        self.id
    }

    pub fn get_encoding(&self) -> &Encoding {
        &self.encoding
    }

    pub fn get_variant(&self) -> &String {
        &self.variant
    }
}

#[derive(Debug, Clone)]
pub struct TagMap {
    id_to_tag: HashMap<u16, Tag>,
    variant_to_tag: HashMap<String, Tag>,
}

impl TagMap {
    pub fn new(tags: &[Tag]) -> TagMap {
        let mut id_to_tag = HashMap::new();
        let mut variant_to_tag = HashMap::new();

        for tag in tags {
            let prev_item = id_to_tag.insert(tag.get_id(), tag.clone());
            assert!(prev_item.is_none(), "Tag id: 0x{:X} is already present in TagMap", tag.get_id());
            variant_to_tag.insert(tag.get_variant().to_string(), tag.clone());
        }

        TagMap { id_to_tag, variant_to_tag }
    }

    pub fn find_by_id(&self, id: u16) -> Option<&Tag> {
        self.id_to_tag.get(&id)
    }

    pub fn find_by_variant(&self, variant: &str) -> Option<&Tag> {
        self.variant_to_tag.get(variant)
    }
}

pub enum SchemaType {
    Json, Binary
}

pub trait SplitEncodingFn: Fn(SchemaType) -> Encoding { }
impl<F> SplitEncodingFn for F where F: Fn(SchemaType) -> Encoding { }
impl fmt::Debug for dyn SplitEncodingFn<Output = Encoding> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Fn(SchemaType) -> Encoding")
    }
}


pub trait RecursiveEncodingFn: Fn() -> Encoding { }
impl<F> RecursiveEncodingFn for F where F: Fn() -> Encoding { }
impl fmt::Debug for dyn RecursiveEncodingFn<Output = Encoding> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Fn() -> Encoding")
    }
}


#[derive(Debug, Clone)]
pub enum Encoding {
    /// Encoded as nothing in binary or null in json
    Unit,
    /// Signed 8 bit integer (data is encoded as a byte in binary and an integer in JSON).
    Int8,
    /// Unsigned 8 bit integer (data is encoded as a byte in binary and an integer in JSON).
    Uint8,
    /// Signed 16 bit integer (data is encoded as a short in binary and an integer in JSON).
    Int16,
    /// Unsigned 16 bit integer (data is encoded as a short in binary and an integer in JSON).
    Uint16,
    /// Signed 31 bit integer, which corresponds to type int on 32-bit OCaml systems (data is encoded as a 32 bit int in binary and an integer in JSON).
    Int31,
    /// Signed 32 bit integer (data is encoded as a 32-bit int in binary and an integer in JSON).
    Int32,
    /// Unsigned 32 bit integer (data is encoded as a 32-bit int in binary and an integer in JSON).
    Uint32,
    /// Signed 64 bit integer (data is encoded as a 64-bit int in binary and a decimal string in JSON).
    Int64,
    /// Integer with bounds in a given range. Both bounds are inclusive.
    RangedInt,
    ///  Big number
    ///  In JSON, data is encoded as a decimal string.
    ///  In binary, data is encoded as a variable length sequence of
    ///  bytes, with a running unary size bit: the most significant bit of
    ///  each byte tells is this is the last byte in the sequence (0) or if
    /// there is more to read (1). The second most significant bit of the
    /// first byte is reserved for the sign (positive if zero). Binary_size and
    /// sign bits ignored, data is then the binary representation of the
    /// absolute value of the number in little-endian order.
    Z,
    /// Encoding of floating point number (encoded as a floating point number in JSON and a double in binary).
    Float,
    /// Float with bounds in a given range. Both bounds are inclusive.
    RangedFloat,
    /// Encoding of a boolean (data is encoded as a byte in binary and a boolean in JSON).
    Bool,
    /// Encoding of a string
    /// - encoded as a byte sequence in binary prefixed by the length
    /// of the string
    /// - encoded as a string in JSON.
    String,
    /// Encoding of arbitrary sized bytes (encoded via hex in JSON and directly as a sequence byte in binary).
    Bytes,
    /// Tag is prefixed by tag id and followed by encoded bytes
    /// First argument represents size of the tag marker in bytes.
    Tags(usize, TagMap),
    /// List combinator. It's behavior is similar to [Encoding::Greedy] encoding. Main distinction
    /// is that we are expecting list of items instead of a single item to be contained in binary data.
    /// - encoded as an array in JSON
    /// - encoded as the concatenation of all the element in binary
    List(Box<Encoding>),
    /// Encode enumeration via association list
    ///  - represented as a string in JSON and
    ///  - represented as an integer representing the element's position in the list in binary. The integer size depends on the list size.
    Enum,
    /// Combinator to make an optional value
    /// (represented as a 1-byte tag followed by the data (or nothing) in binary
    ///  and either the raw value or an empty object in JSON).
    Option(Box<Encoding>),
    /// Is the collection of fields.
    /// not prefixed by anything in binary, encoded as the concatenation of all the element in binary
    Obj(Schema),
    /// Is the collection of fields.
    /// prefixed its length in bytes, encoded as the concatenation of all the element in binary
    Dynamic(Box<Encoding>),
    /// Represents fixed size block in binary encoding.
    Sized(usize, Box<Encoding>),
    /// Almost same as [Encoding::Dynamic] but without bytes size information prefix.
    /// It assumes that encoding passed as argument will process rest of the available data.
    Greedy(Box<Encoding>),
    /// Decode various types of hashes. Hash has it's own predefined length and prefix.
    /// This is controller by a hash implementation.
    Hash(HashEncoding),
    /// Provides different encoding based on target data type.
    Split(Rc<dyn SplitEncodingFn<Output = Encoding>>),
    /// Timestamp encoding.
    /// - encoded as RFC 3339 in json
    /// - encoded as [Encoding::Int64] in binary
    Timestamp,
    /// This is used to handle recursive encodings needed to encode tree structure.
    /// Encoding itself produces no output in binary or json.
    Lazy(Rc<dyn RecursiveEncodingFn<Output = Encoding>>)
}

impl Encoding {

    pub fn try_unwrap_option_encoding(&self) -> &Encoding {
        match self {
            Encoding::Option(encoding) => encoding,
            _ => panic!("This function can be called only on Encoding::Option or Encoding::List but it was called on {:?}", self)
        }
    }

    /// Utility function to construct [Encoding::List] without the need
    /// to manually create new [Box].
    pub fn list(encoding: Encoding) -> Encoding {
        Encoding::List(Box::new(encoding))
    }

    /// Utility function to construct [Encoding::Sized] without the need
    /// to manually create new [Box].
    pub fn sized(bytes_sz: usize, encoding: Encoding) -> Encoding {
        Encoding::Sized(bytes_sz, Box::new(encoding))
    }

    /// Utility function to construct [Encoding::Greedy] without the need
    /// to manually create new [Box].
    pub fn greedy(encoding: Encoding) -> Encoding {
        Encoding::Greedy(Box::new(encoding))
    }

    /// Utility function to construct [Encoding::Dynamic] without the need
    /// to manually create new [Box].
    pub fn dynamic(encoding: Encoding) -> Encoding {
        Encoding::Dynamic(Box::new(encoding))
    }

    /// Utility function to construct [Encoding::Option] without the need
    /// to manually create new [Box].
    pub fn option(encoding: Encoding) -> Encoding {
        Encoding::Option(Box::new(encoding))
    }
}

/// Indicates that type has it's own ser/de schema.
pub trait HasEncoding {
    fn encoding() -> Encoding;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_split() {
        let split_encoding = Encoding::Split(Rc::new(|schema_type| {
            match schema_type {
                SchemaType::Json => Encoding::Uint16,
                SchemaType::Binary => Encoding::Float
            }
        }));

        if let Encoding::Split(inner_encoding) = split_encoding {
            match inner_encoding(SchemaType::Json) {
                Encoding::Uint16 => { }
                _ => panic!("Was expecting Encoding::Uint16")
            }
            match inner_encoding(SchemaType::Binary) {
                Encoding::Float => { }
                _ => panic!("Was expecting Encoding::Float")
            }
        } else {
            panic!("Was expecting Encoding::Split");
        }
    }

}