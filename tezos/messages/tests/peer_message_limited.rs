// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use anyhow::Error;
use crypto::hash::HashType;
use std::{
    borrow::Cow,
    cmp::{max, min},
    collections::HashMap,
    fmt,
    path::PathBuf,
    rc::Rc,
};
use tezos_encoding::encoding::{Encoding, Field};
use tezos_messages::p2p::encoding::{
    connection::ConnectionMessage, metadata::MetadataMessage, peer::PeerMessageResponse,
};

#[derive(PartialEq, Eq, Clone, Copy)]
enum Limit {
    Fixed(usize),
    UpTo(usize),
    Var,
}

impl Limit {
    fn is_limited(&self) -> bool {
        match self {
            Limit::Var => false,
            _ => true,
        }
    }

    fn union(self, other: Self) -> Self {
        match (self, other) {
            (Limit::Fixed(a), Limit::Fixed(b)) => Limit::Fixed(max(a, b)),
            (Limit::Fixed(a), Limit::UpTo(b))
            | (Limit::UpTo(a), Limit::Fixed(b))
            | (Limit::UpTo(a), Limit::UpTo(b)) => Limit::UpTo(max(a, b)),
            _ => Limit::Var,
        }
    }

    fn restrict(self, max: usize) -> Self {
        match self {
            Limit::Fixed(a) => {
                assert!(a <= max, "cannot restrict fixed size {} to {}", a, max);
                self
            }
            Limit::UpTo(a) => Limit::UpTo(min(a, max)),
            Limit::Var => Limit::UpTo(max),
        }
    }
}

impl fmt::Display for Limit {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Limit::Fixed(size) => write!(f, "{}", size),
            Limit::UpTo(size) => write!(f, "up to {}", size),
            Limit::Var => write!(f, "variable"),
        }
    }
}

impl Default for Limit {
    fn default() -> Self {
        0.into()
    }
}

use std::ops;

impl ops::Add for Limit {
    type Output = Limit;
    fn add(self, other: Self) -> Self {
        match (self, other) {
            (Limit::Fixed(a), Limit::Fixed(b)) => Limit::Fixed(a + b),
            (Limit::Fixed(a), Limit::UpTo(b))
            | (Limit::UpTo(a), Limit::Fixed(b))
            | (Limit::UpTo(a), Limit::UpTo(b)) => Limit::UpTo(a + b),
            _ => Limit::Var,
        }
    }
}

impl ops::Add<usize> for Limit {
    type Output = Limit;
    fn add(self, other: usize) -> Self {
        match self {
            Limit::Fixed(a) => Limit::Fixed(a + other),
            Limit::UpTo(a) => Limit::UpTo(a + other),
            _ => Limit::Var,
        }
    }
}

impl ops::AddAssign<usize> for Limit {
    fn add_assign(&mut self, other: usize) {
        match self {
            Limit::Fixed(a) | Limit::UpTo(a) => *a += other,
            _ => (),
        }
    }
}

impl ops::Mul for Limit {
    type Output = Limit;
    fn mul(self, other: Self) -> Self {
        match (self, other) {
            (Limit::Fixed(a), Limit::Fixed(b)) => Limit::Fixed(a * b),
            (Limit::Fixed(a), Limit::UpTo(b))
            | (Limit::UpTo(a), Limit::Fixed(b))
            | (Limit::UpTo(a), Limit::UpTo(b)) => Limit::UpTo(a * b),
            _ => Limit::Var,
        }
    }
}

impl ops::Mul<usize> for Limit {
    type Output = Limit;
    fn mul(self, other: usize) -> Self {
        match self {
            Limit::Fixed(a) => Limit::Fixed(a * other),
            Limit::UpTo(a) => Limit::UpTo(a * other),
            _ => Limit::Var,
        }
    }
}

impl ops::Mul<&usize> for Limit {
    type Output = Limit;
    fn mul(self, other: &usize) -> Self {
        match self {
            Limit::Fixed(a) => Limit::Fixed(a * *other),
            Limit::UpTo(a) => Limit::UpTo(a * *other),
            _ => Limit::Var,
        }
    }
}

impl From<usize> for Limit {
    fn from(source: usize) -> Self {
        Limit::Fixed(source)
    }
}

impl From<&usize> for Limit {
    fn from(source: &usize) -> Self {
        Limit::Fixed(*source)
    }
}

fn get_contents(
    context: &String,
    encoding: &Encoding,
    infos: &mut HashMap<String, Rc<MessageInfo>>,
) -> FieldContents {
    match encoding {
        Encoding::Unit => "empty".into(),
        Encoding::Int8 => "signed byte".into(),
        Encoding::Uint8 => "unsigned byte".into(),
        Encoding::Int16 => "signed 16-bit integer".into(),
        Encoding::Uint16 => "unsigned 16-bit integer".into(),
        Encoding::Int31 => "signed 31-bit integer".into(),
        Encoding::Int32 => "signed 32-bit integer".into(),
        Encoding::Uint32 => "unsigned 32-bit integer".into(),
        Encoding::Int64 => "signed 64-bit integer".into(),
        Encoding::RangedInt => "range of integers".into(),
        Encoding::Z => "Z-encoded big integer".into(),
        Encoding::Mutez => "MuteZ-encoded big integer".into(),
        Encoding::Float => "64-bit float".into(),
        Encoding::RangedFloat => "range of 64-bit floats".into(),
        Encoding::Bool => "boolean".into(),
        Encoding::String => "UTF8-encoded string".into(),
        Encoding::BoundedString(limit) => {
            format!("UTF8-encoded string with {} elements max", limit).into()
        }
        Encoding::Bytes => "fixed-length sequence of bytes".into(),
        Encoding::List(encoding) => match encoding.as_ref() {
            Encoding::Dynamic(_) | Encoding::BoundedDynamic(_, _) => {
                let name = format!("{} items", context);
                let mut info = MessageInfo::new(name.clone());
                add_fields(&mut info, &String::from("item"), encoding, infos);
                let info = Rc::new(info);
                infos.insert(name, info.clone());
                FieldContents::list(Limit::Var, FieldContents::Reference(info))
            }
            _ => FieldContents::list(Limit::Var, get_contents(context, encoding, infos)),
        },
        Encoding::BoundedList(limit, encoding) => match encoding.as_ref() {
            Encoding::Dynamic(_) | Encoding::BoundedDynamic(_, _) => {
                let name = format!("{} items", context);
                let mut info = MessageInfo::new(name.clone());
                add_fields(&mut info, &String::from("list element"), encoding, infos);
                let info = Rc::new(info);
                infos.insert(name, info.clone());
                FieldContents::list(Limit::UpTo(*limit), get_contents(context, encoding, infos))
            }
            _ => FieldContents::list(Limit::UpTo(*limit), get_contents(context, encoding, infos)),
        },
        Encoding::Obj(name, fields) => {
            let info = get_info(name, fields, infos);
            FieldContents::Reference(info)
        }
        Encoding::Hash(hash) => match hash {
            HashType::ChainId => "ChainId",
            HashType::BlockHash => "BlockHash",
            HashType::BlockMetadataHash => "BlockMetadataHash",
            HashType::ProtocolHash => "ProtocolHash",
            HashType::ContextHash => "ContextHash",
            HashType::OperationHash => "OperationHash",
            HashType::OperationListListHash => "OperationListListHash",
            HashType::OperationMetadataHash => "OperationMetadataHash",
            HashType::OperationMetadataListListHash => "OperationMetadataListListHash",
            HashType::CryptoboxPublicKeyHash => "CryptoboxPublicKeyHash",
            HashType::ContractKt1Hash => "ContractKt1Hash",
            HashType::ContractTz1Hash => "ContractTz1Hash",
            HashType::ContractTz2Hash => "ContractTz2Hash",
            HashType::ContractTz3Hash => "ContractTz3Hash",
            HashType::PublicKeyEd25519 => "PublicKeyEd25519",
            HashType::PublicKeySecp256k1 => "PublicKeySecp256k1",
            HashType::PublicKeyP256 => "PublicKeyP256",
        }
        .into(),
        Encoding::Sized(size, encoding) => {
            FieldContents::sized(*size, get_contents(context, encoding, infos))
        }
        Encoding::Bounded(_, encoding) => get_contents(context, encoding, infos),
        Encoding::Timestamp => "timestamp".into(),
        Encoding::Custom(_) => "Merkle tree path encoding".into(),
        _ => todo!(
            "Getting contents description for unhandled encoding: {:?}",
            encoding
        ),
    }
}

fn get_limit(encoding: &Encoding) -> Limit {
    use Encoding::*;
    use Limit::Var;
    match encoding {
        Unit => 0.into(),
        Int8 | Uint8 | Bool => 1.into(),
        Int16 | Uint16 => 2.into(),
        Int31 | Int32 | Uint32 => 4.into(),
        Int64 | RangedInt | Float | Timestamp => 8.into(),
        RangedFloat => 16.into(),
        Z | Mutez => Var,
        Hash(hash) => hash.size().into(),
        String => Var,
        BoundedString(max) => Limit::UpTo(*max),
        Bytes => Var,
        Tags(size, map) => {
            let mut max = Limit::default();
            for tag in map.tags() {
                let size = get_limit(tag.get_encoding());
                max = max.union(size);
            }
            max + *size
        }
        List(_) => Var,
        BoundedList(max, encoding) => {
            let element_size = get_limit(encoding);
            element_size * Limit::UpTo(*max)
        }
        Enum => 1.into(),
        Option(encoding) => get_limit(encoding) + 1,
        OptionalField(encoding) => get_limit(encoding) + 1,
        Obj(_, fields) => {
            let mut sum = 0.into();
            for field in fields {
                let size = get_limit(field.get_encoding());
                sum = sum + size;
            }
            sum
        }
        Dynamic(encoding) => {
            let size = get_limit(encoding);
            size + 4
        }
        BoundedDynamic(max, encoding) => {
            let size = get_limit(encoding);
            size.restrict(*max) + 4
        }
        Sized(fixed_size, encoding) => {
            let _size = get_limit(encoding);
            fixed_size.into()
        }
        Bounded(bounded_size, encoding) => {
            let _size = get_limit(encoding);
            Limit::UpTo(*bounded_size)
        }
        Custom(_) => Limit::UpTo(100), // 3 hashes, three left/right tags, one op tag, 3 * (32 + 1) + 1
        _ => unimplemented!(),
    }
}

use tezos_encoding::encoding::HasEncoding;

#[derive(Default)]
struct MessageInfo {
    name: String,
    size: Limit,
    fields: Vec<MessageField>,
}

impl MessageInfo {
    fn new<'a, T: Into<Cow<'a, str>>>(name: T) -> Self {
        let name = name.into().into_owned();
        Self {
            name,
            ..Default::default()
        }
    }

    fn add_field<'a, T: Into<Cow<'a, str>>>(
        &mut self,
        name: T,
        size: Limit,
        contents: FieldContents,
    ) {
        let contents = contents.into();
        let name = name.into().into_owned();
        self.fields.push(MessageField {
            name,
            size,
            contents,
        });
        self.size = self.size + size;
    }
}

impl std::fmt::Display for MessageInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "
### {}
Size: {}

| Name | Size | Contents |
|:-----|:-----|:---------|",
            self.name, self.size,
        )?;
        self.fields
            .iter()
            .map(|field| {
                writeln!(
                    f,
                    "| {} | {} | {} |",
                    field.name, field.size, field.contents
                )
            })
            .collect::<Result<_, _>>()?;
        Ok(())
    }
}

enum FieldContents {
    Comment(String),
    Reference(Rc<MessageInfo>),
    List(Limit, Box<FieldContents>),
    Sized(usize, Box<FieldContents>),
}

fn info_id(info: &MessageInfo) -> String {
    info.name
        .chars()
        .filter(|ch| ch.is_alphanumeric() || ch.is_whitespace())
        .map(|ch| if ch.is_whitespace() { '-' } else { ch })
        .collect::<String>()
        .to_lowercase()
}

fn info_ref(info: &MessageInfo) -> String {
    let name = &info.name;
    let id = info_id(info);
    format!("[{}](#{})", name, id)
}

impl fmt::Display for FieldContents {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FieldContents::Comment(comment) => write!(f, "{}", comment),
            FieldContents::Reference(msg) => write!(f, "{}", info_ref(msg)),
            FieldContents::List(limit, contents) => match limit {
                Limit::Fixed(limit) => write!(f, "{} of {}", limit, contents),
                Limit::UpTo(limit) => write!(f, "up to {} of {}", limit, contents),
                Limit::Var => write!(f, "list of {}", contents),
            },
            FieldContents::Sized(_, contents) => {
                write!(f, "{}", contents)
            }
        }
    }
}

impl<'a, T: Into<Cow<'a, str>>> From<T> for FieldContents {
    fn from(source: T) -> Self {
        FieldContents::Comment(source.into().into_owned())
    }
}

impl FieldContents {
    fn sized(limit: usize, contents: Self) -> Self {
        Self::Sized(limit, Box::new(contents))
    }

    fn list(limit: Limit, contents: Self) -> Self {
        Self::List(limit, Box::new(contents))
    }
}

struct MessageField {
    name: String,
    size: Limit,
    contents: FieldContents,
}

type MessageInfoList = Vec<Rc<MessageInfo>>;

fn add_fields(
    info: &mut MessageInfo,
    name: &String,
    encoding: &Encoding,
    infos: &mut HashMap<String, Rc<MessageInfo>>,
) {
    match encoding {
        Encoding::Dynamic(encoding) => {
            info.add_field(
                "# of bytes in the next field",
                4.into(),
                "unsigned 32-bit integer".into(),
            );
            add_fields(info, name, encoding, infos);
        }
        Encoding::BoundedDynamic(limit, encoding) => {
            info.add_field(
                format!("# of bytes in the next field (up to {})", limit),
                4.into(),
                "unsigned 32-bit integer".into(),
            );
            add_fields(info, name, encoding, infos);
        }
        Encoding::OptionalField(encoding) => {
            info.add_field(
                "presense of the next field",
                1.into(),
                "0xff if presend, 0x00 if absent ".into(),
            );
            add_fields(info, name, encoding, infos);
        }
        _ => {
            let size = get_limit(encoding);
            let contents = get_contents(&format!("{}.{}", info.name, name), encoding, infos);
            info.add_field(name, size, contents);
        }
    }
}

fn get_obj_info(
    encoding: &Encoding,
    infos: &mut HashMap<String, Rc<MessageInfo>>,
) -> Rc<MessageInfo> {
    match encoding {
        Encoding::Obj(name, fields) => {
            if let Limit::Fixed(limit) | Limit::UpTo(limit) = get_limit(encoding) {
                assert!(
                    limit
                        <= tezos_messages::p2p::binary_message::CONTENT_LENGTH_MAX
                            - crypto::crypto_box::BOX_ZERO_BYTES,
                    "Message {} may not fit in a single chunk",
                    name
                );
            } else {
                panic!("Message {} is not bounded", name);
            }
            get_single_info(name, fields, infos)
        }
        _ => unreachable!(),
    }
}

fn get_single_info(
    name: &str,
    fields: &Vec<Field>,
    infos: &mut HashMap<String, Rc<MessageInfo>>,
) -> Rc<MessageInfo> {
    if let Some(info) = infos.get(name) {
        return info.clone();
    }

    let mut info = MessageInfo::new(name);

    for field in fields {
        add_fields(&mut info, field.get_name(), field.get_encoding(), infos);
    }

    Rc::new(info)
}

fn get_info(
    name: &str,
    fields: &Vec<Field>,
    infos: &mut HashMap<String, Rc<MessageInfo>>,
) -> Rc<MessageInfo> {
    let info = get_single_info(name, fields, infos);
    infos.insert(name.to_owned(), info.clone());
    info
}

fn peer_message_response(infos: &mut HashMap<String, Rc<MessageInfo>>) -> MessageInfoList {
    use Encoding::*;
    let mut pre_size = Limit::default();
    if let BoundedDynamic(total_size, obj_encoding) = PeerMessageResponse::encoding() {
        pre_size += 4; // Dynamic encoding size, 4 bytes
        if let Obj(_, ref fields) = **obj_encoding {
            assert_eq!(fields.len(), 1);
            assert_eq!(fields[0].get_name(), &"message".to_string());
            if let Tags(tag_size, tag_map) = fields[0].get_encoding() {
                let mut res = vec![];
                pre_size += *tag_size; // number of bytes used to encode a tag

                let mut max = Limit::default();
                let mut tags = tag_map
                    .tags()
                    .collect::<Vec<&tezos_encoding::encoding::Tag>>();
                tags.sort_by(|a, b| a.get_id().cmp(&b.get_id()));
                for tag in tags {
                    let tag_id = format!("{:#04X}", tag.get_id());

                    let mut info = MessageInfo::new(format!("P2P {}", tag.get_variant()));
                    info.add_field(
                        "# of bytes in the message",
                        4.into(),
                        "unsigned 32-bit integer".into(),
                    );
                    info.add_field(
                        "tag",
                        (*tag_size).into(),
                        format!("tag corresponding to the message ({})", tag_id).into(),
                    );

                    let tag_path = tag.get_variant();
                    let size = get_limit(tag.get_encoding());
                    assert!(size.is_limited(), "Size for {} should be limited", tag_path);

                    add_fields(&mut info, fields[0].get_name(), tag.get_encoding(), infos);

                    res.push(Rc::new(info));

                    max = max.union(size);
                }

                if let Limit::UpTo(limit) = pre_size + max {
                    assert!(limit <= *total_size, "PeerMessageResponse is limited to {} and cannot contain message of size {}", total_size, limit);
                }

                return res;
            }
        }
    }
    unreachable!("Unexpected encoding for PeerMessageResponse");
}

#[test]
fn peer_message_limited() -> Result<(), Error> {
    use std::io::prelude::*;

    let mut infos = HashMap::new();

    let mut file = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap_or(".".to_string()));
    file.push("Messages.md");
    let mut file = std::fs::File::create(file)?;

    writeln!(file, "# P2P Messages\n")?;
    writeln!(file, "## Handshaking Messages")?;
    [ConnectionMessage::encoding(), MetadataMessage::encoding()]
        .iter()
        .map(|encoding| get_obj_info(encoding, &mut infos))
        .map(|info| writeln!(file, "{}", info))
        .collect::<Result<_, _>>()?;

    writeln!(file, "## Distributed DB Messages")?;
    peer_message_response(&mut infos)
        .iter()
        .map(|info| writeln!(file, "{}", info))
        .collect::<Result<_, _>>()?;

    writeln!(file, "## Auxiliary Types")?;
    let mut infos = infos.into_iter().map(|(_, v)| v).collect::<Vec<_>>();
    infos.sort_by_key(|info| info.name.to_string());
    infos
        .iter()
        .map(|info| writeln!(file, "{}", info))
        .collect::<Result<_, _>>()?;

    Ok(())
}
