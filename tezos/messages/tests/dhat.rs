// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::{fs::File, io::Read, path::PathBuf};

use anyhow::{Context, Error};
use tezos_encoding::binary_reader::BinaryReaderError;
use tezos_messages::p2p::{
    binary_message::BinaryRead,
    encoding::{
        ack::AckMessage,
        block_header::{BlockHeaderMessage, GetBlockHeadersMessage},
        connection::ConnectionMessage,
        current_head::{CurrentHeadMessage, GetCurrentHeadMessage},
        operations_for_blocks::{GetOperationsForBlocksMessage, OperationsForBlocksMessage},
        prelude::{CurrentBranchMessage, GetCurrentBranchMessage},
    },
};

fn read_data(file: &str) -> Result<Vec<u8>, Error> {
    let dir =
        std::env::var("CARGO_MANIFEST_DIR").context(format!("`CARGO_MANIFEST_DIR` is not set"))?;
    let path = PathBuf::from(dir).join("resources").join(file);
    let data = File::open(&path)
        .and_then(|mut file| {
            let mut data = Vec::new();
            file.read_to_end(&mut data)?;
            Ok(data)
        })
        .with_context(|| format!("Cannot read message from {}", path.to_string_lossy()))?;
    Ok(data)
}

fn read_data_unwrap(file: &str) -> Vec<u8> {
    read_data(file).unwrap_or_else(|e| panic!("Unexpected error: {}", e))
}

trait Decoder<T> {
    fn decode(&self, data: Vec<u8>);
}

impl<T, F> Decoder<T> for F
where
    F: Fn(Vec<u8>) -> Result<T, BinaryReaderError>,
{
    fn decode(&self, data: Vec<u8>) {
        self(data).unwrap_or_else(|e| panic!("Unexpected error: {}", e));
    }
}

fn decode_connection<F: Decoder<ConnectionMessage>>(decoder: F) {
    decoder.decode(hex::decode(CONNECTION_DATA).unwrap());
}

fn decode_ack<F: Decoder<AckMessage>>(decoder: F) {
    decoder.decode(hex::decode(ACK_DATA).unwrap());
}

fn decode_get_current_branch<F: Decoder<GetCurrentBranchMessage>>(decoder: F) {
    let data = read_data_unwrap("get-current-branch.msg");
    decoder.decode(data);
}

fn decode_current_branch<F: Decoder<CurrentBranchMessage>>(decoder: F) {
    let data = read_data_unwrap("current-branch.big.msg");
    decoder.decode(data);
}

fn decode_get_current_head<F: Decoder<GetCurrentHeadMessage>>(decoder: F) {
    let data = read_data_unwrap("get-current-head.msg");
    decoder.decode(data);
}

fn decode_current_head<F: Decoder<CurrentHeadMessage>>(decoder: F) {
    let data = read_data_unwrap("current-head.big.msg");
    decoder.decode(data);
}

fn decode_get_block_headers<F: Decoder<GetBlockHeadersMessage>>(decoder: F) {
    let data = read_data_unwrap("get-block-headers.msg");
    decoder.decode(data);
}

fn decode_block_header<F: Decoder<BlockHeaderMessage>>(decoder: F) {
    let data = hex::decode(BLOCK_HEADER_DATA).unwrap();
    decoder.decode(data);
}

fn decode_get_operations_for_blocks<F: Decoder<GetOperationsForBlocksMessage>>(decoder: F) {
    let data = read_data_unwrap("get-operations-for-blocks.msg");
    decoder.decode(data);
}

fn decode_operations_for_blocks<F: Decoder<OperationsForBlocksMessage>>(decoder: F) {
    let data = read_data_unwrap("operations-for-blocks.huge.msg");
    decoder.decode(data);
}

#[test]
fn all_decode() {
    decode_connection(ConnectionMessage::from_bytes);
    decode_ack(AckMessage::from_bytes);
    decode_get_current_branch(GetCurrentBranchMessage::from_bytes);
    decode_current_branch(CurrentBranchMessage::from_bytes);
    decode_get_current_head(GetCurrentHeadMessage::from_bytes);
    decode_current_head(CurrentHeadMessage::from_bytes);
    decode_get_block_headers(GetBlockHeadersMessage::from_bytes);
    decode_block_header(BlockHeaderMessage::from_bytes);
    decode_get_operations_for_blocks(GetOperationsForBlocksMessage::from_bytes);
    decode_operations_for_blocks(OperationsForBlocksMessage::from_bytes);
}

const CONNECTION_DATA: &str = "\
260470387c4cbd115833626a88a9f2b9bbadd70705f5ad059f494655ac823620c6410ec0d9b1716a\
d6d59d0cc551c96992c8031df8c77735d21158dc510aabbca27bd5fff7f085912327012a4d91e587\
e3bc0000000d54455a4f535f4d41494e4e455400000001\
";

const ACK_DATA: &str = "\
01000100000193000000133231332E3233392E3230312E37393A393733320000001237352E313535\
2E36352E3137313A393733320000001334362E3234352E3137392E3136323A393733320000001235\
302E32312E3136372E3231373A393733320000001331382E3138352E3136322E3134343A39373332\
000000133230392E3135392E3134372E37393A39373332000000133130372E3135312E3139302E31\
313A39373332000000123130372E3135312E3139302E363A393733320000001337382E3133372E32\
31352E3133363A393733330000001133352E3138312E33392E31343A39373332000000133130372E\
3135312E3139302E31363A393733320000001231342E3139322E3230392E38343A39373332000000\
133131392E3134372E3231322E31313A39373332000000113231322E34302E34372E31343A393733\
32000000123130372E3135312E3139302E393A393733320000001133342E39352E35372E3134393A\
39373332000000133133382E3230312E37342E3137393A39373332000000133230332E3234332E32\
392E3133373A39373332\
";

const BLOCK_HEADER_DATA: &str = "\
00094F1F048D51777EF01C0106A09F747615CC72271A46EA75E097B48C7200CA2F1EAE6617000000\
005D7F495004C8626895CC82299089F495F7AD8864D1D3B0F364D497F1D175296B5F4A901EC80000\
001100000001000000000800000000012631B27A9F0E1DA2D2CA10202938298CFB1133D5F9A642F8\
1E6697342263B6ECB621F10000000000032DB85C0E00961D14664ECBDF10CBE4DE7DD71096A4E1A1\
77DB0890B13F0AB85999EB0D715E807BCA0438D3CEAA5C58560D60767F28A9E16326657FBE7FC841\
4FDE3C54A504\
";
