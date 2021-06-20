// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use anyhow::Error;
use common::block_header_message_encoded;
use networking::p2p::stream::{EncryptedMessageWriterBase, MessageWriterBase};
use tezos_messages::p2p::{
    binary_message::BinaryChunk,
    encoding::{
        peer::{PeerMessage, PeerMessageResponse},
        swap::SwapMessage,
    },
};
use tezos_messages::p2p::{
    binary_message::{BinaryRead, BinaryWrite},
    encoding::limits::BLOCK_HEADER_MAX_SIZE,
};
use tokio_test::io::Builder;

pub mod common;

use self::common::{data_sample, new_log, CryptoMock, PeerMock};

#[async_std::test]
async fn can_write_chunks() -> Result<(), Error> {
    let chunks = vec![
        data_sample(0, 0x1),
        data_sample(1, 0x1),
        data_sample(16, 0x2),
        data_sample(1024, 0x3),
        data_sample(65519, 0xfe),
        data_sample(65535, 0xff),
    ];

    let mut builder = Builder::new();
    for data in &chunks {
        let chunk = BinaryChunk::from_content(&data)?;
        builder.write(chunk.raw());
    }
    let mock = builder.build();

    let mut writer = MessageWriterBase { stream: mock };
    for data in chunks {
        writer
            .write_message(&BinaryChunk::from_content(&data)?)
            .await?;
    }

    Ok(())
}

#[async_std::test]
async fn can_write_message_swap() -> Result<(), Error> {
    let crypto_mock = CryptoMock::new();

    let crypto_remote = crypto_mock.remote;
    let mut peer_mock = PeerMock::new(
        crypto_remote.precompute_key,
        crypto_remote.nonce_pair.remote,
    );

    let message = SwapMessage::new("0.0.0.0:1234".to_string(), crypto_remote.pkh);
    let message = PeerMessageResponse::from(PeerMessage::SwapRequest(message));

    peer_mock.outgoing_message(message.as_bytes()?);

    let writer = MessageWriterBase {
        stream: peer_mock.get_mock(),
    };
    let crypto_local = crypto_mock.local;
    let mut writer = EncryptedMessageWriterBase::new(
        writer,
        crypto_local.precompute_key,
        crypto_local.nonce_pair.local,
        new_log(),
    );

    writer.write_message(&message).await?;

    Ok(())
}

#[async_std::test]
async fn can_write_message_block_header() -> Result<(), Error> {
    let crypto_mock = CryptoMock::new();

    let crypto_remote = crypto_mock.remote;
    let mut peer_mock = PeerMock::new(
        crypto_remote.precompute_key,
        crypto_remote.nonce_pair.remote,
    );

    let messages = [1, 1024, BLOCK_HEADER_MAX_SIZE]
        .iter()
        .map(|data_size| block_header_message_encoded(*data_size))
        .collect::<Vec<_>>();

    messages
        .iter()
        .for_each(|message| peer_mock.outgoing_message(message));

    let writer = MessageWriterBase {
        stream: peer_mock.get_mock(),
    };
    let crypto_local = crypto_mock.local;
    let mut writer = EncryptedMessageWriterBase::new(
        writer,
        crypto_local.precompute_key,
        crypto_local.nonce_pair.local,
        new_log(),
    );

    for message in messages {
        let message = PeerMessageResponse::from_bytes(message).unwrap();
        writer.write_message(&message).await?;
    }

    Ok(())
}
