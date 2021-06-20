// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use anyhow::Error;
use networking::p2p::stream::{EncryptedMessageReaderBase, MessageReaderBase};
use tezos_messages::p2p::binary_message::BinaryWrite;
use tezos_messages::p2p::encoding::limits::BLOCK_HEADER_MAX_SIZE;
use tezos_messages::p2p::{
    binary_message::BinaryChunk,
    encoding::{
        peer::{PeerMessage, PeerMessageResponse},
        swap::SwapMessage,
    },
};
use tokio_test::io::Builder;

pub mod common;

use self::common::{block_header_message_encoded, data_sample, new_log, CryptoMock, PeerMock};

#[async_std::test]
#[ignore]
async fn error_read_empty_chunk() -> Result<(), Error> {
    let mut builder = Builder::new();
    let chunk = BinaryChunk::from_content(&data_sample(0, 0))?;
    builder.read(chunk.raw());
    let mock = builder.build();

    let mut reader = MessageReaderBase { stream: mock };
    let _err = reader.read_message().await.expect_err("Error is expected");

    Ok(())
}

#[async_std::test]
async fn can_read_chunks() -> Result<(), Error> {
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
        builder.read(chunk.raw());
    }
    let mock = builder.build();

    let mut reader = MessageReaderBase { stream: mock };
    for data in chunks {
        let chunk = reader.read_message().await?;
        assert_eq!(data, chunk.content());
    }

    Ok(())
}

#[async_std::test]
async fn can_read_message_swap() -> Result<(), Error> {
    let crypto_mock = CryptoMock::new();

    let crypto_remote = crypto_mock.remote;
    let mut peer_mock = PeerMock::new(crypto_remote.precompute_key, crypto_remote.nonce_pair.local);

    let message = SwapMessage::new("0.0.0.0:1234".to_string(), crypto_remote.pkh);
    let message = PeerMessageResponse::from(PeerMessage::SwapRequest(message));

    peer_mock.incoming_message(message.as_bytes()?);

    let reader = MessageReaderBase {
        stream: peer_mock.get_mock(),
    };
    let crypto_local = crypto_mock.local;
    let mut reader = EncryptedMessageReaderBase::new(
        reader,
        crypto_local.precompute_key,
        crypto_local.nonce_pair.remote,
        new_log(),
    );

    let recv_message = reader.read_message::<PeerMessageResponse>().await?;

    assert_eq!(message.as_bytes()?, recv_message.as_bytes()?);

    Ok(())
}

#[async_std::test]
async fn can_read_message_block_header() -> Result<(), Error> {
    let crypto_mock = CryptoMock::new();

    let crypto_remote = crypto_mock.remote;
    let mut peer_mock = PeerMock::new(crypto_remote.precompute_key, crypto_remote.nonce_pair.local);

    let messages = vec![
        block_header_message_encoded(1),
        block_header_message_encoded(1024),
        block_header_message_encoded(BLOCK_HEADER_MAX_SIZE),
    ];

    for message in messages.iter() {
        peer_mock.incoming_message(message.clone());
    }

    let reader = MessageReaderBase {
        stream: peer_mock.get_mock(),
    };
    let crypto_local = crypto_mock.local;
    let mut reader = EncryptedMessageReaderBase::new(
        reader,
        crypto_local.precompute_key,
        crypto_local.nonce_pair.remote,
        new_log(),
    );

    for message in messages {
        let recv_message = reader.read_message::<PeerMessageResponse>().await?;
        assert_eq!(message, recv_message.as_bytes()?);
    }

    Ok(())
}

#[async_std::test]
async fn can_read_message_block_header_small_chunks() -> Result<(), Error> {
    let crypto_mock = CryptoMock::new();

    let crypto_remote = crypto_mock.remote;
    let mut peer_mock = PeerMock::new(crypto_remote.precompute_key, crypto_remote.nonce_pair.local)
        .chunk_size(1024);

    let messages = vec![
        block_header_message_encoded(1),
        block_header_message_encoded(1024),
        block_header_message_encoded(BLOCK_HEADER_MAX_SIZE),
    ];

    for message in messages.iter() {
        peer_mock.incoming_message(message.clone());
    }

    let reader = MessageReaderBase {
        stream: peer_mock.get_mock(),
    };
    let crypto_local = crypto_mock.local;
    let mut reader = EncryptedMessageReaderBase::new(
        reader,
        crypto_local.precompute_key,
        crypto_local.nonce_pair.remote,
        new_log(),
    );

    for message in messages {
        let recv_message = reader.read_message::<PeerMessageResponse>().await?;
        assert_eq!(message, recv_message.as_bytes()?);
    }

    Ok(())
}
