// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

//! This module encapsulates p2p communication between peers.
//!
//! It provides message packaging from/to binary format, encryption, message nonce handling.

use std::convert::TryInto;
use std::io;

use bytes::Buf;
use core::time::Duration;
use slog::{trace, FnValue, Logger};
use thiserror::Error;
use tokio::io::{
    AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, ReadHalf, WriteHalf,
};
use tokio::net::TcpStream;

use crypto::crypto_box::PrecomputedKey;
use crypto::nonce::Nonce;
use crypto::CryptoError;
use tezos_encoding::{binary_reader::BinaryReaderError, binary_writer::BinaryWriterError};
use tezos_messages::p2p::binary_message::{
    BinaryChunk, BinaryChunkError, BinaryMessage, SizeFromChunk, CONTENT_LENGTH_FIELD_BYTES,
};

/// Max allowed content length in bytes when taking into account extra data added by encryption
pub const CONTENT_LENGTH_MAX: usize =
    tezos_messages::p2p::binary_message::CONTENT_LENGTH_MAX - crypto::crypto_box::BOX_ZERO_BYTES;

/// This is common error that might happen when communicating with peer over the network.
#[derive(Debug, Error)]
pub enum StreamError {
    #[error("Failed to encrypt message")]
    FailedToEncryptMessage { error: CryptoError },
    #[error("Failed to decrypt message")]
    FailedToDecryptMessage { error: CryptoError },
    #[error("Message serialization error: {error}")]
    SerializationError { error: BinaryWriterError },
    #[error("Message de-serialization error: {error}")]
    DeserializationError { error: BinaryReaderError },
    #[error("Network error: {message}, cause: {error}")]
    NetworkError {
        message: &'static str,
        error: anyhow::Error,
    },
}

impl From<BinaryWriterError> for StreamError {
    fn from(error: BinaryWriterError) -> Self {
        StreamError::SerializationError { error }
    }
}

impl From<std::io::Error> for StreamError {
    fn from(error: std::io::Error) -> Self {
        StreamError::NetworkError {
            error: error.into(),
            message: "Stream error",
        }
    }
}

impl From<BinaryChunkError> for StreamError {
    fn from(error: BinaryChunkError) -> Self {
        StreamError::NetworkError {
            error: error.into(),
            message: "Binary chunk error",
        }
    }
}

impl From<BinaryReaderError> for StreamError {
    fn from(error: BinaryReaderError) -> Self {
        StreamError::DeserializationError { error }
    }
}

impl slog::Value for StreamError {
    fn serialize(
        &self,
        _record: &slog::Record,
        key: slog::Key,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        serializer.emit_arguments(key, &format_args!("{}", self))
    }
}

/// Holds read and write parts of the message stream.
pub struct MessageStream {
    reader: MessageReader,
    writer: MessageWriter,
}

impl MessageStream {
    fn new(stream: TcpStream) -> MessageStream {
        let _ = stream.set_linger(Some(Duration::from_secs(2)));
        let _ = stream.set_nodelay(true);

        let (rx, tx) = tokio::io::split(stream);
        MessageStream {
            reader: MessageReaderBase {
                stream: BufReader::new(rx),
            },
            writer: MessageWriter { stream: tx },
        }
    }

    #[inline]
    pub fn split(self) -> (MessageReader, MessageWriter) {
        (self.reader, self.writer)
    }
}

impl From<TcpStream> for MessageStream {
    fn from(stream: TcpStream) -> Self {
        MessageStream::new(stream)
    }
}

pub struct Crypto {
    /// Precomputed key is created from merge of peer public key and our secret key.
    /// It's used to speedup of crypto operations.
    precomputed_key: PrecomputedKey,
    /// Nonce used to encrypt outgoing messages
    nonce: Nonce,
}

impl Crypto {
    #[inline]
    pub fn new(precomputed_key: PrecomputedKey, nonce: Nonce) -> Self {
        Self {
            precomputed_key,
            nonce,
        }
    }

    #[inline]
    fn nonce_fetch_increment(&mut self) -> Nonce {
        let nonce = self.nonce.increment();
        std::mem::replace(&mut self.nonce, nonce)
    }

    #[inline]
    pub fn encrypt<T: AsRef<[u8]>>(&mut self, data: &T) -> Result<Vec<u8>, CryptoError> {
        let nonce = self.nonce_fetch_increment();
        self.precomputed_key.encrypt(data.as_ref(), &nonce)
    }

    #[inline]
    pub fn decrypt<T: AsRef<[u8]>>(&mut self, data: &T) -> Result<Vec<u8>, CryptoError> {
        let nonce = self.nonce_fetch_increment();
        self.precomputed_key.decrypt(data.as_ref(), &nonce)
    }
}

/// Reader of a TCP/IP connection.
type MessageReader = MessageReaderBase<BufReader<ReadHalf<TcpStream>>>;

/// Reader of an async stream
pub struct MessageReaderBase<R> {
    /// reader part or the TCP/IP network stream
    pub stream: R,
}

impl<R: AsyncRead + Unpin + Send> MessageReaderBase<R> {
    /// Read message from network and return message contents in a form of bytes.
    /// Each message is prefixed by a 2 bytes indicating total length of the message.
    pub async fn read_message(&mut self) -> Result<BinaryChunk, StreamError> {
        // read encoding length (2 bytes)
        let msg_len_bytes = self.read_message_length_bytes().await?;
        // copy bytes containing encoding length to raw encoding buffer
        let mut all_recv_bytes = vec![];
        all_recv_bytes.extend(&msg_len_bytes);

        // read the message contents
        let msg_len = (&msg_len_bytes[..]).get_u16() as usize;
        let mut msg_content_bytes = vec![0u8; msg_len];
        self.stream.read_exact(&mut msg_content_bytes).await?;
        all_recv_bytes.extend(&msg_content_bytes);

        Ok(all_recv_bytes.try_into()?)
    }

    /// Read 2 bytes containing total length of the message contents from the network stream.
    /// Total length is encoded as u big endian u16.
    async fn read_message_length_bytes(&mut self) -> io::Result<[u8; CONTENT_LENGTH_FIELD_BYTES]> {
        let mut msg_len_bytes: [u8; CONTENT_LENGTH_FIELD_BYTES] = [0; CONTENT_LENGTH_FIELD_BYTES];
        self.stream.read_exact(&mut msg_len_bytes).await?;
        Ok(msg_len_bytes)
    }
}

pub type MessageWriter = MessageWriterBase<WriteHalf<TcpStream>>;

pub struct MessageWriterBase<W> {
    pub stream: W,
}

impl<W: AsyncWrite + Unpin> MessageWriterBase<W> {
    /// Construct and write message to network stream.
    ///
    /// # Arguments
    /// * `bytes` - A message contents represented ab bytes
    ///
    /// In case all bytes are successfully written to network stream a raw binary
    /// message is returned as a result.
    #[inline]
    pub async fn write_message(&mut self, bytes: &BinaryChunk) -> Result<(), StreamError> {
        Ok(self.stream.write_all(bytes.raw()).await?)
    }
}

/// The `EncryptedMessageWriter` encapsulates process of the encrypted outgoing message transmission.
/// This process involves (not only) nonce increment, encryption and network transmission.
pub type EncryptedMessageWriter = EncryptedMessageWriterBase<WriteHalf<TcpStream>>;

pub struct EncryptedMessageWriterBase<W> {
    /// Outgoing message writer
    tx: MessageWriterBase<W>,
    /// To encrypt data
    crypto: Crypto,
    /// Logger
    log: Logger,
}

impl<W: AsyncWrite + Unpin> EncryptedMessageWriterBase<W> {
    pub fn new(
        tx: MessageWriterBase<W>,
        precomputed_key: PrecomputedKey,
        nonce_local: Nonce,
        log: Logger,
    ) -> Self {
        EncryptedMessageWriterBase {
            tx,
            crypto: Crypto {
                precomputed_key,
                nonce: nonce_local,
            },
            log,
        }
    }

    pub async fn write_message<'a>(
        &'a mut self,
        message: &'a impl BinaryMessage,
    ) -> Result<(), StreamError> {
        let message_bytes = message.as_bytes()?;
        trace!(self.log, "Writing message"; "message" => FnValue(|_| hex::encode(&message_bytes)));

        for chunk_content_bytes in message_bytes.chunks(CONTENT_LENGTH_MAX) {
            let message_bytes_encrypted = match self.crypto.encrypt(&chunk_content_bytes) {
                Ok(msg) => msg,
                Err(error) => return Err(StreamError::FailedToEncryptMessage { error }),
            };

            // send
            let chunk = BinaryChunk::from_content(&message_bytes_encrypted)?;
            self.tx.write_message(&chunk).await?;
        }

        Ok(())
    }
}

/// The `MessageReceiver` encapsulates process of the encrypted incoming message transmission.
/// This process involves (not only) nonce increment, encryption and network transmission.
pub type EncryptedMessageReader = EncryptedMessageReaderBase<BufReader<ReadHalf<TcpStream>>>;

pub struct EncryptedMessageReaderBase<A> {
    /// To encrypt data
    crypto: Crypto,
    /// Incoming message reader
    rx: MessageReaderBase<A>,
    /// Logger
    log: Logger,
}

impl<A: AsyncRead + Unpin + Send> EncryptedMessageReaderBase<A> {
    /// Create new encrypted message from async reader and peer data
    pub fn new(
        rx: MessageReaderBase<A>,
        precomputed_key: PrecomputedKey,
        nonce_remote: Nonce,
        log: Logger,
    ) -> Self {
        EncryptedMessageReaderBase {
            rx,
            crypto: Crypto {
                precomputed_key,
                nonce: nonce_remote,
            },
            log,
        }
    }

    /// Consume content of inner message reader into specific message
    pub async fn read_message<M>(&mut self) -> Result<M, StreamError>
    where
        M: BinaryMessage + SizeFromChunk,
    {
        let mut input_size = 0;
        let mut input_data = vec![];

        loop {
            // read
            let message_encrypted = self.rx.read_message().await?;

            // decrypt
            match self.crypto.decrypt(&message_encrypted.content()) {
                Ok(mut message_decrypted) => {
                    trace!(self.log, "Message received"; "message" => FnValue(|_| hex::encode(&message_decrypted)));

                    if input_size == 0 {
                        input_size = M::size_from_chunk(&message_decrypted)?;
                    }
                    input_data.append(&mut message_decrypted);

                    if input_size <= input_data.len() {
                        match M::from_bytes(&input_data) {
                            Ok(message) => break Ok(message),
                            Err(e) => break Err(e.into()),
                        }
                    }
                }
                Err(error) => {
                    break Err(StreamError::FailedToDecryptMessage { error });
                }
            }
        }
    }
}

impl EncryptedMessageReaderBase<BufReader<ReadHalf<TcpStream>>> {
    pub fn unsplit(self, tx: EncryptedMessageWriter) -> TcpStream {
        self.rx.stream.into_inner().unsplit(tx.tx.stream)
    }
}
