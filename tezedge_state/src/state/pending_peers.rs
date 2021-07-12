use std::io::{self, Read, Write};
use std::fmt::{self, Debug};
use std::time::{Instant, Duration};

use crypto::crypto_box::{CryptoKey, PrecomputedKey, PublicKey};
use crypto::nonce::{Nonce, generate_nonces};
use crypto::proof_of_work::{PowError, PowResult, check_proof_of_work};
use tezos_identity::Identity;
use tezos_messages::p2p::binary_message::{BinaryChunk, BinaryWrite};
use tezos_messages::p2p::encoding::ack::{NackInfo, NackMotive};
pub use tla_sm::{Proposal, GetRequests};
use tezos_messages::p2p::encoding::prelude::{
    NetworkVersion,
    ConnectionMessage,
    MetadataMessage,
    AckMessage,
};

use crate::peer_address::PeerListenerAddress;
use crate::proposals::{PeerHandshakeMessage, PeerHandshakeMessageError};
use crate::{Effects, PeerAddress, PeerCrypto, Port, ShellCompatibilityVersion, TezedgeConfig};
use crate::state::{NotMatchingAddress, RequestState};
use crate::chunking::{HandshakeReadBuffer, ChunkWriter, EncryptedMessageWriter, WriteMessageError};

#[derive(Clone)]
pub struct ReceivedConnectionMessageData {
    port: Port,
    compatible_version: Option<NetworkVersion>,
    public_key: PublicKey,
    encoded: BinaryChunk,
}

impl ReceivedConnectionMessageData {
    pub fn port(&self) -> Port {
        self.port
    }

    pub fn compatible_version(&self) -> Option<&NetworkVersion> {
        self.compatible_version.as_ref()
    }

    pub fn public_key(&self) -> &PublicKey {
        &self.public_key
    }
}

impl Debug for ReceivedConnectionMessageData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectionMessageEncodingCached")
            .field("port", &self.port)
            .field("version", &self.compatible_version)
            .field("public_key", &self.public_key)
            .finish()
    }
}

#[derive(Debug)]
pub enum HandleReceivedMessageError {
    UnexpectedState,
    BadPow,
    ConnectingToMyself,
    BadHandshakeMessage(PeerHandshakeMessageError),
}

impl From<PeerHandshakeMessageError> for HandleReceivedMessageError {
    fn from(err: PeerHandshakeMessageError) -> Self {
        Self::BadHandshakeMessage(err)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum HandshakeMessageType {
    Connection,
    Metadata,
    Ack,
}

pub struct HandshakeResult {
    pub port: Port,
    pub compatible_version: NetworkVersion,
    pub public_key: PublicKey,
    pub disable_mempool: bool,
    pub private_node: bool,
    pub crypto: PeerCrypto,
}

#[derive(Clone)]
pub enum HandshakeStep {
    Initiated { at: Instant },
    Connect {
        sent: RequestState,
        received: Option<ReceivedConnectionMessageData>,
        sent_conn_msg: ConnectionMessage,
    },
    Metadata {
        sent: RequestState,
        received: Option<MetadataMessage>,

        port: Port,
        compatible_version: Option<NetworkVersion>,
        public_key: PublicKey,
        crypto: PeerCrypto,
    },
    Ack {
        sent: RequestState,
        received: bool,

        port: Port,
        compatible_version: Option<NetworkVersion>,
        public_key: PublicKey,
        disable_mempool: bool,
        private_node: bool,
        crypto: PeerCrypto,
    },
}

impl HandshakeStep {
    pub fn is_finished(&self) -> bool {
        use RequestState::*;
        matches!(
            self,
            Self::Ack { sent: Success { .. }, received: true, .. }
        )
    }

    pub fn public_key(&self) -> Option<&PublicKey> {
        match self {
            Self::Connect { received: Some(conn_msg), .. } => {
                Some(conn_msg.public_key())
            }
            Self::Metadata { public_key, .. }
            | Self::Ack { public_key, .. } => {
                Some(&public_key)
            }
            _ => None,
        }
    }

    pub fn crypto(&mut self) -> Option<&mut PeerCrypto> {
        match self {
            Self::Connect { .. }
            | Self::Initiated { .. } => None,
            Self::Metadata { crypto, .. }
            | Self::Ack { crypto, .. } => Some(crypto),
        }
    }

    /// Port on which peer is listening.
    pub fn listener_port(&self) -> Option<Port> {
        match self {
            Self::Initiated { .. } => None,
            Self::Connect { received, .. } => received.as_ref().map(|x| x.port()),
            Self::Metadata { port, .. }
            | Self::Ack { port, .. } => Some(*port),
        }
    }

    pub fn to_result(self) -> Option<HandshakeResult> {
        use RequestState::*;

        match self {
            Self::Ack {
                sent: Success { .. },
                received: true,

                port,
                compatible_version: Some(compatible_version),
                public_key,
                disable_mempool,
                private_node,
                crypto,
            } => {
                Some(HandshakeResult {
                    port,
                    compatible_version,
                    public_key,
                    disable_mempool,
                    private_node,
                    crypto,
                })
            }
            _ => None,
        }
    }
}

impl Debug for HandshakeStep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Initiated { at } => {
                f.debug_struct("HandshakeStep::Initiated")
                    .field("at", at)
                    .finish()
            }
            Self::Connect { sent, received, .. } => {
                f.debug_struct("HandshakeStep::Connect")
                    .field("sent", sent)
                    .field("received", &received.is_some())
                    .finish()
            }
            Self::Metadata { sent, received, .. } => {
                f.debug_struct("HandshakeStep::Metadata")
                    .field("sent", sent)
                    .field("received", &received.is_some())
                    .finish()
            }
            Self::Ack { sent, received, .. } => {
                f.debug_struct("HandshakeStep::Ack")
                    .field("sent", sent)
                    .field("received", received)
                    .finish()
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct PendingPeer {
    pub address: PeerAddress,
    pub incoming: bool,
    /// Handshake step.
    pub step: HandshakeStep,
    /// Will be some if we should nack the handshake.
    nack_motive: Option<NackMotive>,
    pub read_buf: HandshakeReadBuffer,
    conn_msg_writer: Option<ChunkWriter>,
    msg_writer: Option<(HandshakeMessageType, EncryptedMessageWriter)>,
}

impl PendingPeer {
    pub(crate) fn new(address: PeerAddress, incoming: bool, step: HandshakeStep) -> Self {
        Self {
            address,
            incoming,
            step,
            nack_motive: None,
            read_buf: HandshakeReadBuffer::new(),
            conn_msg_writer: None,
            msg_writer: None,
        }
    }

    /// Port on which peer is listening.
    pub fn listener_port(&self) -> Option<Port> {
        if !self.incoming {
            // if it's outgoing connection, then the port is listener port.
            Some(self.address.port())
        } else {
            self.step.listener_port()
        }
    }

    pub fn listener_address(&self) -> Option<PeerListenerAddress> {
        self.listener_port()
            .map(|port| PeerListenerAddress::new(self.address.ip(), port))
    }

    pub fn public_key(&self) -> Option<&PublicKey> {
        self.step.public_key()
    }

    pub fn nack_motive(&self) -> Option<NackMotive> {
        self.nack_motive.clone()
    }

    pub fn nack_peer(&mut self, motive: NackMotive) {
        if self.nack_motive.is_none() {
            self.nack_motive = Some(motive);
        }
    }

    /// Advance to the `Metadata` step if current step is finished.
    fn advance_to_metadata(&mut self, at: Instant, node_identity: &Identity) -> bool {
        use HandshakeStep::*;
        use RequestState::*;

        match &self.step {
            Connect {
                sent: Success { .. },
                received: Some(conn_msg),
                sent_conn_msg,
            } => {
                let nonce_pair = generate_nonces(
                    &BinaryChunk::from_content(&sent_conn_msg.as_bytes().unwrap()).unwrap().raw(),
                    conn_msg.encoded.raw(),
                    self.incoming,
                ).unwrap();

                let precomputed_key = PrecomputedKey::precompute(
                    &conn_msg.public_key(),
                    &node_identity.secret_key,
                );

                let crypto = PeerCrypto::new(precomputed_key, nonce_pair);
                self.step = Metadata {
                    sent: Idle { at },
                    received: None,

                    port: conn_msg.port,
                    public_key: conn_msg.public_key.clone(),
                    compatible_version: conn_msg.compatible_version.clone(),

                    crypto,
                };
                true
            }
            _ => false,
        }
    }

    fn advance_to_ack(&mut self, at: Instant) -> bool {
        use HandshakeStep::*;
        use RequestState::*;

        match &self.step {
            Metadata {
                sent: Success { .. },
                received: Some(meta_msg),

                port,
                public_key,
                compatible_version,
                crypto,
            } => {
                self.step = Ack {
                    sent: Idle { at },
                    received: false,

                    port: port.clone(),
                    public_key: public_key.clone(),
                    compatible_version: compatible_version.clone(),
                    private_node: meta_msg.private_node(),
                    disable_mempool: meta_msg.disable_mempool(),
                    crypto: crypto.clone(),
                };
                true
            }
            _ => false,
        }
    }

    /// We should read from pending peer only if we are waiting for a
    /// message from them.
    pub fn should_read(&mut self) -> bool {
        use HandshakeStep::*;
        use RequestState::*;

        match &self.step {
            Initiated { .. } if self.incoming => true,
            Connect { sent: Success { .. }, received: None, .. } => true,
            Metadata { sent: Success { .. }, received: None, .. } => true,
            Ack { sent: Success { .. }, received: false, .. } => true,
            _ => false,
        }
    }

    #[inline]
    pub fn read_message_from<R: Read>(&mut self, reader: &mut R) -> Result<(), io::Error> {
        self.read_buf.read_from(reader)
    }

    /// Enqueues send connection message and updates `RequestState` with
    /// `Pending` state, if we should be sending connection message based
    /// on current handshake state with the peer.
    ///
    /// Returns:
    ///
    /// - `Ok(was_message_queued)`: if `false`, means we shouldn't be
    ///   sending this concrete message at a current stage(state).
    ///
    /// - `Err(error)`: if error ocurred when encoding the message.
    pub fn enqueue_send_conn_msg(&mut self, at: Instant) -> Result<bool, WriteMessageError> {
        use HandshakeStep::*;
        use RequestState::*;

        match &mut self.step {
            Connect { sent: req_state @ Idle { .. }, sent_conn_msg, .. } => {
                self.conn_msg_writer = Some(ChunkWriter::new(
                    BinaryChunk::from_content(
                        &sent_conn_msg.as_bytes()?,
                    )?,
                ));
                *req_state = Pending { at };
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    /// Enqueues send metadata message and updates `RequestState` with
    /// `Pending` state, if we should be sending connection message based
    /// on current handshake state with the peer.
    ///
    /// Returns:
    ///
    /// - `Ok(was_message_queued)`: if `false`, means we shouldn't be
    ///   sending this concrete message at a current stage(state).
    ///
    /// - `Err(error)`: if error ocurred when encoding the message.
    pub fn enqueue_send_meta_msg(&mut self, at: Instant, meta_msg: MetadataMessage) -> Result<bool, WriteMessageError> {
        use HandshakeStep::*;
        use RequestState::*;

        match &mut self.step {
            Metadata { sent: req_state @ Idle { .. }, .. } => {
                self.msg_writer = Some((
                    HandshakeMessageType::Metadata,
                    EncryptedMessageWriter::try_new(&meta_msg)?,
                ));
                *req_state = Pending { at };
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    /// Enqueues send ack message and updates `RequestState` with
    /// `Pending` state, if we should be sending connection message based
    /// on current handshake state with the peer.
    ///
    /// Returns:
    ///
    /// - `Ok(was_message_queued)`: if `false`, means we shouldn't be
    ///   sending this concrete message at a current stage(state).
    ///
    /// - `Err(error)`: if error ocurred when encoding the message.
    pub fn enqueue_send_ack_msg<F>(
        &mut self,
        at: Instant,
        get_potential_peers: F,
    ) -> Result<bool, WriteMessageError>
        where F: FnOnce() -> Vec<String>,
    {
        use HandshakeStep::*;
        use RequestState::*;

        match &mut self.step {
            Ack { sent: req_state @ Idle { .. }, .. } => {
                self.msg_writer = Some((
                    HandshakeMessageType::Ack,
                    EncryptedMessageWriter::try_new(&match &self.nack_motive {
                        Some(motive) => {
                            AckMessage::Nack(NackInfo::new(motive.clone(), &get_potential_peers()))
                        }
                        None => AckMessage::Ack,
                    })?,
                ));
                *req_state = Pending { at };
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    pub fn send_conn_msg_successful(&mut self, at: Instant, node_identity: &Identity) -> bool {
        use HandshakeStep::*;
        use RequestState::*;

        match &mut self.step {
            Connect { sent: req_state @ Pending { .. }, sent_conn_msg, .. } => {
                *req_state = Success { at };
                self.advance_to_metadata(at, node_identity);
                self.conn_msg_writer = None;
                true
            }
            _ => false
        }
    }

    pub fn send_meta_msg_successful(&mut self, at: Instant) -> bool {
        use HandshakeStep::*;
        use RequestState::*;

        match &mut self.step {
            Metadata { sent: req_state @ Pending { .. }, .. } => {
                *req_state = Success { at };
                self.advance_to_ack(at);
                self.msg_writer = None;
                true
            }
            _ => false,
        }
    }

    pub fn send_ack_msg_successful(&mut self, at: Instant) -> bool {
        use HandshakeStep::*;
        use RequestState::*;

        match &mut self.step {
            Ack { sent: req_state @ Pending { .. }, .. } => {
                *req_state = Success { at };
                self.msg_writer = None;

                true
            }
            _ => false,
        }
    }

    pub fn write_to<W: Write>(
        &mut self,
        writer: &mut W,
    ) -> Result<HandshakeMessageType, WriteMessageError>
    {
        if let Some(chunk_writer) = self.conn_msg_writer.as_mut() {
            chunk_writer.write_to(writer)?;
            self.conn_msg_writer = None;
            Ok(HandshakeMessageType::Connection)
        } else if let Some((msg_type, msg_writer)) = self.msg_writer.as_mut() {
            let msg_type = *msg_type;

            let crypto = match self.step.crypto() {
                Some(crypto) => crypto,
                None => {
                    #[cfg(test)]
                    unreachable!("this shouldn't be reachable, as encryption is needed by metadata and ack message, and we shouldn't be sending that if we haven't exchange connection messages.");

                    self.msg_writer = None;
                    return Err(WriteMessageError::Empty);
                }
            };

            msg_writer.write_to(writer, crypto)?;
            self.msg_writer = None;
            Ok(msg_type)
        } else {
            Err(WriteMessageError::Empty)
        }
    }

    fn check_proof_of_work(pow_target: f64, conn_msg_bytes: &[u8]) -> PowResult {
        if conn_msg_bytes.len() < 58 {
            Err(PowError::CheckFailed)
        } else {
            // skip first 2 bytes which are for port.
            check_proof_of_work(&conn_msg_bytes[2..58], pow_target)
        }
    }

    pub fn handle_received_conn_message<E, M>(
        &mut self,
        config: &TezedgeConfig,
        node_identity: &Identity,
        shell_compatibility_version: &ShellCompatibilityVersion,
        effects: &mut E,
        at: Instant,
        mut message: M,
    ) -> Result<PublicKey, HandleReceivedMessageError>
        where E: Effects,
              M: PeerHandshakeMessage,
    {
        use HandshakeStep::*;
        use RequestState::*;

        if let Err(e) = Self::check_proof_of_work(config.pow_target, message.binary_chunk().content()){
            // TODO: check maybe this message is nack.
            return Err(HandleReceivedMessageError::BadPow);
        }
        let conn_msg = message.as_connection_msg()?;

        if node_identity.public_key.as_ref().as_ref() == conn_msg.public_key() {
            return Err(HandleReceivedMessageError::ConnectingToMyself);
        }
        let compatible_version =
            match shell_compatibility_version.choose_compatible_version(conn_msg.version()) {
                Ok(compatible_version) => Some(compatible_version),
                Err(motive) => {
                    self.nack_peer(motive);
                    None
                }
            };

        let public_key = PublicKey::from_bytes(conn_msg.public_key()).unwrap();
        let received_conn_msg = ReceivedConnectionMessageData {
            port: conn_msg.port,
            compatible_version,
            public_key: public_key.clone(),
            encoded: message.take_binary_chunk(),
        };

        match &mut self.step {
            Initiated { .. } => {
                self.step = Connect {
                    sent: Idle { at },
                    received: Some(received_conn_msg),
                    sent_conn_msg: ConnectionMessage::try_new(
                        config.port,
                        &node_identity.public_key,
                        &node_identity.proof_of_work_stamp,
                        effects.get_nonce(&self.address),
                        shell_compatibility_version.to_network_version(),
                    ).unwrap(),
                };
            }
            Connect { received, .. } => {
                *received = Some(received_conn_msg);
                self.advance_to_metadata(at, node_identity);
            }
            _ => return Err(HandleReceivedMessageError::UnexpectedState),
        }

        Ok(public_key)
    }

    pub fn handle_received_meta_message<M>(
        &mut self,
        at: Instant,
        mut message: M,
    ) -> Result<(), HandleReceivedMessageError>
        where M: PeerHandshakeMessage,
    {
        use HandshakeStep::*;

        match &mut self.step {
            Metadata { received, crypto, .. } => {
                let meta_msg = message.as_metadata_msg(crypto)?;
                *received = Some(meta_msg);
                self.advance_to_ack(at);
                Ok(())
            }
            _ => return Err(HandleReceivedMessageError::UnexpectedState),

        }
    }

    pub fn handle_received_ack_message<M>(
        &mut self,
        at: Instant,
        mut message: M,
    ) -> Result<AckMessage, HandleReceivedMessageError>
        where M: PeerHandshakeMessage,
    {
        use HandshakeStep::*;

        match &mut self.step {
            Ack { received, crypto, .. } => {
                let msg = message.as_ack_msg(crypto)?;
                *received = true;
                Ok(msg)
            }
            _ => return Err(HandleReceivedMessageError::UnexpectedState),
        }
    }

    #[inline]
    pub fn is_handshake_finished(&self) -> bool {
        self.step.is_finished()
    }

    /// Returns handshake result if the handshake is finished and
    /// we didn't nack the peer.
    #[inline]
    pub fn to_handshake_result(self) -> Option<HandshakeResult> {
        if self.nack_motive.is_some() {
            None
        } else {
            self.step.to_result()
        }
    }
}

#[derive(Debug, Clone)]
pub struct PendingPeers {
    peers: slab::Slab<PendingPeer>,
}

impl PendingPeers {
    #[inline]
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            peers: slab::Slab::with_capacity(capacity),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.peers.len()
    }

    fn find_index(&self, address: &PeerAddress) -> Option<usize> {
        self.peers.iter()
            .find(|(_, x)| &x.address == address)
            .map(|(index, _)| index)
    }

    #[inline]
    pub fn contains_address(&self, address: &PeerAddress) -> bool {
        self.find_index(address).is_some()
    }

    #[inline]
    pub fn get(&self, id: &PeerAddress) -> Option<&PendingPeer> {
        if let Some(index) = self.find_index(id) {
            self.peers.get(index)
        } else {
            None
        }
    }

    #[inline]
    pub fn get_mut(&mut self, id: &PeerAddress) -> Option<&mut PendingPeer> {
        if let Some(index) = self.find_index(id) {
            self.peers.get_mut(index)
        } else {
            None
        }
    }

    #[inline]
    pub(crate) fn insert(&mut self, peer: PendingPeer) -> usize {
        self.peers.insert(peer)
    }

    #[inline]
    pub(crate) fn remove(&mut self, id: &PeerAddress) -> Option<PendingPeer> {
        self.find_index(id)
            .map(|index| self.peers.remove(index))
    }

    #[inline]
    pub fn iter(&self) -> slab::Iter<PendingPeer> {
        self.peers.iter()
    }

    #[inline]
    pub fn iter_mut(&mut self) -> slab::IterMut<PendingPeer> {
        self.peers.iter_mut()
    }

    #[inline]
    pub(crate) fn take(&mut self) -> Self {
        std::mem::replace(self, Self::new())
    }
}

impl IntoIterator for PendingPeers {
    type Item = (usize, PendingPeer);
    type IntoIter = slab::IntoIter<PendingPeer>;

    fn into_iter(self) -> Self::IntoIter {
        self.peers.into_iter()
    }
}
