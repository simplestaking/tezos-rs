use std::iter::FromIterator;
use std::{convert::TryInto, io::{self, Read, Write}};
use std::{convert::TryFrom, error::Error, time::{Duration, Instant}};
use std::fmt::Debug;
use std::collections::{HashMap, HashSet};
use tla_sm::{Proposal, Acceptor, GetRequests};

use crypto::{crypto_box::{CryptoKey, PrecomputedKey, PublicKey, SecretKey}, hash::{CryptoboxPublicKeyHash, HashTrait}, nonce::NoncePair, proof_of_work::ProofOfWork};
use hex::FromHex;
use mio::net::{SocketAddr, TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};
use bytes::Buf;

use slog::debug;
use tezos_encoding::binary_writer::BinaryWriterError;
use tezos_identity::Identity;
use tezos_messages::p2p::{binary_message::BinaryMessage, encoding::{ack::AckMessage, connection::ConnectionMessage, prelude::{MetadataMessage, NetworkVersion}}};

use tezos_messages::p2p::binary_message::{
    BinaryChunk, BinaryChunkError, CONTENT_LENGTH_FIELD_BYTES,
};
use tezedge_state::{TezedgeState, TezedgeRequest, TezedgeConfig, PeerCrypto, PeerAddress};
use tezedge_state::proposals::{
    TickProposal,
    ExtendPotentialPeersProposal,
    HandshakeProposal, HandshakeMsg,
    PeerProposal, PeerMessage,
    PendingRequestProposal, PendingRequestMsg,
};
use tezedge_state::proposals::peer_message::PeerBinaryMessage;

fn network_version() -> NetworkVersion {
    NetworkVersion::new("TEZOS_MAINNET".to_string(), 0, 1)
}

fn identity(pkh: &[u8], pk: &[u8], sk: &[u8], pow: &[u8]) -> Identity {
    Identity {
        peer_id: CryptoboxPublicKeyHash::try_from_bytes(pkh).unwrap(),
        public_key: PublicKey::from_bytes(pk).unwrap(),
        secret_key: SecretKey::from_bytes(sk).unwrap(),
        proof_of_work_stamp: ProofOfWork::from_hex(hex::encode(pow)).unwrap(),
    }
}

fn identity_1() -> Identity {
    identity(
        &[86, 205, 231, 178, 152, 146, 2, 157, 213, 131, 90, 117, 83, 132, 177, 84],
        &[148, 73, 141, 148, 22, 20, 15, 188, 69, 132, 149, 51, 61, 170, 193, 180, 200, 126, 65, 159, 87, 38, 113, 122, 84, 249, 182, 198, 116, 118, 174, 28],
        &[172, 122, 207, 58, 254, 215, 99, 123, 225, 15, 143, 199, 106, 46, 182, 179, 53, 156, 120, 173, 177, 216, 19, 180, 28, 186, 179, 250, 233, 84, 244, 177],
        &[187, 194, 48, 1, 73, 36, 158, 28, 204, 132, 165, 67, 98, 35, 108, 60, 187, 194, 204, 47, 251, 211, 182, 234],
    )
}

// Some tokens to allow us to identify which event is for which socket.
const SERVER: Token = Token(0);

const SERVER_PORT: u16 = 13632;

trait GetMessageType {
    fn get_message_type(&self) -> SendMessageType;
}

trait AsSendMessage {
    type Error;

    fn as_send_message(&self) -> Result<SendMessage, Self::Error>;
}

trait AsEncryptedSendMessage {
    type Error;

    fn as_encrypted_send_message(
        &self,
        crypto: &mut PeerCrypto,
    ) -> Result<SendMessage, Self::Error>;
}

#[derive(Debug, Clone, Copy)]
enum SendMessageType {
    Connect,
    Meta,
    Ack,
}

#[derive(Debug)]
enum SendMessageError {
    IO(io::Error),
    QueueFull,
    EncodeFailed,
}

impl From<io::Error> for SendMessageError {
    fn from(err: io::Error) -> Self {
        Self::IO(err)
    }
}

#[derive(Debug)]
enum SendMessageResult {
    Empty,

    Pending {
        message_type: SendMessageType,
    },

    Ok {
        message_type: SendMessageType,
    },

    Err {
        message_type: SendMessageType,
        error: SendMessageError,
    },
}

impl SendMessageResult {
    pub fn empty() -> Self {
        Self::Empty
    }

    pub fn pending(message_type: SendMessageType) -> Self {
        Self::Pending { message_type }
    }

    pub fn ok(message_type: SendMessageType) -> Self {
        Self::Ok { message_type }
    }

    pub fn err(message_type: SendMessageType, error: SendMessageError) -> Self {
        Self::Err { message_type, error }
    }
}

#[derive(Debug)]
enum ReadMessageError {
    IO(io::Error),
    BinaryChunk(BinaryChunkError),
    DecodeFailed,
}

impl From<io::Error> for ReadMessageError {
    fn from(err: io::Error) -> Self {
        Self::IO(err)
    }
}

impl From<BinaryChunkError> for ReadMessageError {
    fn from(err: BinaryChunkError) -> Self {
        Self::BinaryChunk(err)
    }
}

#[derive(Debug)]
enum ReadMessageResult {
    Empty,
    Pending,
    Ok(BinaryChunk),
    Err(ReadMessageError),
}

impl From<ReadMessageError> for ReadMessageResult {
    fn from(err: ReadMessageError) -> Self {
        ReadMessageResult::Err(err)
    }
}

#[derive(Debug)]
struct SendMessage {
    bytes: BinaryChunk,
    message_type: SendMessageType,
}

impl SendMessage {
    fn new(message_type: SendMessageType, bytes: BinaryChunk) -> Self {
        Self { message_type, bytes }
    }

    #[inline]
    fn bytes(&self) -> &[u8] {
        self.bytes.raw()
    }

    #[inline]
    fn message_type(&self) -> SendMessageType {
        self.message_type
    }
}

impl GetMessageType for SendMessage {
    fn get_message_type(&self) -> SendMessageType {
        self.message_type()
    }
}


impl GetMessageType for ConnectionMessage {
    fn get_message_type(&self) -> SendMessageType {
        SendMessageType::Connect
    }
}


impl GetMessageType for MetadataMessage {
    fn get_message_type(&self) -> SendMessageType {
        SendMessageType::Meta
    }
}


impl GetMessageType for AckMessage {
    fn get_message_type(&self) -> SendMessageType {
        SendMessageType::Ack
    }
}

impl<M> AsSendMessage for M
    where M: BinaryMessage + GetMessageType
{
    type Error = failure::Error;

    fn as_send_message(&self) -> Result<SendMessage, Self::Error> {
        Ok(SendMessage {
            bytes: BinaryChunk::from_content(&self.as_bytes()?)?,
            message_type: self.get_message_type(),
        })
    }
}

impl<M> AsEncryptedSendMessage for M
    where M: BinaryMessage + GetMessageType
{
    type Error = failure::Error;

    fn as_encrypted_send_message(
        &self,
        crypto: &mut PeerCrypto,
    ) -> Result<SendMessage, Self::Error>
    {
        let encrypted = crypto.encrypt(
            &self.as_bytes()?,
        )?;
        Ok(SendMessage {
            bytes: BinaryChunk::from_content(&encrypted)?,
            message_type: self.get_message_type(),
        })
    }
}

#[derive(Debug)]
enum ReadQueue {
    ReadLen {
        buf: [u8; CONTENT_LENGTH_FIELD_BYTES],
        index: usize,
    },
    ReadContent {
        buf: Vec<u8>,
        index: usize,
        expected_len: usize,
    },
}

impl ReadQueue {
    pub fn new() -> Self {
        Self::ReadLen {
            buf: [0; CONTENT_LENGTH_FIELD_BYTES],
            index: 0,
        }
    }

    pub fn is_finished(&self) -> bool {
        match self {
            Self::ReadContent { index, expected_len, .. } => {
                *index == expected_len - 1
            }
            _ => false,
        }
    }

    fn next_slice(&mut self) -> &mut [u8] {
        match *self {
            Self::ReadLen { ref mut buf, index } => {
                &mut buf[index..]
            }
            Self::ReadContent { ref mut buf, index, .. } => {
                &mut buf[index..]
            }
        }
    }

    fn advance(&mut self, by: usize) {
        match self {
            Self::ReadLen { buf: expected_len_bytes, index, .. } => {
                *index = (*index + by).min(CONTENT_LENGTH_FIELD_BYTES - 1);
                if *index == CONTENT_LENGTH_FIELD_BYTES - 1 {
                    let expected_len = CONTENT_LENGTH_FIELD_BYTES
                        + (&expected_len_bytes[..]).get_u16() as usize;
                    let mut buf = vec![0; expected_len];

                    for i in 0..CONTENT_LENGTH_FIELD_BYTES {
                        buf[i] = expected_len_bytes[i];
                    }

                    *self = ReadQueue::ReadContent {
                        expected_len,
                        buf,
                        index: CONTENT_LENGTH_FIELD_BYTES,
                    };
                }
            }
            Self::ReadContent { index, expected_len, .. } => {
                *index = (*index + by).min(*expected_len - 1);
            }
        }
    }

    fn take(self) -> Result<BinaryChunk, BinaryChunkError> {
        match self {
            Self::ReadLen { buf, .. } => BinaryChunk::from_raw(buf.to_vec()),
            Self::ReadContent { buf, .. } => BinaryChunk::from_raw(buf),
        }
    }
}

#[derive(Debug)]
struct WriteQueue {
    message: SendMessage,
    index: usize,
}

impl WriteQueue {
    fn new(message: SendMessage) -> Self {
        Self {
            message,
            index: 0,
        }
    }

    #[inline]
    fn bytes(&self) -> &[u8] {
        self.message.bytes()
    }

    #[inline]
    fn message_type(&self) -> SendMessageType {
        self.message.message_type()
    }

    fn is_finished(&self) -> bool {
        self.index == self.bytes().len() - 1
    }

    fn next_slice(&self) -> &[u8] {
        &self.bytes()[self.index..]
    }

    fn advance(&mut self, by: usize) {
        self.index = (self.index + by).min(self.bytes().len() - 1);
    }

    fn result_pending(&self) -> SendMessageResult {
        SendMessageResult::pending(self.message_type())
    }

    fn result_ok(&self) -> SendMessageResult {
        SendMessageResult::ok(self.message_type())
    }

    fn result_err(&self, error: SendMessageError) -> SendMessageResult {
        SendMessageResult::err(self.message_type(), error)
    }
}

#[derive(Debug)]
struct Connection {
    address: PeerAddress,
    stream: TcpStream,
    read_queue: Option<ReadQueue>,
    write_queue: Option<WriteQueue>,
}

impl Connection {
    pub fn try_flush(&mut self) -> SendMessageResult {
        let queue = &mut self.write_queue;
        let stream = &mut self.stream;

        match queue.as_mut() {
            Some(queue) => {
                match self.stream.write(queue.next_slice()) {
                    Ok(size) => {
                        queue.advance(size);
                        if queue.is_finished() {
                            let result = queue.result_ok();
                            self.write_queue.take();
                            result
                        } else {
                            queue.result_pending()
                        }
                    }
                    Err(err) => {
                        match err.kind() {
                            io::ErrorKind::WouldBlock => queue.result_pending(),
                            _ => {
                                let result = queue.result_err(err.into());
                                self.write_queue.take();
                                result
                            }
                        }
                    }
                }
            }
            None => SendMessageResult::empty(),
        }
    }

    pub fn read(&mut self) -> ReadMessageResult {
        let queue = match self.read_queue.as_mut() {
            Some(queue) => queue,
                // maybe don't dealocate and reallocate for efficiency?
            None => {
                self.read_queue.replace(ReadQueue::new());
                self.read_queue.as_mut().unwrap()
            }
        };

        loop {
            match self.stream.read(queue.next_slice()) {
                Ok(size) if size == 0 => break,
                Ok(size) => queue.advance(size),
                Err(err) => {
                    match err.kind() {
                        io::ErrorKind::WouldBlock => break,
                        _ => {
                            self.read_queue.take();
                            return ReadMessageResult::Err(ReadMessageError::IO(err));
                        }
                    }
                }
            }
        }

        if queue.is_finished() {
            let queue = self.read_queue.take().unwrap();
            match queue.take() {
                Ok(bytes) => ReadMessageResult::Ok(bytes),
                Err(err) => ReadMessageResult::Err(err.into()),
            }
        } else {
            ReadMessageResult::Pending
        }
    }

    pub fn write(&mut self, msg: SendMessage) -> SendMessageResult {
        match self.write_queue.as_mut() {
            Some(queue) => {
                SendMessageResult::err(msg.message_type(), SendMessageError::QueueFull)
            }
            None => {
                self.write_queue.replace(WriteQueue::new(msg));
                self.try_flush()
            }
        }
    }

    pub fn encrypted_write(
        &mut self,
        msg: SendMessage,
    ) -> SendMessageResult
    {
        match self.write_queue.as_mut() {
            Some(queue) => {
                SendMessageResult::err(msg.message_type(), SendMessageError::QueueFull)
            }
            None => {
                self.write_queue.replace(WriteQueue::new(msg));
                self.try_flush()
            }
        }
    }
}

struct ConnectionManager {
    counter: usize,
    poll: Poll,
    address_to_token: HashMap<PeerAddress, Token>,
    token_to_connection: HashMap<Token, Connection>,
}

impl ConnectionManager {
    fn new() -> Self {
        Self {
            counter: 0,
            poll: Poll::new().unwrap(),
            address_to_token: HashMap::new(),
            token_to_connection: HashMap::new(),
        }
    }

    fn get_by_token(&self, token: Token) -> &Connection {
        self.token_to_connection.get(&token).unwrap()
    }

    fn get_by_token_mut(&mut self, token: Token) -> &mut Connection {
        self.token_to_connection.get_mut(&token).unwrap()
    }

    fn get_or_connect_mut(&mut self, addr: &PeerAddress) -> io::Result<&mut Connection> {
        if let Some(token) = self.address_to_token.get(addr) {
            if let Some(_) = self.token_to_connection.get(token) {
                return Ok(self.token_to_connection.get_mut(token).unwrap())
            }
        }

        let counter = self.counter.wrapping_add(1);
        self.counter = counter;

        let token = Token(counter);

        self.address_to_token.insert(addr.clone(), token);

        match TcpStream::connect(addr.0.parse().unwrap()) {
            Ok(mut stream) => {
                self.poll.registry()
                    .register(&mut stream, token, Interest::READABLE | Interest::WRITABLE).unwrap();
                let conn = Connection {
                    stream,
                    address: addr.clone(),
                    read_queue: None,
                    write_queue: None,
                };
                self.token_to_connection.insert(token, conn);
                Ok(self.token_to_connection.get_mut(&token).unwrap())
            }
            Err(err) => {
                self.address_to_token.remove(addr);
                Err(err)
            }
        }
    }

    pub fn try_send_msg<M, E>(
        &mut self,
        addr: &PeerAddress,
        msg: M,
    ) -> SendMessageResult
        where M: GetMessageType + AsSendMessage<Error = E>,
              E: Debug,
    {
        let msg = match msg.as_send_message() {
            Ok(msg) => msg,
            Err(err) => {
                eprintln!("failed to encode message: {:?}", err);
                return SendMessageResult::err(
                    msg.get_message_type(),
                    SendMessageError::EncodeFailed,
                );
            }
        };
        match self.get_or_connect_mut(addr) {
            Ok(conn) => conn.write(msg),
            Err(err) => {
                SendMessageResult::err(
                    msg.get_message_type(),
                    SendMessageError::EncodeFailed,
                )
            }
        }
    }

    pub fn try_send_msg_encrypted<M, E>(
        &mut self,
        addr: &PeerAddress,
        crypto: &mut PeerCrypto,
        msg: M,
    ) -> SendMessageResult
        where M: GetMessageType + AsEncryptedSendMessage<Error = E>,
              E: Debug,
    {
        let msg = match msg.as_encrypted_send_message(crypto) {
            Ok(msg) => msg,
            Err(err) => {
                eprintln!("failed to encode message: {:?}", err);
                return SendMessageResult::err(
                    msg.get_message_type(),
                    SendMessageError::EncodeFailed,
                );
            }
        };
        match self.get_or_connect_mut(addr) {
            Ok(conn) => conn.write(msg),
            Err(err) => {
                SendMessageResult::err(
                    msg.get_message_type(),
                    SendMessageError::EncodeFailed,
                )
            }
        }
    }
}

fn handle_send_message_result(
    tezedge_state: &mut TezedgeState,
    address: PeerAddress,
    result: SendMessageResult,
) {
    use SendMessageResult::*;
    match result {
        Empty => {}
        Pending { .. } => {}
        Ok { message_type } => {
            let msg = match message_type {
                SendMessageType::Connect => HandshakeMsg::SendConnectSuccess,
                SendMessageType::Meta => HandshakeMsg::SendMetaSuccess,
                SendMessageType::Ack => HandshakeMsg::SendAckSuccess,
            };

            tezedge_state.accept(HandshakeProposal {
                at: Instant::now(),
                peer: address,
                message: msg,
            });
        }
        Err { message_type, error } => {
            dbg!(error);
            let msg = match message_type {
                SendMessageType::Connect => HandshakeMsg::SendConnectError,
                SendMessageType::Meta => HandshakeMsg::SendMetaError,
                SendMessageType::Ack => HandshakeMsg::SendAckError,
            };

            tezedge_state.accept(HandshakeProposal {
                at: Instant::now(),
                peer: address,
                message: msg,
            });
        }
    }
}

fn main() {
    let mut mgr = ConnectionManager::new();
    let mut events = Events::with_capacity(128);

    let addr = format!("0.0.0.0:{}", SERVER_PORT).parse().unwrap();

    // println!("generating identity...");
    // let node_identity = Identity::generate(ProofOfWork::DEFAULT_TARGET).unwrap();
    // dbg!(&node_identity);
    // dbg!(node_identity.secret_key.as_ref().0);

    let node_identity = identity_1();

    // println!("identity generated!");
    let mut tezedge_state = TezedgeState::new(
        TezedgeConfig {
            port: SERVER_PORT,
            disable_mempool: true,
            private_node: true,
            min_connected_peers: 500,
            max_connected_peers: 1000,
            max_pending_peers: 1000,
            max_potential_peers: 100000,
            periodic_react_interval: Duration::from_millis(250),
            peer_blacklist_duration: Duration::from_secs(30 * 60),
            peer_timeout: Duration::from_secs(8),
        },
        node_identity.clone(),
        network_version(),
        Instant::now(),
    );


    let raw_peer_addresses = HashSet::<_>::from_iter(
        [
            vec!["159.65.98.117:9732", "34.245.171.88:9732", "18.182.168.120:9732", "13.115.2.66:9732", "18.179.219.134:9732", "45.77.35.193:9732", "73.96.221.90:9732", "62.149.16.61:9732", "18.182.169.115:9732", "143.110.185.25:9732", "45.32.203.167:9732", "66.70.178.32:9732", "64.225.6.118:9732", "104.236.125.54:9732", "84.201.132.206:9732", "46.245.179.162:9733", "18.158.218.189:9732", "138.201.9.113:9735", "95.217.154.147:9732", "62.109.18.93:9732", "24.134.10.217:9732", "135.181.49.110:9732", "95.217.46.253:9732", "46.245.179.163:9732", "18.185.162.213:9732", "34.107.95.94:9732", "162.55.1.145:9732", "34.208.149.159:9732", "13.251.146.136:9732", "143.110.209.198:9732", "34.255.45.216:9732", "107.191.62.113:9732", "15.236.199.66:9732", "[::ffff:95.216.45.62]:9732", "157.90.35.112:9732", "144.76.200.188:9732", "[::ffff:18.185.162.213]:9732", "[::ffff:18.184.136.151]:9732", "[::ffff:18.195.59.36]:9732", "[::ffff:18.185.162.144]:9732", "[::ffff:18.185.78.112]:9732", "[::ffff:116.202.172.21]:9732"]
                .into_iter()
                .map(|x| x.to_owned())
                .collect(),
            // fake peers, just for testing.
            (0..10000).map(|x| format!(
                    "{}.{}.{}.{}:12345",
                    (x / 256 / 256 / 256) % 256,
                    (x / 256 / 256) % 256,
                    (x / 256) % 256,
                    x % 256,
                )).collect::<Vec<_>>(),
        ].concat().into_iter()
    );

    let _ = tezedge_state.accept(ExtendPotentialPeersProposal {
        at: Instant::now(),
        peers: raw_peer_addresses.into_iter()
            .map(|x| PeerAddress::new(x)),
    });

    let mut server = TcpListener::bind(addr).unwrap();
    mgr.poll.registry()
        .register(&mut server, SERVER, Interest::READABLE).unwrap();

    println!("starting loop");

    let bytes = tezedge_state.connection_msg().as_bytes().unwrap();
    // Start an event loop.
    loop {
        // Poll Mio for events, blocking until we get an event or before we timeout 250ms.
        mgr.poll.poll(&mut events, Some(Duration::from_millis(250))).unwrap();

        // Process each event.
        for event in events.iter() {
            // dbg!(event);
            // We can use the token we previously provided to `register` to
            // determine for which socket the event is.
            if event.token() == SERVER {
                let mut connection = server.accept().unwrap();
                drop(dbg!(connection));
                continue;
            }

            let conn = mgr.get_by_token_mut(event.token());
            // dbg!((&conn.address, event.is_readable(), event.is_writable()));

            if event.is_readable() {
                match conn.read() {
                    ReadMessageResult::Empty => {}
                    ReadMessageResult::Pending => {}
                    ReadMessageResult::Ok(msg_bytes) => {
                        tezedge_state.accept(PeerProposal {
                            at: Instant::now(),
                            peer: conn.address.clone(),
                            message: PeerBinaryMessage::new(msg_bytes),
                        });
                    }
                    ReadMessageResult::Err(err) => {
                        dbg!(err);
                        // TODO: handle somehow.
                    }
                }
            }

            if event.is_writable() {
                handle_send_message_result(
                    &mut tezedge_state,
                    conn.address.clone(),
                    conn.try_flush(),
                );
            }
        }

        tezedge_state.accept(TickProposal {
            at: Instant::now(),
        });


        for req in tezedge_state.get_requests() {
            match req {
                TezedgeRequest::SendPeerConnect { peer, message } => {
                    eprintln!("sending connection message to peer: {:?}", peer);
                    let result = dbg!(mgr.try_send_msg(&peer, message));
                    tezedge_state.accept(HandshakeProposal {
                        at: Instant::now(),
                        peer: peer.clone(),
                        message: HandshakeMsg::SendConnectPending,
                    });
                    handle_send_message_result(&mut tezedge_state, peer, result);
                }
                TezedgeRequest::SendPeerMeta { peer, message } => {
                    if let Some(crypto) = tezedge_state.get_peer_crypto(&peer) {
                        let result = mgr.try_send_msg_encrypted(&peer, crypto, message);
                        tezedge_state.accept(HandshakeProposal {
                            at: Instant::now(),
                            peer: peer.clone(),
                            message: HandshakeMsg::SendMetaPending,
                        });
                        handle_send_message_result(&mut tezedge_state, peer, result);
                    }
                }
                TezedgeRequest::SendPeerAck { peer, message } => {
                    if let Some(crypto) = tezedge_state.get_peer_crypto(&peer) {
                        let result = mgr.try_send_msg_encrypted(&peer,crypto, message);
                        tezedge_state.accept(HandshakeProposal {
                            at: Instant::now(),
                            peer: peer.clone(),
                            message: HandshakeMsg::SendAckPending,
                        });
                        handle_send_message_result(&mut tezedge_state, peer, result);
                    }
                }
                TezedgeRequest::DisconnectPeer { req_id, peer } => {
                    if let Some(token) = mgr.address_to_token.remove(&peer) {
                        mgr.token_to_connection.remove(&token);
                    }
                    tezedge_state.accept(PendingRequestProposal {
                        req_id,
                        at: Instant::now(),
                        message: PendingRequestMsg::DisconnectPeerSuccess,
                    });
                    eprintln!("peer disconnected: {:?}", peer);
                }
                TezedgeRequest::BlacklistPeer { req_id, peer } => {
                    if let Some(token) = mgr.address_to_token.remove(&peer) {
                        dbg!(mgr.token_to_connection.remove(&token));
                    }
                    tezedge_state.accept(PendingRequestProposal {
                        req_id,
                        at: Instant::now(),
                        message: PendingRequestMsg::BlacklistPeerSuccess,
                    });
                    eprintln!("peer blacklisted: {:?}", peer);
                }
            }
        }
    }
}
