use std::time::{Instant, Duration};
use std::io::{self, Read, Write};
use std::fmt::Debug;
use bytes::Buf;

use tla_sm::{Acceptor, GetRequests};

use tezos_messages::p2p::binary_message::{BinaryMessage, BinaryChunk, BinaryChunkError, CONTENT_LENGTH_FIELD_BYTES};
use tezos_messages::p2p::encoding::prelude::{
    ConnectionMessage, MetadataMessage, AckMessage,
};
use crate::{TezedgeState, TezedgeRequest, PeerCrypto, PeerAddress};
use crate::proposals::{
    TickProposal,
    NewPeerConnectProposal,
    PeerProposal,
    PeerDisconnectProposal,
    PendingRequestProposal, PendingRequestMsg,
    HandshakeProposal, HandshakeMsg,
};
use crate::proposals::peer_message::PeerBinaryMessage;

pub mod mio_manager;

pub trait GetMessageType {
    fn get_message_type(&self) -> SendMessageType;
}

pub trait AsSendMessage {
    type Error;

    fn as_send_message(&self) -> Result<SendMessage, Self::Error>;
}

pub trait AsEncryptedSendMessage {
    type Error;

    fn as_encrypted_send_message(
        &self,
        crypto: &mut PeerCrypto,
    ) -> Result<SendMessage, Self::Error>;
}

#[derive(Debug, Clone, Copy)]
pub enum SendMessageType {
    Connect,
    Meta,
    Ack,
}

#[derive(Debug)]
pub enum SendMessageError {
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
pub enum SendMessageResult {
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
pub enum ReadMessageError {
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
pub enum ReadMessageResult {
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
pub struct SendMessage {
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

pub enum Event<NetE> {
    Tick(Instant),
    Network(NetE),
}

type EventRef<'a, NetE> = Event<&'a NetE>;

pub trait NetworkEvent {
    fn is_server_event(&self) -> bool;

    fn is_readable(&self) -> bool;
    fn is_writable(&self) -> bool;

    fn is_read_closed(&self) -> bool;
    fn is_write_closed(&self) -> bool;

    fn time(&self) -> Instant {
        Instant::now()
    }
}

pub trait Events {
    fn set_limit(&mut self, limit: usize);
}

pub struct Peer<S> {
    address: PeerAddress,
    stream: S,
    read_queue: Option<ReadQueue>,
    write_queue: Option<WriteQueue>,
}

impl<S> Peer<S> {
    pub fn address(&self) -> &PeerAddress {
        &self.address
    }

    pub fn new(address: PeerAddress, stream: S) -> Self {
        Self {
            address,
            stream,
            read_queue: None,
            write_queue: None,
        }
    }
}

impl<S: Read> Peer<S> {
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

}

impl<S: Write> Peer<S> {
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
}

pub trait Manager {
    type Stream: Read + Write;
    type NetworkEvent: NetworkEvent;
    type Events;

    fn start_listening_to_server_events(&mut self);
    fn stop_listening_to_server_events(&mut self);

    fn accept_connection(&mut self, event: &Self::NetworkEvent) -> Option<&mut Peer<Self::Stream>>;

    fn wait_for_events(&mut self, events_container: &mut Self::Events, timeout: Option<Duration>);

    fn get_peer_or_connect_mut(&mut self, address: &PeerAddress) -> io::Result<&mut Peer<Self::Stream>>;
    fn get_peer_for_event_mut(&mut self, event: &Self::NetworkEvent) -> Option<&mut Peer<Self::Stream>>;

    fn disconnect_peer(&mut self, peer: &PeerAddress);

    fn try_send_msg<M, E>(
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
        match self.get_peer_or_connect_mut(addr) {
            Ok(conn) => conn.write(msg),
            Err(err) => {
                SendMessageResult::err(
                    msg.get_message_type(),
                    SendMessageError::EncodeFailed,
                )
            }
        }
    }

    fn try_send_msg_encrypted<M, E>(
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
        match self.get_peer_or_connect_mut(addr) {
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

pub struct TezedgeProposerConfig {
    pub wait_for_events_timeout: Option<Duration>,
    pub events_limit: usize,
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

pub struct TezedgeProposer<Es, M> {
    config: TezedgeProposerConfig,
    pub state: TezedgeState,
    pub events: Es,
    pub manager: M,
}

impl<Es, M> TezedgeProposer<Es, M>
    where Es: Events,
{
    pub fn new(
        config: TezedgeProposerConfig,
        state: TezedgeState,
        mut events: Es,
        manager: M,
    ) -> Self
    {
        events.set_limit(config.events_limit);
        Self {
            config,
            state,
            events,
            manager,
        }
    }
}

impl<S, NetE, Es, M> TezedgeProposer<Es, M>
    where S: Read + Write,
          NetE: NetworkEvent,
          M: Manager<Stream = S, NetworkEvent = NetE, Events = Es>,
{
    fn handle_event(
        event: Event<NetE>,
        state: &mut TezedgeState,
        manager: &mut M,
    ) {
        match event {
            Event::Tick(at) => {
                state.accept(TickProposal { at });
            }
            Event::Network(event) => {
                Self::handle_network_event(&event, state, manager);
            }
        }
    }

    fn handle_event_ref<'a>(
        event: EventRef<'a, NetE>,
        state: &mut TezedgeState,
        manager: &mut M,
    ) {
        match event {
            Event::Tick(at) => {
                state.accept(TickProposal { at });
            }
            Event::Network(event) => {
                Self::handle_network_event(event, state, manager);
            }
        }
    }

    fn handle_network_event(
        event: &NetE,
        state: &mut TezedgeState,
        manager: &mut M,
    ) {
        let peer = if event.is_server_event() {
            // we received event for the server (client opened tcp stream to us).
            match manager.accept_connection(&event) {
                Some(peer) => {
                    state.accept(NewPeerConnectProposal {
                        at: event.time(),
                        peer: peer.address().clone(),
                    });
                    peer
                }
                None => return,
            }
        } else {
            match manager.get_peer_for_event_mut(&event) {
                Some(peer) => peer,
                None => {
                    // TODO: replace with just error log.
                    unreachable!()
                }
            }
        };

        if event.is_read_closed() || event.is_write_closed() {
            state.accept(PeerDisconnectProposal {
                at: event.time(),
                peer: peer.address().clone(),
            });
            return;
        }

        if event.is_readable() {
            match peer.read() {
                ReadMessageResult::Empty => {}
                ReadMessageResult::Pending => {}
                ReadMessageResult::Ok(msg_bytes) => {
                    state.accept(PeerProposal {
                        at: Instant::now(),
                        peer: peer.address().clone(),
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
                state,
                peer.address().clone(),
                peer.try_flush(),
            );
        }
    }

    fn execute_requests(state: &mut TezedgeState, manager: &mut M) {
        for req in state.get_requests() {
            match req {
                TezedgeRequest::StartListeningForNewPeers { req_id } => {
                    manager.start_listening_to_server_events();
                    state.accept(PendingRequestProposal {
                        req_id,
                        at: state.newest_time_seen(),
                        message: PendingRequestMsg::StartListeningForNewPeersSuccess,
                    });
                }
                TezedgeRequest::StopListeningForNewPeers { req_id } => {
                    manager.stop_listening_to_server_events();
                    state.accept(PendingRequestProposal {
                        req_id,
                        at: state.newest_time_seen(),
                        message: PendingRequestMsg::StopListeningForNewPeersSuccess,
                    });
                }
                TezedgeRequest::SendPeerConnect { peer, message } => {
                    let result = manager.try_send_msg(&peer, message);
                    state.accept(HandshakeProposal {
                        at: state.newest_time_seen(),
                        peer: peer.clone(),
                        message: HandshakeMsg::SendConnectPending,
                    });
                    handle_send_message_result(state, peer, result);
                }
                TezedgeRequest::SendPeerMeta { peer, message } => {
                    if let Some(crypto) = state.get_peer_crypto(&peer) {
                        let result = manager.try_send_msg_encrypted(&peer, crypto, message);
                        state.accept(HandshakeProposal {
                            at: state.newest_time_seen(),
                            peer: peer.clone(),
                            message: HandshakeMsg::SendMetaPending,
                        });
                        handle_send_message_result(state, peer, result);
                    }
                }
                TezedgeRequest::SendPeerAck { peer, message } => {
                    if let Some(crypto) = state.get_peer_crypto(&peer) {
                        let result = manager.try_send_msg_encrypted(&peer,crypto, message);
                        state.accept(HandshakeProposal {
                            at: state.newest_time_seen(),
                            peer: peer.clone(),
                            message: HandshakeMsg::SendAckPending,
                        });
                        handle_send_message_result(state, peer, result);
                    }
                }
                TezedgeRequest::DisconnectPeer { req_id, peer } => {
                    manager.disconnect_peer(&peer);
                    state.accept(PendingRequestProposal {
                        req_id,
                        at: state.newest_time_seen(),
                        message: PendingRequestMsg::DisconnectPeerSuccess,
                    });
                }
                TezedgeRequest::BlacklistPeer { req_id, peer } => {
                    manager.disconnect_peer(&peer);
                    state.accept(PendingRequestProposal {
                        req_id,
                        at: state.newest_time_seen(),
                        message: PendingRequestMsg::BlacklistPeerSuccess,
                    });
                }
            }
        }
    }

    fn wait_for_events(&mut self) {
        let wait_for_events_timeout = self.config.wait_for_events_timeout;
        self.manager.wait_for_events(&mut self.events, wait_for_events_timeout)
    }

    pub fn make_progress(&mut self)
        where for<'a> &'a Es: IntoIterator<Item = EventRef<'a, NetE>>,
    {
        self.wait_for_events();

        let events_limit = self.config.events_limit;

        for event in self.events.into_iter().take(events_limit) {
            Self::handle_event_ref(
                event,
                &mut self.state,
                &mut self.manager,
            );
        }

        Self::execute_requests(&mut self.state, &mut self.manager);
    }

    pub fn make_progress_owned(&mut self)
        where for<'a> &'a Es: IntoIterator<Item = Event<NetE>>,
    {
        let time = Instant::now();
        self.wait_for_events();
        eprintln!("waited for events for: {}ms", time.elapsed().as_millis());

        let events_limit = self.config.events_limit;

        let time = Instant::now();
        let mut count = 0;
        for event in self.events.into_iter().take(events_limit) {
            count += 1;
            Self::handle_event(
                event,
                &mut self.state,
                &mut self.manager,
            );
        }
        eprintln!("handled {} events in: {}ms", count, time.elapsed().as_millis());

        let time = Instant::now();
        Self::execute_requests(&mut self.state, &mut self.manager);
        eprintln!("executed requests in: {}ms", time.elapsed().as_millis());
    }
}
