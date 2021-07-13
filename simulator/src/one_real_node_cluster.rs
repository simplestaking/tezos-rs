use std::collections::VecDeque;
use std::fmt::Debug;
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use crypto::crypto_box::CryptoKey;
use crypto::crypto_box::PrecomputedKey;
use crypto::crypto_box::PublicKey;
use crypto::nonce::Nonce;
use crypto::nonce::generate_nonces;
use tezedge_state::DefaultEffects;
use tezedge_state::PeerAddress;
use tezedge_state::PeerCrypto;
use tezedge_state::TezedgeState;
use tezedge_state::chunking::ChunkReadBuffer;
use tezedge_state::chunking::EncryptedMessageWriter;
use tezedge_state::chunking::MessageReadBuffer;
use tezedge_state::proposals::ExtendPotentialPeersProposal;
use tezedge_state::proposer::{Event, EventRef};
use tezedge_state::proposer::{Events, Manager, Peer};
use tezedge_state::proposer::NetworkEvent;
use tezedge_state::proposer::TezedgeProposer;
use tezedge_state::proposer::TezedgeProposerConfig;
use tezos_identity::Identity;
use tezos_messages::p2p::binary_message::BinaryChunk;
use tezos_messages::p2p::binary_message::BinaryRead;
use tezos_messages::p2p::binary_message::BinaryWrite;
use tezos_messages::p2p::encoding::ack::AckMessage;
use tezos_messages::p2p::encoding::ack::NackInfo;
use tezos_messages::p2p::encoding::connection::ConnectionMessage;
use tezos_messages::p2p::encoding::peer::PeerMessage;
use tezos_messages::p2p::encoding::peer::PeerMessageResponse;
use tezos_messages::p2p::encoding::prelude::MetadataMessage;
use tezos_messages::p2p::encoding::prelude::NetworkVersion;
use tla_sm::Acceptor;

/// Events with time difference of less than given duration will be
/// grouped together.
pub const EVENT_GROUP_BOUNDARY: Duration = Duration::from_millis(1);

#[derive(Debug)]
pub enum ConnectToNodeError {
    NodeNotListening,
}

#[derive(Debug)]
pub enum HandshakeError {
    NotConnected,
    NackV0,
    Nack(NackInfo),
}

pub type FakeEvent = Event<FakeNetworkEvent>;
pub type FakeEventRef<'a> = EventRef<'a, FakeNetworkEvent>;

#[derive(Debug, Clone)]
pub enum FakeNetworkEventType {
    IncomingConnection,
    Disconnected,

    BytesWritable(Option<usize>),
    // ByteChunksWritable(Vec<usize>),

    BytesReadable(Option<usize>),
    // ByteChunksReadable(Vec<usize>),
}

#[derive(Debug, Clone)]
pub struct FakeNetworkEvent {
    time: Instant,
    from: FakePeerId,
    event_type: FakeNetworkEventType
}

impl NetworkEvent for FakeNetworkEvent {
    fn is_server_event(&self) -> bool {
        matches!(&self.event_type, FakeNetworkEventType::IncomingConnection)
    }

    fn is_readable(&self) -> bool {
        use FakeNetworkEventType::*;
        matches!(&self.event_type, BytesReadable(_))
        // matches!(&self.event_type, BytesReadable(_) | ByteChunksReadable(_))
    }

    fn is_writable(&self) -> bool {
        use FakeNetworkEventType::*;
        matches!(&self.event_type, BytesWritable(_))
        // matches!(&self.event_type, BytesWritable(_) | ByteChunksWritable(_))
    }

    fn is_read_closed(&self) -> bool {
        matches!(&self.event_type, FakeNetworkEventType::Disconnected)
    }

    fn is_write_closed(&self) -> bool {
        false
    }

    fn time(&self) -> Instant {
        self.time
    }
}

/// Fake Events.
///
/// Events with time difference of less than a [EVENT_GROUP_BOUNDARY]
/// will be grouped together.
///
/// # Panics
///
/// Time in the events must be ascending or it will panic.
#[derive(Debug, Clone)]
pub struct FakeEvents {
    events: Vec<FakeEvent>,
    limit: usize,
}

impl FakeEvents {
    pub fn new() -> Self {
        Self {
            events: vec![],
            // will be later set from proposer using [Events::set_limit].
            limit: 0,
        }
    }

    /// Try to push the event if we didn't accumulate more events than
    /// the set `limit`. If we did, passed event will be return back
    /// as a `Result::Err(FakeEvent)`.
    pub fn try_push(&mut self, event: FakeEvent) -> Result<&mut Self, FakeEvent> {
        if self.events.len() >= self.limit {
            Err(event)
        } else {
            self.events.push(event);
            Ok(self)
        }
    }
}

impl Events for FakeEvents {
    fn set_limit(&mut self, limit: usize) {
        self.limit = limit;
    }
}

#[derive(Debug, Clone)]
pub struct FakeEventsIter<'a> {
    events: &'a [FakeEvent],
}

impl<'a> Iterator for FakeEventsIter<'a> {
    type Item = FakeEventRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.events.len() == 0 {
            return None;
        }
        let event = &self.events[0];
        self.events = &self.events[1..];
        Some(event.as_event_ref())
    }
}

impl<'a, > IntoIterator for &'a FakeEvents {
    type Item = FakeEventRef<'a>;
    type IntoIter = FakeEventsIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        FakeEventsIter {
            events: &self.events,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub struct FakePeerId {
    id: usize,
}

impl From<FakePeerId> for PeerAddress {
    fn from(id: FakePeerId) -> Self {
        PeerAddress::ipv4_from_index(id.id as u64)
    }
}

impl From<FakePeerId> for SocketAddr {
    fn from(id: FakePeerId) -> Self {
        PeerAddress::from(id).into()
    }
}

impl From<PeerAddress> for FakePeerId {
    fn from(addr: PeerAddress) -> Self {
        Self {
            id: addr.to_index() as usize,
        }
    }
}

impl From<&PeerAddress> for FakePeerId {
    fn from(addr: &PeerAddress) -> Self {
        Self {
            id: addr.to_index() as usize,
        }
    }
}

pub type FakePeer = Peer<FakePeerStream>;

#[derive(Debug, Clone)]
pub enum ConnectedState {
    /// Disconnected.
    Disconnected,
    /// Incoming connection to the fake peer from real node.
    Incoming(bool),
    /// Outgoing connection from the fake peer to real node.
    Outgoing(bool),
}

impl ConnectedState {
    pub fn is_connected(&self) -> bool {
        match self {
            Self::Disconnected => false,
            Self::Incoming(is_connected) => *is_connected,
            Self::Outgoing(is_connected) => *is_connected,
        }
    }

    fn disconnect(&mut self) {
        *self = Self::Disconnected;
    }

    fn to_incoming(&mut self) {
        match self {
            Self::Incoming(_) => {}
            Self::Disconnected => *self = Self::Incoming(false),
            Self::Outgoing(_) => {}
        }
    }

    fn to_outgoing(&mut self) {
        match self {
            Self::Outgoing(_) => {}
            Self::Disconnected => *self = Self::Outgoing(false),
            Self::Incoming(_) => {}
        }
    }
}

#[derive(Debug, Clone)]
pub struct FakePeerStream {
    conn_state: ConnectedState,
    identity: Identity,
    crypto: Option<PeerCrypto>,
    read_buf: VecDeque<u8>,
    read_limit: Option<usize>,
    write_buf: VecDeque<u8>,
    write_limit: Option<usize>,
}

impl FakePeerStream {
    pub fn new(pow_target: f64) -> Self {
        Self {
            conn_state: ConnectedState::Disconnected,
            identity: Identity::generate(pow_target).unwrap(),
            crypto: None,
            read_buf: VecDeque::new(),
            read_limit: Some(0),
            write_buf: VecDeque::new(),
            write_limit: Some(0),
        }
    }

    pub fn read_buf(&self) -> &VecDeque<u8> {
        &self.read_buf
    }

    pub fn write_buf(&self) -> &VecDeque<u8> {
        &self.write_buf
    }

    pub fn is_connected(&self) -> bool {
        self.conn_state.is_connected()
    }

    fn disconnect(&mut self) {
        self.conn_state.disconnect()
    }

    fn set_read_limit(&mut self, limit: Option<usize>) {
        self.read_limit = limit;
    }

    fn set_write_limit(&mut self, limit: Option<usize>) {
        self.write_limit = limit;
    }

    fn set_limit_from_event(&mut self, event: &FakeNetworkEvent) {
        match &event.event_type {
            FakeNetworkEventType::BytesReadable(limit) => {
                if limit.is_none() {
                    self.read_limit = None;
                } else {
                    self.read_limit = self.read_limit.max(limit.clone());
                }
            }
            FakeNetworkEventType::BytesWritable(limit) => {
                if limit.is_none() {
                    self.write_limit = None;
                }
                self.write_limit = self.write_limit.max(limit.clone());
            }
            _ => {}
        }
    }

    pub fn send_bytes(&mut self, bytes: &[u8]) {
        self.read_buf.extend(bytes);
    }

    /// Send connection message to real node.
    pub fn send_conn_msg(&mut self, conn_msg: &ConnectionMessage) {
        self.send_bytes(BinaryChunk::from_content(&conn_msg.as_bytes().unwrap()).unwrap().raw());
    }

    pub fn read_conn_msg(&mut self) -> ConnectionMessage {
        let mut reader = ChunkReadBuffer::new();
        while !reader.is_finished() {
            reader.read_from(&mut VecDequeReadable::from(&mut self.write_buf)).unwrap();
        }
        ConnectionMessage::from_bytes(reader.take_if_ready().unwrap().content()).unwrap()
    }

    pub fn send_meta_msg(&mut self, meta_msg: &MetadataMessage) {
        let crypto = self.crypto.as_mut().expect("missing PeerCrypto for encryption");
        let mut encrypted_msg_writer = EncryptedMessageWriter::try_new(meta_msg).unwrap();
        encrypted_msg_writer.write_to_extendable(&mut self.read_buf, crypto).unwrap();
    }

    pub fn read_meta_msg(&mut self) -> MetadataMessage {
        let crypto = self.crypto.as_mut().expect("missing PeerCrypto for encryption");
        let mut reader = ChunkReadBuffer::new();
        while !reader.is_finished() {
            reader.read_from(&mut VecDequeReadable::from(&mut self.write_buf)).unwrap();
        }
        let bytes = crypto.decrypt(&reader.take_if_ready().unwrap().content()).unwrap();
        MetadataMessage::from_bytes(bytes).unwrap()
    }

    pub fn send_ack_msg(&mut self, ack_msg: &AckMessage) {
        let crypto = self.crypto.as_mut().expect("missing PeerCrypto for encryption");
        let mut encrypted_msg_writer = EncryptedMessageWriter::try_new(ack_msg).unwrap();
        encrypted_msg_writer.write_to_extendable(&mut self.read_buf, crypto).unwrap();
    }

    pub fn read_ack_message(&mut self) -> AckMessage {
        let crypto = self.crypto.as_mut().expect("missing PeerCrypto for encryption");
        let mut reader = ChunkReadBuffer::new();
        while !reader.is_finished() {
            reader.read_from(&mut VecDequeReadable::from(&mut self.write_buf)).unwrap();
        }
        let bytes = crypto.decrypt(&reader.take_if_ready().unwrap().content()).unwrap();
        AckMessage::from_bytes(bytes).unwrap()
    }

    pub fn send_peer_message(&mut self, peer_message: PeerMessage) {
        let crypto = self.crypto.as_mut().expect("missing PeerCrypto for encryption");
        let resp = PeerMessageResponse::from(peer_message);
        let mut encrypted_msg_writer = EncryptedMessageWriter::try_new(&resp).unwrap();
        encrypted_msg_writer.write_to_extendable(&mut self.read_buf, crypto).unwrap();
    }

    pub fn read_peer_message(&mut self) -> Option<PeerMessage> {
        let crypto = self.crypto.as_mut().expect("missing PeerCrypto for encryption");
        if self.write_buf.len() == 0 {
            return None;
        }
        let mut reader = MessageReadBuffer::new();
        Some(reader.read_from(&mut VecDequeReadable::from(&mut self.write_buf), crypto).unwrap())
    }
}

impl Read for FakePeerStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        assert_ne!(buf.len(), 0, "empty(len = 0) buffer shouldn't be passed to read method");
        let len = match &self.read_limit {
            Some(limit) => buf.len().min(*limit),
            None => buf.len(),
        };
        // eprintln!("read {}. limit: {:?}", len, &self.read_limit);
        if len == 0 {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "limit reached"));
        }

        VecDequeReadable::from(&mut self.read_buf).read(&mut buf[..len])
    }
}

impl Write for FakePeerStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = match &self.write_limit {
            Some(limit) => buf.len().min(*limit),
            None => buf.len(),
        };
        // eprintln!("write {}. limit: {:?}", len, &self.write_limit);
        if len == 0 {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "limit reached"));
        }

        self.write_buf.extend(&buf[..len]);

        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct VecDequeReadable<'a> {
    v: &'a mut VecDeque<u8>,
}

impl<'a> Read for VecDequeReadable<'a> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.v.len().min(buf.len());
        for (i, v) in self.v.drain(..len).enumerate() {
            buf[i] = v;
        }
        Ok(len)
    }
}

impl<'a> From<&'a mut VecDeque<u8>> for VecDequeReadable<'a> {
    fn from(v: &'a mut VecDeque<u8>) -> Self {
        Self { v }
    }
}

#[derive(Debug, Clone)]
pub struct OneRealNodeManager {
    listening: bool,
    pow_target: f64,
    wait_for_events_timeout: Option<Duration>,

    fake_peers: Vec<Peer<FakePeerStream>>,
    events: VecDeque<FakeEvent>,
    last_event_time: Option<Instant>,
}

impl OneRealNodeManager {
    fn new(pow_target: f64, wait_for_events_timeout: Option<Duration>) -> Self {
        Self {
            listening: false,
            pow_target,
            wait_for_events_timeout,
            fake_peers: vec![],
            events: VecDeque::new(),
            last_event_time: None,
        }
    }

    /// Panics if peer with such id is not found.
    fn get_mut(&mut self, id: FakePeerId) -> &mut FakePeer {
        &mut self.fake_peers[id.id]
    }

    fn init_new_fake_peer(&mut self) -> FakePeerId {
        let peer_id = FakePeerId { id: self.fake_peers.len() };
        self.fake_peers.push(Peer::new(
            peer_id.into(),
            FakePeerStream::new(self.pow_target),
        ));
        peer_id
    }

    fn push_event(&mut self, event: FakeEvent) {
        self.events.push_back(event);
    }
}

impl Manager for OneRealNodeManager {
    type Stream = FakePeerStream;
    type NetworkEvent = FakeNetworkEvent;
    type Events = FakeEvents;

    fn start_listening_to_server_events(&mut self) {
        self.listening = true;
    }
    fn stop_listening_to_server_events(&mut self) {
        self.listening = false;
    }

    fn accept_connection(&mut self, event: &Self::NetworkEvent) -> Option<&mut Peer<Self::Stream>> {
        let peer = self.get_mut(event.from);
        match &mut peer.stream.conn_state {
            conn_state @ ConnectedState::Disconnected
            | conn_state @ ConnectedState::Outgoing(false)
            => {
                *conn_state = ConnectedState::Outgoing(true);
                Some(peer)
            }
            _ => None,
        }
    }

    fn wait_for_events(&mut self, events_container: &mut Self::Events, timeout: Option<Duration>) {
        events_container.events.clear();

        let first_event_time = match self.events.get(0) {
            Some(e) => e.time(),
            None => return,
        };

        // simulate tick events.
        if let Some(&last_event_time) = self.last_event_time.as_ref() {
            if let Some(&wait_for_events_timeout) = self.wait_for_events_timeout.as_ref() {
                let time = last_event_time + wait_for_events_timeout;

                if time < first_event_time {
                    if let Ok(_) = events_container.try_push(FakeEvent::Tick(time)) {
                        self.last_event_time = Some(time);
                    }
                    return;
                }
            }
        }

        while let Some(event) = self.events.pop_front() {
            let event_time = event.time();
            if event_time.duration_since(first_event_time) > EVENT_GROUP_BOUNDARY {
                self.events.push_front(event);
                return;
            }
            if let Err(event) = events_container.try_push(event.clone()) {
                self.events.push_front(event);
                return;
            }
            self.last_event_time = Some(event_time);
            match event {
                Event::Network(event) => {
                    self.get_mut(event.from).stream.set_limit_from_event(&event);
                }
                Event::Tick(_) => {}
            }
        }
    }

    fn get_peer(&mut self, address: &PeerAddress) -> Option<&mut Peer<Self::Stream>> {
        Some(self.get_mut(address.into()))
    }

    fn get_peer_or_connect_mut(&mut self, address: &PeerAddress) -> io::Result<&mut Peer<Self::Stream>> {
        let peer = self.get_mut(address.into());
        match &mut peer.stream.conn_state {
            conn_state @ ConnectedState::Disconnected
            | conn_state @ ConnectedState::Incoming(false)
            => {
                *conn_state = ConnectedState::Incoming(true);
                Ok(peer)
            }
            ConnectedState::Outgoing(true) => Err(io::Error::new(io::ErrorKind::Other, "unexpected fake peer conn_state: ConnectedState::Outgoing!")),
            conn_state => Err(io::Error::new(io::ErrorKind::Other, format!("unexpected fake peer conn_state: {:?}!", conn_state))),
        }
    }

    fn get_peer_for_event_mut(&mut self, event: &Self::NetworkEvent) -> Option<&mut Peer<Self::Stream>> {
        Some(self.get_mut(event.from))
    }

    fn disconnect_peer(&mut self, peer: &PeerAddress) {
        self.get_mut(peer.into()).stream.disconnect();
    }
}

#[derive(Clone)]
pub struct OneRealNodeCluster {
    time: Instant,
    proposer: TezedgeProposer<FakeEvents, DefaultEffects, OneRealNodeManager>,
}

impl OneRealNodeCluster {
    pub fn new(
        initial_time: Instant,
        proposer_config: TezedgeProposerConfig,
        state: TezedgeState,
    ) -> OneRealNodeCluster {
        let wait_for_events_timeout = proposer_config.wait_for_events_timeout;
        let pow_target = state.config().pow_target;
        let mut proposer = TezedgeProposer::new(
            proposer_config,
            state,
            FakeEvents::new(),
            OneRealNodeManager::new(
                pow_target,
                wait_for_events_timeout,
            ),
        );
        // needed to start listening for incoming connections.
        proposer.make_progress();

        Self {
            time: initial_time,
            proposer,
        }
    }

    pub fn proposer(&self) -> &TezedgeProposer<FakeEvents, DefaultEffects, OneRealNodeManager> {
        &self.proposer
    }

    pub fn advance_time(&mut self, by: Duration) -> &mut Self {
        self.time += by;
        self
    }

    /// Whether events queue is empty.
    pub fn is_done(&self) -> bool {
        self.proposer.manager.events.is_empty()
    }

    pub fn make_progress(&mut self) -> &mut Self {
        self.proposer.make_progress();
        self
    }

    pub fn assert_state(&mut self) -> &mut Self {
        self.proposer.state.assert_state();
        self
    }

    fn _extend_node_potential_peers<I>(&mut self, peers: I)
        where I: Debug + IntoIterator<Item = SocketAddr>,
    {
        self.proposer.state.accept(
            ExtendPotentialPeersProposal { at: self.time, peers }
        )
    }

    fn extend_node_potential_peers<I>(&mut self, peers: I)
        where I: IntoIterator<Item = FakePeerId>,
              I::IntoIter: Debug,
    {
        self._extend_node_potential_peers(peers.into_iter().map(|x| x.into()))
    }

    pub fn init_new_fake_peer(&mut self) -> FakePeerId {
        self.proposer.manager.init_new_fake_peer()
    }

    pub fn get_peer(&mut self, peer_id: FakePeerId) -> &mut FakePeerStream {
        &mut self.proposer.manager.get_mut(peer_id).stream
    }

    pub fn is_peer_connected(&mut self, peer_id: FakePeerId) -> bool {
        self.get_peer(peer_id).is_connected()
    }

    /// Connect to node if not already connected.
    pub fn connect_to_node(&mut self, from: FakePeerId) -> Result<&mut Self, ConnectToNodeError> {
        if self.proposer.manager.listening == false {
            return Err(ConnectToNodeError::NodeNotListening);
        }

        self.proposer.manager.get_mut(from).stream.conn_state.to_outgoing();
        self.proposer.manager.push_event(FakeNetworkEvent {
            time: self.time,
            from,
            event_type: FakeNetworkEventType::IncomingConnection,
        }.into());

        Ok(self)
    }

    /// Connect to fake peer from node if maximum number of outgoing
    /// connections isn't reached for node.
    pub fn connect_from_node(&mut self, to: FakePeerId) -> &mut Self {
        self.extend_node_potential_peers(std::iter::once(to));
        self.proposer.manager.get_mut(to).stream.conn_state.to_incoming();

        self
    }

    pub fn disconnect_peer(&mut self, peer_id: FakePeerId) -> &mut Self {
        self.proposer.manager.events.push_back(Event::Network(FakeNetworkEvent {
            time: self.time,
            from: peer_id,
            event_type: FakeNetworkEventType::Disconnected,
        }));

        self
    }

    pub fn add_readable_event(&mut self, peer_id: FakePeerId, read_limit: Option<usize>) -> &mut Self {
        self.proposer.manager.events.push_back(Event::Network(FakeNetworkEvent {
            time: self.time,
            from: peer_id,
            event_type: FakeNetworkEventType::BytesReadable(read_limit),
        }));

        self
    }

    pub fn add_writable_event(&mut self, peer_id: FakePeerId, write_limit: Option<usize>) -> &mut Self {
        self.proposer.manager.events.push_back(Event::Network(FakeNetworkEvent {
            time: self.time,
            from: peer_id,
            event_type: FakeNetworkEventType::BytesWritable(write_limit),
        }));

        self
    }

    pub fn do_handshake(&mut self, peer_id: FakePeerId) -> Result<&mut Self, HandshakeError> {
        let peer = &mut self.get_peer(peer_id);
        peer.write_limit = None;
        peer.read_limit = None;

        let incoming = match &peer.conn_state {
            ConnectedState::Incoming(_) => true,
            ConnectedState::Outgoing(_) => false,
            ConnectedState::Disconnected => return Err(HandshakeError::NotConnected),
        };

        let sent_conn_msg = ConnectionMessage::try_new(
            12345,
            &peer.identity.public_key,
            &peer.identity.proof_of_work_stamp,
            Nonce::random(),
            NetworkVersion::new("TEZOS_MAINNET".to_owned(), 0, 1)
        ).unwrap();

        peer.send_conn_msg(&sent_conn_msg);
        self.add_readable_event(peer_id, None)
            .add_writable_event(peer_id, None)
            .make_progress();
        dbg!(&self.proposer.state);

        let conn_msg = self.get_peer(peer_id).read_conn_msg();

        let nonce_pair = generate_nonces(
            &BinaryChunk::from_content(&sent_conn_msg.as_bytes().unwrap()).unwrap().raw(),
            &BinaryChunk::from_content(&conn_msg.as_bytes().unwrap()).unwrap().raw(),
            incoming,
        ).unwrap();

        let peer = self.get_peer(peer_id);

        let precomputed_key = PrecomputedKey::precompute(
            &PublicKey::from_bytes(&conn_msg.public_key()).unwrap(),
            &peer.identity.secret_key,
        );

        let crypto = PeerCrypto::new(precomputed_key, nonce_pair);
        peer.crypto = Some(crypto);

        peer.send_meta_msg(&MetadataMessage::new(false, false));
        self.add_readable_event(peer_id, None)
            .add_writable_event(peer_id, None)
            .make_progress();

        let peer = self.get_peer(peer_id);
        peer.read_meta_msg();

        peer.send_ack_msg(&AckMessage::Ack);
        self.add_readable_event(peer_id, None)
            .add_writable_event(peer_id, None)
            .make_progress();

        let peer = self.get_peer(peer_id);
        match peer.read_ack_message() {
            AckMessage::Ack => Ok(self),
            AckMessage::NackV0 => Err(HandshakeError::NackV0),
            AckMessage::Nack(nack_info) => Err(HandshakeError::Nack(nack_info)),
        }
    }

    pub fn send_bytes_to_node(&mut self, from: FakePeerId, bytes: &[u8]) -> &mut Self {
        self.get_peer(from).send_bytes(bytes);
        self
    }

    pub fn send_message_to_node(&mut self, from: FakePeerId, message: PeerMessage) {
        unimplemented!()
    }

    pub fn read_message_from_node(&mut self, message_for: FakePeerId) -> PeerMessage {
        unimplemented!()
    }

    pub fn add_tick_for_current_time(&mut self) {
        self.proposer.manager.events.push_back(Event::Tick(self.time));
    }
}

#[cfg(test)]
mod tests {
    use tezedge_state::{TezedgeConfig, sample_tezedge_state};

    use super::*;

    #[test]
    fn test_can_handshake_incoming() {
        let initial_time = Instant::now();
        let mut cluster = OneRealNodeCluster::new(
            initial_time,
            TezedgeProposerConfig {
                wait_for_events_timeout: Some(Duration::from_millis(250)),
                events_limit: 1024,
            },
            sample_tezedge_state::build(initial_time, TezedgeConfig {
                port: 9732,
                disable_mempool: true,
                private_node: true,
                min_connected_peers: 1,
                max_connected_peers: 100,
                max_pending_peers: 100,
                max_potential_peers: 1000,
                periodic_react_interval: Duration::from_millis(250),
                peer_blacklist_duration: Duration::from_secs(15 * 60),
                peer_timeout: Duration::from_secs(8),
                // use high number to speed up identity generation.
                pow_target: 1.0,
            }),
        );

        let peer_id = cluster.init_new_fake_peer();

        cluster
            .connect_to_node(peer_id).unwrap()
            .make_progress()
            .do_handshake(peer_id).unwrap()
            .make_progress();
    }

    #[test]
    fn test_can_handshake_outgoing() {
        let initial_time = Instant::now();
        let mut cluster = OneRealNodeCluster::new(
            initial_time,
            TezedgeProposerConfig {
                wait_for_events_timeout: Some(Duration::from_millis(250)),
                events_limit: 1024,
            },
            sample_tezedge_state::build(initial_time, TezedgeConfig {
                port: 9732,
                disable_mempool: true,
                private_node: true,
                min_connected_peers: 1,
                max_connected_peers: 100,
                max_pending_peers: 100,
                max_potential_peers: 1000,
                periodic_react_interval: Duration::from_millis(250),
                peer_blacklist_duration: Duration::from_secs(15 * 60),
                peer_timeout: Duration::from_secs(8),
                // use high number to speed up identity generation.
                pow_target: 1.0,
            }),
        );

        let peer_id = cluster.init_new_fake_peer();

        cluster
            .connect_from_node(peer_id)
            .make_progress()
            .do_handshake(peer_id).unwrap()
            .make_progress();
    }
}
