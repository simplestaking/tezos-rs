use std::io::{Read, Write};
use std::time::Instant;
use std::collections::{HashMap, VecDeque};
use getset::{Getters, CopyGetters};

use tezos_messages::p2p::encoding::peer::{PeerMessage, PeerMessageResponse};
use tezos_messages::p2p::encoding::prelude::NetworkVersion;
use crate::chunking::{
    MessageReadBuffer, EncryptedMessageWriter,
    ReadMessageError, WriteMessageError,
};
use crate::peer_address::PeerListenerAddress;
use crate::state::pending_peers::HandshakeResult;
use crate::{PeerCrypto, PeerAddress, Port};

#[derive(Getters, CopyGetters, Debug, Clone)]
pub struct ConnectedPeer {
    // #[get = "pub"]
    pub address: PeerAddress,

    // #[get = "pub"]
    pub port: Port,

    // #[get = "pub"]
    pub version: NetworkVersion,

    // #[get = "pub"]
    pub public_key: Vec<u8>,

    pub proof_of_work_stamp: Vec<u8>,

    // #[get = "pub"]
    pub crypto: PeerCrypto,

    // #[get_copy = "pub"]
    pub disable_mempool: bool,

    // #[get_copy = "pub"]
    pub private_node: bool,

    // #[get_copy = "pub"]
    pub connected_since: Instant,

    read_buf: MessageReadBuffer,

    cur_send_message: Option<EncryptedMessageWriter>,
    send_message_queue: VecDeque<PeerMessage>,
}

impl ConnectedPeer {
    pub fn listener_port(&self) -> Port {
        self.port
    }

    pub fn listener_address(&self) -> PeerListenerAddress {
        PeerListenerAddress::new(self.address.ip(), self.port)
    }

    pub fn read_message_from<R: Read>(
        &mut self,
        reader: &mut R,
    ) -> Result<PeerMessage, ReadMessageError>
    {
        self.read_buf.read_from(reader, &mut self.crypto)
    }

    /// Enqueue message to be sent to the peer.
    pub fn enqueue_send_message(&mut self, message: PeerMessage) {
        self.send_message_queue.push_back(message);
    }

    /// Write any enqueued messages to the given writer.
    pub fn write_to<W: Write>(
        &mut self,
        writer: &mut W,
    ) -> Result<(), WriteMessageError>
    {
        if let Some(message_writer) = self.cur_send_message.as_mut() {
            message_writer.write_to(writer, &mut self.crypto)?;
            self.cur_send_message = None;
            Ok(())
        } else if let Some(message) = self.send_message_queue.pop_front() {
            self.cur_send_message = Some(
                EncryptedMessageWriter::try_new(
                    &PeerMessageResponse::from(message),
                )?
            );
            self.write_to(writer)
        } else {
            Err(WriteMessageError::Empty)
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConnectedPeers {
    // peers: slab::Slab<ConnectedPeer>,
    peers: HashMap<PeerAddress, ConnectedPeer>,
}

impl ConnectedPeers {
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            // peers: slab::Slab::with_capacity(capacity),
            peers: HashMap::with_capacity(capacity),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.peers.len()
    }

    // fn find_index(&self, address: &PeerAddress) -> Option<usize> {
    //     // TODO: use token instead of address.
    //     self.peers.iter()
    //         .find(|(_, x)| &x.address == address)
    //         .map(|(index, _)| index)
    // }

    #[inline]
    pub fn contains_address(&self, address: &PeerAddress) -> bool {
        // self.find_index(address).is_some()
        self.peers.contains_key(address)
    }

    #[inline]
    pub fn get(&self, id: &PeerAddress) -> Option<&ConnectedPeer> {
        // if let Some(index) = self.find_index(id) {
        //     self.peers.get(index)
        // } else {
        //     None
        // }
        self.peers.get(id)
    }

    #[inline]
    pub fn get_mut(&mut self, id: &PeerAddress) -> Option<&mut ConnectedPeer> {
        // if let Some(index) = self.find_index(id) {
        //     self.peers.get_mut(index)
        // } else {
        //     None
        // }
        self.peers.get_mut(id)
    }

    // #[inline]
    // pub(crate) fn insert(&mut self, peer: ConnectedPeer) -> usize {
    //     self.peers.insert(peer)
    // }

    #[inline]
    pub(crate) fn remove(&mut self, id: &PeerAddress) -> Option<ConnectedPeer> {
        // self.find_index(id)
        //     .map(|index| self.peers.remove(index))
        self.peers.remove(id)
    }

    #[inline]
    // pub fn iter(&self) -> slab::Iter<ConnectedPeer> {
    pub fn iter(&self) -> impl Iterator<Item = &ConnectedPeer> {
        self.peers.iter().map(|(_, peer)| peer)
    }

    #[inline]
    // pub fn iter_mut(&mut self) -> slab::IterMut<ConnectedPeer> {
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut ConnectedPeer> {
        self.peers.iter_mut().map(|(_, peer)| peer)
    }

    pub(crate) fn set_peer_connected(
        &mut self,
        at: Instant,
        peer_address: PeerAddress,
        result: HandshakeResult,
    ) -> &mut ConnectedPeer
    {
        // self.peers.vacant_entry().insert(ConnectedPeer {
        self.peers.entry(peer_address).or_insert_with(|| ConnectedPeer {
            connected_since: at,
            address: peer_address,
            port: result.conn_msg.port,
            version: result.conn_msg.version,
            public_key: result.conn_msg.public_key,
            proof_of_work_stamp: result.conn_msg.proof_of_work_stamp,
            crypto: result.crypto,
            disable_mempool: result.meta_msg.disable_mempool(),
            private_node: result.meta_msg.private_node(),
            read_buf: MessageReadBuffer::new(),
            cur_send_message: None,
            send_message_queue: VecDeque::new(),
        })
    }
}
