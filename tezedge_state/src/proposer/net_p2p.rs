use std::time::{Instant, Duration};
use std::io::{self, Read, Write};
use std::collections::HashMap;
use slab::Slab;
use mio::net::{TcpListener, TcpStream};

use crate::{PeerCrypto, PeerAddress};
use super::*;

pub type MioEvent = mio::event::Event;
pub type MioEvents = mio::event::Events;
pub type NetPeer = Peer<TcpStream>;

pub const MIO_SERVER_TOKEN: mio::Token = mio::Token(usize::MAX);

impl P2pEvent for MioEvent {
    #[inline(always)]
    fn is_server_event(&self) -> bool {
        self.token() == MIO_SERVER_TOKEN
    }

    #[inline(always)]
    fn is_readable(&self) -> bool {
        self.is_readable()
    }

    #[inline(always)]
    fn is_writable(&self) -> bool {
        self.is_writable()
    }

    #[inline(always)]
    fn is_read_closed(&self) -> bool {
        self.is_read_closed()
    }

    #[inline(always)]
    fn is_write_closed(&self) -> bool {
        self.is_write_closed()
    }
}

impl P2pEvents for MioEvents {
    fn set_limit(&mut self, limit: usize) {
        *self = MioEvents::with_capacity(limit);
    }
}

pub struct NetP2pManager {
    server_port: u16,
    poll: mio::Poll,
    server: Option<TcpListener>,

    address_to_token: HashMap<PeerAddress, usize>,
    peers: Slab<NetPeer>,
}

impl NetP2pManager {
    pub fn new(server_port: u16) -> Self {
        Self {
            server_port,
            poll: mio::Poll::new().unwrap(),
            server: None,
            address_to_token: HashMap::new(),
            peers: Slab::new(),
        }
    }

    fn listen_on(&mut self, port: u16) {
    }
}

impl P2pManager for NetP2pManager {
    type Stream = TcpStream;
    type Event = MioEvent;
    type Events = MioEvents;

    fn start_listening_to_server_events(&mut self) {
        if self.server.is_none() {
            let mut server = TcpListener::bind(format!("0.0.0.0:{}", self.server_port).parse().unwrap()).unwrap();
            self.poll.registry()
                .register(&mut server, MIO_SERVER_TOKEN, mio::Interest::READABLE)
                .unwrap();

            self.server = Some(server);
        }
    }

    fn stop_listening_to_server_events(&mut self) {
        drop(self.server.take());
    }

    fn accept_connection(&mut self, event: &Self::Event) -> Option<&mut Peer<Self::Stream>> {
        let server = &mut self.server;
        let poll = &mut self.poll;
        let peers = &mut self.peers;
        let address_to_token = &mut self.address_to_token;

        if let Some(server) = server.as_mut() {
            match server.accept() {
                Ok((mut stream, addr)) => {
                    let address = PeerAddress::new(addr.to_string());

                    let peer_entry = peers.vacant_entry();
                    let token = mio::Token(peer_entry.key());

                    let registered_poll = poll.registry()
                        .register(&mut stream, token, mio::Interest::READABLE | mio::Interest::WRITABLE);


                    match registered_poll {
                        Ok(_) => {
                            address_to_token.insert(address.clone(), peer_entry.key());
                            Some(peer_entry.insert(NetPeer::new(address.clone(), stream)))
                        }
                        Err(err) => {
                            eprintln!("error while registering poll: {:?}", err);
                            None
                        }
                    }
                }
                Err(err) => {
                    eprintln!("error while accepting connection: {:?}", err);
                    None
                }
            }
        } else {
            None
        }
    }

    fn wait_for_events(&mut self, events: &mut Self::Events, timeout: Option<Duration>) {
        self.poll.poll(events, timeout).unwrap();
    }

    fn get_peer_for_event_mut(&mut self, event: &Self::Event) -> Option<&mut NetPeer> {
        self.peers.get_mut(event.token().into())
    }

    fn get_peer_or_connect_mut(&mut self, address: &PeerAddress) -> io::Result<&mut NetPeer> {
        if let Some(&token) = self.address_to_token.get(address) {
            if let Some(_) = self.peers.get(token) {
                return Ok(self.peers.get_mut(token).unwrap());
            }
        }

        let poll = &mut self.poll;
        let address_to_token = &mut self.address_to_token;
        let peers = &mut self.peers;

        let peer_entry = peers.vacant_entry();
        let token = mio::Token(peer_entry.key());

        match TcpStream::connect(address.0.parse().unwrap()) {
            Ok(mut stream) => {
                poll.registry()
                    .register(&mut stream, token, mio::Interest::READABLE | mio::Interest::WRITABLE)?;

                let peer = NetPeer::new(address.clone(), stream);

                address_to_token.insert(address.clone(), token.into());
                Ok(peer_entry.insert(peer))
            }
            Err(err) => {
                self.address_to_token.remove(address);
                Err(err)
            }
        }
    }

    fn disconnect_peer(&mut self, peer: &PeerAddress) {
        if let Some(token) = self.address_to_token.remove(peer) {
            self.peers.remove(token);
        }
    }
}
