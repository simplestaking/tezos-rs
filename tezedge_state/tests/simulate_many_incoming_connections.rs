use std::marker::PhantomData;
use std::time::{Instant, Duration};
use std::io::{self, Read, Write};
use std::collections::{HashMap, HashSet};
use slab::Slab;
use mio::net::{TcpListener, TcpStream};

use tezedge_state::*;
use tezedge_state::proposer::*;

type SimulatedPeer = Peer<SimulatedPeerStream>;

#[derive(Debug, Clone)]
struct SimulatedPeerStream {}

impl Read for SimulatedPeerStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        Ok(0)
    }
}

impl Write for SimulatedPeerStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        Ok(0)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
enum SimulatedEventType {
    IncomingConnect,
}

#[derive(Debug, Clone)]
struct SimulatedEvent {
    event_type: SimulatedEventType,
    time: Instant,
    from: String,
    stream: SimulatedPeerStream,
}

impl P2pEvent for SimulatedEvent {
    #[inline(always)]
    fn is_server_event(&self) -> bool {
        matches!(self.event_type, SimulatedEventType::IncomingConnect)
    }

    #[inline(always)]
    fn is_readable(&self) -> bool {
        match self.event_type {
            SimulatedEventType::IncomingConnect => false,
        }
    }

    #[inline(always)]
    fn is_writable(&self) -> bool {
        match self.event_type {
            SimulatedEventType::IncomingConnect => false,
        }
    }

    #[inline(always)]
    fn is_read_closed(&self) -> bool {
        false
    }

    #[inline(always)]
    fn is_write_closed(&self) -> bool {
        false
    }

    #[inline(always)]
    fn time(&self) -> Instant {
        self.time
    }
}

#[derive(Debug, Clone)]
struct SimulatedEvents {
    initial_time: Instant,
    range: std::ops::Range<usize>,
    limit: usize,
    last_event: Option<SimulatedEvent>,
}

impl SimulatedEvents {
    fn new(range: std::ops::Range<usize>) -> Self {
        Self {
            range,
            initial_time: Instant::now(),
            limit: 0,
            last_event: None,
        }
    }
}

impl P2pEvents for SimulatedEvents {
    fn set_limit(&mut self, limit: usize) {
        self.limit = limit;
    }
}

struct SimulatedEventsIter {
    initial_time: Instant,
    index: usize,
    limit: usize,
}

impl Iterator for SimulatedEventsIter
{
    type Item = SimulatedEvent;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.limit {
            None
        } else {
            self.index += 1;
            Some(SimulatedEvent {
                event_type: SimulatedEventType::IncomingConnect,
                time: self.initial_time + Duration::from_secs(self.index as u64),
                from: format!("peer-{}", self.index),
                stream: SimulatedPeerStream {},
            })
        }
    }
}

impl<'a> IntoIterator for &'a SimulatedEvents {
    type Item = SimulatedEvent;
    type IntoIter = SimulatedEventsIter;

    fn into_iter(self) -> Self::IntoIter {
        SimulatedEventsIter {
            initial_time: self.initial_time,
            index: self.range.start,
            limit: self.range.end,
        }
    }
}


struct SimulatedP2pManager {
    peer_count: usize,
    last_peer_index: usize,
    listener_enabled: bool,

    connected_peers: HashMap<PeerAddress, SimulatedPeer>,
}

impl SimulatedP2pManager {
    pub fn new(peer_count: usize) -> Self {
        Self {
            peer_count,
            last_peer_index: 0,
            listener_enabled: false,

            connected_peers: HashMap::new(),
        }
    }

    pub fn is_finished(&self) -> bool {
        self.last_peer_index >= self.peer_count
    }
}

impl P2pManager for SimulatedP2pManager {
    type Stream = SimulatedPeerStream;
    type Event = SimulatedEvent;
    type Events = SimulatedEvents;

    fn start_listening_to_server_events(&mut self) {
        self.listener_enabled = true;
    }

    fn stop_listening_to_server_events(&mut self) {
        self.listener_enabled = false;
    }

    fn accept_connection(&mut self, event: &Self::Event) -> Option<&mut Peer<Self::Stream>> {
        let address = PeerAddress::new(event.from.clone());
        self.connected_peers.insert(
            address.clone(),
            SimulatedPeer::new(PeerAddress(event.from.clone()), SimulatedPeerStream {}),
        );

        self.connected_peers.get_mut(&address)
    }

    fn wait_for_events(&mut self, events: &mut Self::Events, _: Option<Duration>) {
        let end = self.peer_count.min(self.last_peer_index + events.limit);
        events.range = self.last_peer_index..end;
        self.last_peer_index = end;

        eprintln!("peers attempting connection: {:?}\n", events.range);
    }

    fn get_peer_for_event_mut(&mut self, event: &Self::Event) -> Option<&mut SimulatedPeer> {
        self.connected_peers.get_mut(&PeerAddress::new(event.from.clone()))
    }

    fn get_peer_or_connect_mut(&mut self, address: &PeerAddress) -> io::Result<&mut SimulatedPeer> {
        Ok(self.connected_peers.entry(address.clone())
            .or_insert(SimulatedPeer::new(address.clone(), SimulatedPeerStream {})))
    }

    fn disconnect_peer(&mut self, peer: &PeerAddress) {
        self.connected_peers.remove(peer);
    }
}

#[test]
fn simulate_many_incoming_connections() {
    let config = TezedgeConfig {
        port: 100,
        disable_mempool: true,
        private_node: true,
        min_connected_peers: 500,
        max_connected_peers: 1000,
        max_pending_peers: 1000,
        max_potential_peers: 100000,
        periodic_react_interval: Duration::from_millis(250),
        peer_blacklist_duration: Duration::from_secs(30 * 60),
        peer_timeout: Duration::from_secs(8),
    };

    let mut proposer = TezedgeProposer::new(
        TezedgeProposerConfig {
            wait_for_events_timeout: Some(Duration::from_millis(250)),
            events_limit: 1024,
        },
        tezedge_state::sample_tezedge_state::build(config.clone()),
        // capacity is changed by events_limit.
        SimulatedEvents::new(0..0),
        SimulatedP2pManager::new(100000),
    );

    println!("starting loop");
    while !proposer.p2p_manager.is_finished() {
        proposer.make_progress_owned();
        assert!(proposer.state.pending_peers_len() <= config.max_pending_peers as usize);
        assert!(proposer.state.pending_peers_len() == proposer.p2p_manager.connected_peers.len());
    }

    dbg!(proposer.state.stats());
}
