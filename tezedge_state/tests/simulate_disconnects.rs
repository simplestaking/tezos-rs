// use std::time::{Instant, Duration};
// use std::io::{self, Read, Write};
// use std::collections::{HashMap, HashSet};

// use tezedge_state::*;
// use tezedge_state::proposer::*;
// use tezos_messages::p2p::binary_message::BinaryChunk;
// use tezos_messages::p2p::encoding::ack::AckMessage;

// pub mod common;
// use common::simulator::GeneratedPeer;

// type SimulatedPeer = Peer<SimulatedPeerStream>;

// #[derive(Debug, Clone)]
// enum PeerStreamStep {
//     Empty,
//     Partial(usize),
//     Full(usize),
// }

// #[derive(Debug, Clone)]
// struct PeerStreamReader {
//     data: BinaryChunk,
//     step: PeerStreamStep,
// }

// impl PeerStreamReader {
//     fn new() -> Self {
//         Self {
//             data: BinaryChunk::from_content(&[]).unwrap(),
//             step: PeerStreamStep::Empty,
//         }
//     }

//     fn set_read_partial(&mut self) {
//         self.step = PeerStreamStep::Partial(0);
//     }

//     fn set_read_full(&mut self) {
//         self.step = PeerStreamStep::Full(self.data.raw().len() / 2);
//     }
// }

// #[derive(Debug, Clone)]
// struct PeerStreamWriter {
//     data: Vec<u8>,
//     message_type: Option<MessageType>,
//     step: PeerStreamStep,
// }

// impl PeerStreamWriter {
//     fn new() -> Self {
//         Self {
//             data: vec![],
//             message_type: None,
//             step: PeerStreamStep::Empty,
//         }
//     }

//     fn set_expected_message(&mut self, msg_type: MessageType) {
//         self.message_type = Some(msg_type);
//     }

//     fn set_write_partial(&mut self) {
//         self.step = PeerStreamStep::Partial(0);
//     }

//     fn set_write_full(&mut self) {
//         self.step = PeerStreamStep::Full(0);
//     }
// }

// #[derive(Debug, Clone)]
// struct SimulatedPeerStream {
//     peer: GeneratedPeer,
//     reader: PeerStreamReader,
//     writer: PeerStreamWriter,
// }

// impl SimulatedPeerStream {
//     fn new() -> Self {
//         Self {
//             peer: GeneratedPeer::generate(),
//             reader: PeerStreamReader::new(),
//             writer: PeerStreamWriter::new(),
//         }
//     }
// }

// impl Read for SimulatedPeerStream {
//     fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
//         let data = self.reader.data.raw();
//         let (data, index) = match &mut self.reader.step {
//             PeerStreamStep::Empty => { return Ok(0); }
//             PeerStreamStep::Partial(index) => (&data[*index..(data.len() / 2)], index),
//             PeerStreamStep::Full(index) => (&data[*index..data.len()], index),
//         };

//         let len_read = (&mut *buf).write(data)?;
//         *index = *index + len_read;

//         Ok(len_read)
//     }
// }

// impl Write for SimulatedPeerStream {
//     fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
//         let data = &mut self.writer.data;
//         let (buf, index) = match &mut self.writer.step {
//             PeerStreamStep::Empty => { return Ok(0); }
//             PeerStreamStep::Partial(index) => {
//                 let end_index = buf.len().min(data.len() / 2);
//                 (&buf[*index..end_index], index)
//             }
//             PeerStreamStep::Full(index) => (&buf[*index..buf.len()], index),
//         };
//         let len_read = data.write(buf)?;
//         *index = *index + len_read;
//         Ok(len_read)
//     }
//     fn flush(&mut self) -> io::Result<()> {
//         let writer = std::mem::replace(&mut self.writer, PeerStreamWriter::new());

//         if let Some(MessageType::Connection) = writer.message_type {
//             let conn_msg = BinaryChunk::from_raw(writer.data).unwrap();
//             // TODO: change after outgoing message is implemented as well.
//             self.peer.set_received_connection_msg(conn_msg, true);
//         }
//         Ok(())
//     }
// }

// type SimulatedEvent<'a> = EventRef<'a, SimulatedNetworkEvent>;

// #[derive(Debug, Clone)]
// enum MessageType {
//     Connection,
//     Metadata,
//     Ack,
// }

// #[derive(Debug, Clone)]
// enum NetworkEventType {
//     IncomingConnect,
//     // OutgoingConnect,

//     ReadyPartialRecv(MessageType),
//     ReadyFullRecv(MessageType),

//     ReadyPartialSend(MessageType),
//     ReadyFullSend(MessageType),

//     Disconnected,
// }

// impl NetworkEventType {
//     pub fn from_index(index: usize) -> Self {
//         match index {
//             0 => Self::IncomingConnect,
//             // 1 => Self::OutgoingConnect,

//             1 => Self::ReadyPartialRecv(MessageType::Connection),
//             2 => Self::ReadyFullRecv(MessageType::Connection),

//             3 => Self::ReadyPartialSend(MessageType::Connection),
//             4 => Self::ReadyFullSend(MessageType::Connection),

//             5 => Self::ReadyPartialRecv(MessageType::Metadata),
//             6 => Self::ReadyFullRecv(MessageType::Metadata),

//             7 => Self::ReadyPartialSend(MessageType::Metadata),
//             8 => Self::ReadyFullSend(MessageType::Metadata),

//             9 => Self::ReadyPartialRecv(MessageType::Ack),
//             10 => Self::ReadyFullRecv(MessageType::Ack),

//             11 => Self::ReadyPartialSend(MessageType::Ack),
//             12 => Self::ReadyFullSend(MessageType::Ack),
//             _ => unimplemented!(),
//         }
//     }
// }

// #[derive(Debug, Clone)]
// struct SimulatedNetworkEvent {
//     event_type: NetworkEventType,
//     time: Instant,
//     from: u64,
// }

// impl NetworkEvent for SimulatedNetworkEvent {
//     #[inline(always)]
//     fn is_server_event(&self) -> bool {
//         matches!(self.event_type, NetworkEventType::IncomingConnect)
//     }

//     #[inline(always)]
//     fn is_readable(&self) -> bool {
//         use NetworkEventType::*;
//         match self.event_type {
//             IncomingConnect => false,
//             ReadyPartialRecv(_) => true,
//             ReadyFullRecv(_) => true,
//             ReadyPartialSend(_) => false,
//             ReadyFullSend(_) => false,
//             Disconnected => false,
//         }
//     }

//     #[inline(always)]
//     fn is_writable(&self) -> bool {
//         use NetworkEventType::*;
//         match self.event_type {
//             IncomingConnect => false,
//             ReadyPartialRecv(_) => false,
//             ReadyFullRecv(_) => false,
//             ReadyPartialSend(_) => true,
//             ReadyFullSend(_) => true,
//             Disconnected => false,
//         }
//     }

//     #[inline(always)]
//     fn is_read_closed(&self) -> bool {
//         match self.event_type {
//             NetworkEventType::Disconnected => true,
//             _ => false
//         }
//     }

//     #[inline(always)]
//     fn is_write_closed(&self) -> bool {
//         false
//     }

//     #[inline(always)]
//     fn time(&self) -> Instant {
//         self.time
//     }
// }

// #[derive(Debug, Clone)]
// struct SimulatedEvents {
//     limit: usize,
//     events: Vec<Event<SimulatedNetworkEvent>>,
// }

// impl SimulatedEvents {
//     fn new() -> Self {
//         Self {
//             limit: 0,
//             events: vec![],
//         }
//     }
// }

// impl Events for SimulatedEvents {
//     fn set_limit(&mut self, limit: usize) {
//         self.limit = limit;
//     }
// }

// struct SimulatedEventsIter<'a> {
//     events: &'a [Event<SimulatedNetworkEvent>],
// }

// impl<'a> Iterator for SimulatedEventsIter<'a> {
//     type Item = SimulatedEvent<'a>;

//     fn next(&mut self) -> Option<Self::Item> {
//         if self.events.len() == 0 {
//             return None;
//         }
//         let event = &self.events[0];
//         self.events = &self.events[1..];
//         Some(event.as_event_ref())
//     }
// }

// impl<'a, > IntoIterator for &'a SimulatedEvents {
//     type Item = SimulatedEvent<'a>;
//     type IntoIter = SimulatedEventsIter<'a>;

//     fn into_iter(self) -> Self::IntoIter {
//         SimulatedEventsIter {
//             events: &self.events,
//         }
//     }
// }

// struct SimulatedManager<'a, const N: usize> {
//     listener_enabled: bool,
//     event_groups: &'a [[Event<SimulatedNetworkEvent>; N]],

//     connected_peers: HashMap<PeerAddress, SimulatedPeer>,
// }

// impl<'a, const N: usize> SimulatedManager<'a, N> {
//     pub fn new(event_groups: &'a [[Event<SimulatedNetworkEvent>; N]]) -> Self {
//         Self {
//             event_groups,
//             listener_enabled: false,

//             connected_peers: HashMap::new(),
//         }
//     }

//     pub fn is_finished(&self) -> bool {
//         self.event_groups.len() == 0
//     }
// }

// impl<'a, const N: usize> Manager for SimulatedManager<'a, N> {
//     type Stream = SimulatedPeerStream;
//     type NetworkEvent = SimulatedNetworkEvent;
//     type Events = SimulatedEvents;

//     fn start_listening_to_server_events(&mut self) {
//         self.listener_enabled = true;
//     }

//     fn stop_listening_to_server_events(&mut self) {
//         self.listener_enabled = false;
//     }

//     fn accept_connection(&mut self, event: &Self::NetworkEvent) -> Option<&mut Peer<Self::Stream>> {
//         let address = PeerAddress::ipv4_from_index(event.from);
//         self.connected_peers.insert(
//             address.clone(),
//             SimulatedPeer::new(address.clone(), SimulatedPeerStream::new()),
//         );

//         self.connected_peers.get_mut(&address)
//     }

//     fn wait_for_events(&mut self, events: &mut Self::Events, _: Option<Duration>) {
//         if self.event_groups.len() > 0 {
//             events.events = self.event_groups[0].to_vec();
//             self.event_groups = &self.event_groups[1..];
//         } else {
//             events.events = vec![];
//         }
//     }

//     fn get_peer_for_event_mut(&mut self, event: &Self::NetworkEvent) -> Option<&mut SimulatedPeer> {
//         let address = PeerAddress::ipv4_from_index(event.from);
//         if let Some(peer) = self.connected_peers.get_mut(&address) {
//             match &event.event_type {
//                 NetworkEventType::IncomingConnect => {}
//                 NetworkEventType::ReadyPartialRecv(msg_type) => {
//                     peer.stream.reader.data = match msg_type {
//                         MessageType::Connection => peer.stream.peer.connection_msg(),
//                         MessageType::Metadata => peer.stream.peer.encrypted_metadata_msg(),
//                         MessageType::Ack => peer.stream.peer.encrypted_ack_msg(AckMessage::Ack),
//                     };

//                     peer.stream.reader.set_read_partial();
//                 }
//                 NetworkEventType::ReadyFullRecv(msg_type) => {
//                     peer.stream.reader.set_read_full();
//                 }
//                 NetworkEventType::ReadyPartialSend(msg_type) => {
//                     peer.stream.writer.set_expected_message(msg_type.clone());
//                     peer.stream.writer.set_write_partial();
//                 }
//                 NetworkEventType::ReadyFullSend(msg_type) => {
//                     peer.stream.writer.set_expected_message(msg_type.clone());
//                     peer.stream.writer.set_write_full();
//                 }
//                 NetworkEventType::Disconnected => {}
//             }
//             Some(peer)
//         } else {
//             None
//         }
//     }

//     fn get_peer_or_connect_mut(&mut self, address: &PeerAddress) -> io::Result<&mut SimulatedPeer> {
//         Ok(self.connected_peers.entry(address.clone())
//             .or_insert(SimulatedPeer::new(address.clone(), SimulatedPeerStream::new())))
//     }

//     fn disconnect_peer(&mut self, peer: &PeerAddress) {
//         self.connected_peers.remove(peer);
//     }
// }

// use std::cell::RefCell;

// struct ScenarioGenerator {
//     peer_count: usize,
//     message_count: usize,
//     scenarios: RefCell<HashSet<Vec<(usize, usize)>>>,
//     count: RefCell<usize>,
// }

// impl ScenarioGenerator {
//     fn new(peer_count: usize, message_count: usize) -> Self {
//         Self { peer_count, message_count, scenarios: RefCell::new(HashSet::new()), count: RefCell::new(0) }
//     }

//     fn run_event_groups<const N: usize>(
//         &self,
//         initial_time: Instant,
//         event_groups: &[[Event<SimulatedNetworkEvent>; N]]
//     ) {
//         let num_of_disconnects = event_groups.iter()
//             .filter(|x| {
//                 if let Event::Network(event) = &x[0] {
//                     return matches!(event.event_type, NetworkEventType::Disconnected);
//                 }
//                 false
//             })
//             .count();
//         let config = TezedgeConfig {
//             port: 100,
//             disable_mempool: true,
//             private_node: true,
//             min_connected_peers: 1,
//             max_connected_peers: 2,
//             max_pending_peers: 2,
//             max_potential_peers: 10,
//             periodic_react_interval: Duration::from_millis(250),
//             peer_blacklist_duration: Duration::from_secs(30 * 60),
//             peer_timeout: Duration::from_secs(8),
//         };

//         let mut proposer = TezedgeProposer::new(
//             TezedgeProposerConfig {
//                 wait_for_events_timeout: Some(Duration::from_millis(250)),
//                 events_limit: 1024,
//             },
//             tezedge_state::sample_tezedge_state::build(initial_time, config.clone()),
//             // capacity is changed by events_limit.
//             SimulatedEvents::new(),
//             SimulatedManager::new(event_groups),
//         );

//         let mut i = 0;
//         while !proposer.manager.is_finished() {
//             proposer.make_progress();
//             let stats = proposer.state.stats();
//             assert!(stats.potential_peers_len <= config.max_potential_peers as usize);
//             assert!(stats.pending_peers_len <= config.max_pending_peers as usize);
//             assert!(stats.connected_peers_len <= config.max_connected_peers as usize);

//             assert!(proposer.manager.connected_peers.len() <= config.max_pending_peers as usize);
//             i += 1;
//         }
//         let stats = proposer.state.stats();

//         assert_eq!(stats.connected_peers_len, self.peer_count - num_of_disconnects);
//         assert_eq!(stats.blacklisted_peers_len, num_of_disconnects);
//         assert_eq!(stats.pending_peers_len, 0);
//     }

//     fn run_scenario(&self, scenario: &[Option<(usize, usize)>]) {
//         *self.count.borrow_mut() += 1;

//         let initial_time = Instant::now();

//         let disconnect_event = SimulatedNetworkEvent {
//             event_type: NetworkEventType::Disconnected,
//             from: 0,
//             time: initial_time,
//         };
//         let mut event_groups = vec![[Event::Network(disconnect_event)]];

//         event_groups.extend(scenario.iter()
//             .enumerate()
//             .filter_map(|(index, val)| {
//                 val.map(|(peer_index, message_index)| {
//                     [Event::Network(SimulatedNetworkEvent {
//                         event_type: NetworkEventType::from_index(message_index),
//                         from: peer_index as u64,
//                         time: initial_time + Duration::from_millis(100 * (index + 1) as u64),
//                     })]
//                 })
//             }));

//         for index in 0..(event_groups.len() - 1) {
//             event_groups.swap(index, index + 1);
//             if let [Event::Network(prev_event)] = &event_groups[index] {
//                 let from = prev_event.from;
//                 let time = prev_event.time;

//                 if let [Event::Network(event)] = &mut event_groups[index + 1] {
//                     event.time = time + Duration::from_millis(50);
//                     event.from = from;
//                 } else {
//                     unreachable!()
//                 }

//                 self.run_event_groups(initial_time, &event_groups);
//             }
//         }

//         // remove disconnect event.
//         event_groups.pop();
//         self.run_event_groups(initial_time, &event_groups);
//     }

//     #[inline(always)]
//     fn min_index(&self, peer_index: usize, message_index: usize) -> usize {
//         peer_index + message_index
//     }

//     #[inline(always)]
//     fn max_index(&self, peer_index: usize, message_index: usize) -> usize {
//         (self.peer_count * self.message_count) - self.message_count + message_index + 1
//     }

//     fn run_scenarios(
//         &self,
//         scenario: &mut Vec<Option<(usize, usize)>>,
//         peer_index: usize,
//         message_index: usize,
//         start_i: usize,
//     ) {
//         let min_index = start_i.max(self.min_index(peer_index, message_index));

//         for i in min_index..self.max_index(peer_index, message_index) {
//             if scenario[i].is_none() {
//                 scenario[i] = Some((peer_index, message_index));
//                 if message_index == self.message_count - 1 {
//                     if peer_index == self.peer_count - 1 {
//                         self.run_scenario(scenario);
//                     }
//                     self.run_scenarios(scenario, peer_index + 1, 0, 0);
//                 } else {
//                     self.run_scenarios(scenario, peer_index, message_index + 1, i + 1);
//                 }
//                 scenario[i] = None;
//             }
//         }
//     }

//     pub fn run(&self) {
//         self.run_scenarios(
//             &mut vec![None; self.peer_count * self.message_count],
//             0,
//             0,
//             0,
//         );
//         dbg!((self.count.borrow(), self.scenarios.borrow().len()));
//     }
// }

// #[test]
// fn simulate_many_incoming_connections() {
//     ScenarioGenerator::new(2, 13).run();
// }
