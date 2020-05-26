// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

//! Manages connected peers.

use std::cmp;
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use dns_lookup::LookupError;
use futures::lock::Mutex;
use rand::seq::SliceRandom;
use riker::actors::*;
use slog::{debug, info, Logger, warn};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Handle;
use tokio::time::timeout;

use networking::p2p::network_channel::{NetworkChannelMsg, NetworkChannelRef, NetworkChannelTopic, PeerBootstrapped, PeerCreated};
use networking::p2p::peer::{Bootstrap, Peer, PeerRef, SendMessage};
use tezos_api::identity::Identity;
use tezos_messages::p2p::encoding::prelude::*;

use crate::shell_channel::{ShellChannelMsg, ShellChannelRef};
use crate::subscription::*;

/// Timeout for outgoing connections
const CONNECT_TIMEOUT: Duration = Duration::from_secs(8);
/// Whitelist all IP addresses after 30 minutes
const WHITELIST_INTERVAL: Duration = Duration::from_secs(1_800);
/// How often to do DNS peer discovery
const DISCOVERY_INTERVAL: Duration = Duration::from_secs(60);
/// Limit how often we allow to trigger check of a peer count
const CHECK_PEER_COUNT_LIMIT: Duration = Duration::from_secs(5);

/// Check peer threshold
#[derive(Clone, Debug)]
pub struct CheckPeerCount;

/// Whitelist all IP address.
#[derive(Clone, Debug)]
pub struct WhitelistAllIpAddresses;

/// Accept incoming peer connection.
#[derive(Clone, Debug)]
pub struct AcceptPeer {
    stream: Arc<Mutex<Option<TcpStream>>>,
    address: SocketAddr,
}

/// Open connection to the remote peer node.
#[derive(Clone, Debug)]
pub struct ConnectToPeer {
    pub address: SocketAddr
}

/// Simple threshold, for representing integral ranges.
#[derive(Copy, Clone, Debug)]
pub struct Threshold {
    low: usize,
    high: usize,
}

impl Threshold {
    /// Create new threshold, by specifying mnimum and maximum (inclusively).
    ///
    /// # Arguments
    /// * `low` - Lower threshold bound
    /// * `higher` - Upper threshold bound
    ///
    /// `low` cannot be bigger than `high`, otherwise function will panic
    pub fn new(low: usize, high: usize) -> Self {
        assert!(low <= high, "low must be less than or equal to high");
        Threshold { low, high }
    }
}

/// This actor is responsible for peer management.
///
/// It monitors number of connected peers. If the number of connected peers is too low it tries to
/// connect to more peers. If the number of connected peers is too high, then randomly selected peers
/// are disconnected.
#[actor(CheckPeerCount, WhitelistAllIpAddresses, AcceptPeer, ConnectToPeer, NetworkChannelMsg, ShellChannelMsg, SystemEvent, DeadLetter)]
pub struct PeerManager {
    /// All events generated by the network layer will end up in this channel
    network_channel: NetworkChannelRef,
    /// All events from shell will be published to this channel
    shell_channel: ShellChannelRef,
    /// Peer count threshold
    threshold: Threshold,
    /// Map of all peers
    peers: HashMap<ActorUri, PeerState>,
    /// DNS addresses used for bootstrapping
    bootstrap_addresses: Vec<String>,
    /// List of initial peers to connect to
    initial_peers: HashSet<SocketAddr>,
    /// List of potential peers to connect to
    potential_peers: HashSet<SocketAddr>,
    /// Tokio runtime
    tokio_executor: Handle,
    /// We will listen for incoming connection at this port
    listener_port: u16,
    /// Tezos identity
    identity: Identity,
    /// Protocol version
    protocol_version: String,
    /// Message receiver boolean indicating whether
    /// more connections should be accepted from network
    rx_run: Arc<AtomicBool>,
    /// set of blacklisted IP addresses
    ip_blacklist: HashSet<IpAddr>,
    /// Last time we did DNS peer discovery
    discovery_last: Option<Instant>,
    /// Last time we checked peer count
    check_peer_count_last: Option<Instant>,
    /// Indicates that system is shutting down
    shutting_down: bool,
}

/// Reference to [peer manager](PeerManager) actor.
pub type PeerManagerRef = ActorRef<PeerManagerMsg>;

impl PeerManager {
    pub fn actor(sys: &impl ActorRefFactory,
                 network_channel: NetworkChannelRef,
                 shell_channel: ShellChannelRef,
                 tokio_executor: Handle,
                 bootstrap_addresses: &[String],
                 initial_peers: &[SocketAddr],
                 threshold: Threshold,
                 listener_port: u16,
                 identity: Identity,
                 protocol_version: String,
    ) -> Result<PeerManagerRef, CreateError> {
        sys.actor_of(
            Props::new_args(PeerManager::new, (
                network_channel,
                shell_channel,
                tokio_executor,
                bootstrap_addresses.to_vec(),
                HashSet::from_iter(initial_peers.to_vec()),
                threshold,
                listener_port,
                identity,
                protocol_version)),
            PeerManager::name())
    }

    /// The `PeerManager` is intended to serve as a singleton actor so that's why
    /// we won't support multiple names per instance.
    fn name() -> &'static str {
        "peer-manager"
    }

    fn new((network_channel, shell_channel, tokio_executor, bootstrap_addresses, initial_peers, threshold, listener_port, identity, protocol_version):
           (NetworkChannelRef, ShellChannelRef, Handle, Vec<String>, HashSet<SocketAddr>, Threshold, u16, Identity, String)) -> Self {
        PeerManager {
            network_channel,
            shell_channel,
            tokio_executor,
            bootstrap_addresses,
            initial_peers,
            threshold,
            listener_port,
            identity,
            protocol_version,
            rx_run: Arc::new(AtomicBool::new(true)),
            potential_peers: HashSet::new(),
            peers: HashMap::new(),
            ip_blacklist: HashSet::new(),
            discovery_last: None,
            check_peer_count_last: None,
            shutting_down: false,
        }
    }

    /// Try to discover new remote peers to connect
    fn discover_peers(&mut self, log: Logger) {
        if self.peers.is_empty() || self.discovery_last.filter(|discovery_last| discovery_last.elapsed() <= DISCOVERY_INTERVAL).is_none() {
            self.discovery_last = Some(Instant::now());

            info!(log, "Doing peer DNS lookup"; "bootstrap_addresses" => format!("{:?}", &self.bootstrap_addresses));
            dns_lookup_peers(&self.bootstrap_addresses, &log).iter()
                .for_each(|address| {
                    if !self.is_blacklisted(&address.ip()) {
                        info!(log, "Found potential peer"; "address" => address);
                        self.potential_peers.insert(*address);
                    }
                });

            if self.potential_peers.is_empty() {
                info!(log, "Using initial peers as a potential peers"; "initial_peers" => format!("{:?}", &self.initial_peers));
                // DNS discovery yield no results, use initial peers
                self.potential_peers.extend(&self.initial_peers);
            }
        } else {
            self.peers.values()
                .for_each(|peer_state| peer_state.peer_ref.tell(SendMessage::new(PeerMessage::Bootstrap.into()), None));
        }
    }

    /// Create new peer actor
    fn create_peer(&mut self, sys: &impl ActorRefFactory, socket_address: &SocketAddr) -> PeerRef {
        let peer = Peer::actor(
            sys,
            self.network_channel.clone(),
            self.listener_port,
            &self.identity.public_key,
            &self.identity.secret_key,
            &self.identity.proof_of_work_stamp,
            &self.protocol_version,
            self.tokio_executor.clone(),
            socket_address
        ).unwrap();

        self.peers.insert(peer.uri().clone(), PeerState { peer_ref: peer.clone(), address: socket_address.clone() });

        self.network_channel.tell(
            Publish {
                msg: PeerCreated {
                    peer: peer.clone(),
                    address: *socket_address,
                }.into(),
                topic: NetworkChannelTopic::NetworkEvents.into(),
            }, None);

        peer
    }

    /// Check if given ip address is blacklisted to connect to
    fn is_blacklisted(&self, ip_address: &IpAddr) -> bool {
        self.ip_blacklist.contains(ip_address)
    }

    fn process_shell_channel_message(&mut self, ctx: &Context<PeerManagerMsg>, msg: ShellChannelMsg) -> Result<(), failure::Error> {
        match msg {
            ShellChannelMsg::ShuttingDown(_) => {
                self.shutting_down = true;
                unsubscribe_from_dead_letters(ctx.system.dead_letters(), ctx.myself());
            }
            _ => ()
        }

        Ok(())
    }

    fn trigger_check_peer_count(&mut self, ctx: &Context<PeerManagerMsg>) {
        let should_trigger = self.check_peer_count_last
            .map(|check_peer_count_last| check_peer_count_last.elapsed() > CHECK_PEER_COUNT_LIMIT)
            .unwrap_or(true);

        if should_trigger {
            self.check_peer_count_last = Some(Instant::now());
            ctx.myself().tell(CheckPeerCount, None);
        }
    }

    fn process_potential_peers(&mut self, potential_peers: &[String]) {
        let sock_addresses = potential_peers.iter()
            .filter_map(|str_ip_port| str_ip_port.parse().ok())
            .filter(|address: &SocketAddr| !self.is_blacklisted(&address.ip()))
            .collect::<Vec<_>>();
        self.potential_peers.extend(sock_addresses);
    }
}

impl Actor for PeerManager {
    type Msg = PeerManagerMsg;

    fn pre_start(&mut self, ctx: &Context<Self::Msg>) {
        subscribe_to_actor_terminated(ctx.system.sys_events(), ctx.myself());
        subscribe_to_network_events(&self.network_channel, ctx.myself());
        subscribe_to_shell_events(&self.shell_channel, ctx.myself());
        subscribe_to_dead_letters(ctx.system.dead_letters(), ctx.myself());

        ctx.schedule::<Self::Msg, _>(
            Duration::from_secs(3),
            Duration::from_secs(10),
            ctx.myself(),
            None,
            CheckPeerCount.into());
        ctx.schedule::<Self::Msg, _>(
            WHITELIST_INTERVAL,
            WHITELIST_INTERVAL,
            ctx.myself(),
            None,
            WhitelistAllIpAddresses.into());


        let listener_port = self.listener_port;
        let myself = ctx.myself();
        let rx_run = self.rx_run.clone();

        // start to listen for incoming p2p connections
        self.tokio_executor.spawn(async move {
            begin_listen_incoming(listener_port, myself, rx_run).await;
        });
    }

    fn post_stop(&mut self) {
        self.rx_run.store(false, Ordering::Relaxed);
    }

    fn sys_recv(&mut self, ctx: &Context<Self::Msg>, msg: SystemMsg, sender: Option<BasicActorRef>) {
        if let SystemMsg::Event(evt) = msg {
            self.receive(ctx, evt, sender);
        }
    }

    fn recv(&mut self, ctx: &Context<Self::Msg>, msg: Self::Msg, sender: Sender) {
        self.receive(ctx, msg, sender);
    }
}

impl Receive<DeadLetter> for PeerManager {
    type Msg = PeerManagerMsg;

    fn receive(&mut self, _ctx: &Context<Self::Msg>, msg: DeadLetter, _sender: Option<BasicActorRef>) {
        self.peers.remove(msg.recipient.uri());
    }
}


impl Receive<ShellChannelMsg> for PeerManager {
    type Msg = PeerManagerMsg;

    fn receive(&mut self, ctx: &Context<Self::Msg>, msg: ShellChannelMsg, _sender: Sender) {
        match self.process_shell_channel_message(ctx, msg) {
            Ok(_) => (),
            Err(e) => warn!(ctx.system.log(), "Failed to process shell channel message"; "reason" => format!("{:?}", e)),
        }
    }
}

impl Receive<SystemEvent> for PeerManager {
    type Msg = PeerManagerMsg;

    fn receive(&mut self, ctx: &Context<Self::Msg>, msg: SystemEvent, _sender: Option<BasicActorRef>) {
        if let SystemEvent::ActorTerminated(evt) = msg {
            if let Some(_) = self.peers.remove(evt.actor.uri()) {
                self.trigger_check_peer_count(ctx);
            }
        }
    }
}

impl Receive<CheckPeerCount> for PeerManager {
    type Msg = PeerManagerMsg;

    fn receive(&mut self, ctx: &Context<Self::Msg>, _msg: CheckPeerCount, _sender: Sender) {
        // received message instructs this actor to check whether number of connected peers is within desired bounds
        if self.shutting_down {
            return;
        }

        if self.peers.len() < self.threshold.low {
            // peer count is too low, try to connect to more peers
            warn!(ctx.system.log(), "Peer count is too low"; "actual" => self.peers.len(), "required" => self.threshold.low);
            if self.potential_peers.len() < self.threshold.low {
                self.discover_peers(ctx.system.log());
            }

            let num_required_peers = cmp::max((self.threshold.high + 3 * self.threshold.low) / 4 - self.peers.len(), self.threshold.low);
            let mut addresses_to_connect = self.potential_peers.iter().cloned().collect::<Vec<SocketAddr>>();
            // randomize peers as a security measurement
            addresses_to_connect.shuffle(&mut rand::thread_rng());
            addresses_to_connect
                .drain(0..cmp::min(num_required_peers, addresses_to_connect.len()))
                .for_each(|address| {
                    self.potential_peers.remove(&address);
                    ctx.myself().tell(ConnectToPeer { address }, ctx.myself().into())
                });
        } else if self.peers.len() > self.threshold.high {
            // peer count is too high, disconnect some peers
            warn!(ctx.system.log(), "Peer count is too high. Some peers will be stopped"; "actual" => self.peers.len(), "limit" => self.threshold.high);

            // stop some peers
            self.peers.values()
                .take(self.peers.len() - self.threshold.high)
                .for_each(|peer_state| ctx.system.stop(peer_state.peer_ref.clone()))
        }

        self.check_peer_count_last = Some(Instant::now());
    }
}

impl Receive<NetworkChannelMsg> for PeerManager {
    type Msg = PeerManagerMsg;

    fn receive(&mut self, ctx: &Context<Self::Msg>, msg: NetworkChannelMsg, _sender: Sender) {
        match msg {
            NetworkChannelMsg::PeerMessageReceived(received) => {
                // received message containing additional peers to which we can connect in the future
                let messages = received.message.messages();
                messages.iter()
                    .for_each(|message| match message {
                        PeerMessage::Advertise(message) => {
                            // extract potential peers from the advertise message
                            info!(ctx.system.log(), "Received advertise message"; "peer" => received.peer.name());
                            self.process_potential_peers(message.id());
                        }
                        PeerMessage::Bootstrap => {
                            // to a bootstrap message we will respond with list of potential peers
                            info!(ctx.system.log(), "Received bootstrap message"; "peer" => received.peer.name());
                            let addresses = self.peers.values()
                                .into_iter()
                                .filter(|peer_state| peer_state.peer_ref != received.peer)
                                .map(|peer_state| peer_state.address)
                                .collect::<Vec<_>>();
                            let msg = AdvertiseMessage::new(&addresses);
                            received.peer.tell(SendMessage::new(PeerMessage::Advertise(msg).into()), None);
                        }
                        _ => {}
                    });
                self.trigger_check_peer_count(ctx);
            }
            NetworkChannelMsg::PeerBootstrapped(PeerBootstrapped::Failure { address, potential_peers_to_connect }) => {
                // received message that bootstrap process failed for the peer
                match potential_peers_to_connect {
                    Some(peers) => {
                        info!(ctx.system.log(), "Received list of potential peers in the NACK message"; "ip" => format!("{}", address.ip()), "peers" => format!("{:?}", &peers));
                        self.process_potential_peers(&peers);
                        self.trigger_check_peer_count(ctx);
                    }
                    None => {
                        info!(ctx.system.log(), "Blacklisting IP because peer failed at bootstrap process"; "ip" => format!("{}", address.ip()));
                        self.ip_blacklist.insert(address.ip());
                    }
                }
            }
            _ => ()
        }
    }
}

impl Receive<WhitelistAllIpAddresses> for PeerManager {
    type Msg = PeerManagerMsg;

    fn receive(&mut self, ctx: &Context<Self::Msg>, _msg: WhitelistAllIpAddresses, _sender: Sender) {
        info!(ctx.system.log(), "Whitelisting all IP addresses");
        self.ip_blacklist.clear();
    }
}

impl Receive<ConnectToPeer> for PeerManager {
    type Msg = PeerManagerMsg;

    fn receive(&mut self, ctx: &Context<Self::Msg>, msg: ConnectToPeer, _sender: Sender) {
        // received message instructing this actor that it should open new p2p connection to the remote peer

        if self.is_blacklisted(&msg.address.ip()) {
            debug!(ctx.system.log(), "Peer is blacklisted - will not connect"; "ip" => format!("{}", msg.address.ip()));
        } else {
            let peer = self.create_peer(ctx, &msg.address);
            let system = ctx.system.clone();

            self.tokio_executor.spawn(async move {
                info!(system.log(), "Connecting to IP"; "ip" => msg.address, "peer" => peer.name());
                match timeout(CONNECT_TIMEOUT, TcpStream::connect(&msg.address)).await {
                    Ok(Ok(stream)) => {
                        info!(system.log(), "Connection successful"; "ip" => msg.address);
                        peer.tell(Bootstrap::outgoing(stream, msg.address), None);
                    }
                    Ok(Err(e)) => {
                        info!(system.log(), "Connection failed"; "ip" => msg.address, "peer" => peer.name(), "reason" => format!("{:?}", e));
                        system.stop(peer);
                    }
                    Err(_) => {
                        info!(system.log(), "Connection timed out"; "ip" => msg.address, "peer" => peer.name());
                        system.stop(peer);
                    }
                }
            });
        }
    }
}

impl Receive<AcceptPeer> for PeerManager {
    type Msg = PeerManagerMsg;

    fn receive(&mut self, ctx: &Context<Self::Msg>, msg: AcceptPeer, _sender: Sender) {
        if self.is_blacklisted(&msg.address.ip()) {
            debug!(ctx.system.log(), "Peer is blacklisted - will not accept connection"; "ip" => format!("{}", msg.address.ip()));
        } else if self.peers.len() < self.threshold.high {
            info!(ctx.system.log(), "Connection from"; "ip" => msg.address);
            let peer = self.create_peer(ctx, &msg.address);
            peer.tell(Bootstrap::incoming(msg.stream, msg.address), None);
        } else {
            debug!(ctx.system.log(), "Cannot accept incoming peer connection because peer limit was reached");
            drop(msg.stream); // not needed, just wanted to be explicit here
        }
    }
}

/// Start to listen for incoming connections indefinitely.
async fn begin_listen_incoming(listener_port: u16, peer_manager: PeerManagerRef, rx_run: Arc<AtomicBool>) {
    let listener_address = format!("0.0.0.0:{}", listener_port).parse::<SocketAddr>().expect("Failed to parse listener address");
    let mut listener = TcpListener::bind(&listener_address).await.expect("Failed to bind to address");

    while rx_run.load(Ordering::Acquire) {
        if let Ok((stream, address)) = listener.accept().await {
            peer_manager.tell(AcceptPeer { stream: Arc::new(Mutex::new(Some(stream))), address }, None);
        }
    }
}

/// Do DNS lookup for collection of names and create collection of socket addresses
fn dns_lookup_peers(bootstrap_addresses: &[String], log: &Logger) -> HashSet<SocketAddr> {
    let mut resolved_peers = HashSet::new();
    for address in bootstrap_addresses {
        match resolve_dns_name_to_peer_address(&address) {
            Ok(peers) => {
                resolved_peers.extend(&peers)
            }
            Err(e) => {
                warn!(log, "DNS lookup failed"; "ip" => address, "reason" => format!("{:?}", e))
            }
        }
    }
    resolved_peers
}

/// Try to resolve common peer name into Socket Address representation
fn resolve_dns_name_to_peer_address(address: &str) -> Result<Vec<SocketAddr>, LookupError> {
    let addrs = dns_lookup::getaddrinfo(Some(address), Some("9732"), None)?
        .filter(Result::is_ok)
        .map(Result::unwrap)
        .map(|mut info| {
            info.sockaddr.set_port(9732);
            info.sockaddr
        })
        .collect();
    Ok(addrs)
}

/// Holds information about a specific peer.
struct PeerState {
    /// Reference to peer actor
    peer_ref: PeerRef,
    /// Peer IP address
    address: SocketAddr
}
