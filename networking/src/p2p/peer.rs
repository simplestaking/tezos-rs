use std::convert::TryFrom;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use failure::{Error, Fail};
use futures::lock::Mutex;
use log::{trace, debug, info, warn};
use riker::actors::*;
use tokio::net::TcpStream;
use tokio::runtime::TaskExecutor;

use crypto::crypto_box::precompute;
use crypto::nonce::{self, Nonce, NoncePair};

use super::binary_message::{BinaryMessage, RawBinaryMessage};
use super::encoding::prelude::*;
use super::network_channel::{NetworkChannelMsg, NetworkChannelTopic, PeerBootstrapped, PeerCreated, PeerMessageReceived};
use super::stream::{EncryptedMessageReader, EncryptedMessageWriter, MessageStream, StreamError};

static ACTOR_ID_GENERATOR: AtomicU64 = AtomicU64::new(0);

pub type PeerId = String;
pub type PublicKey = Vec<u8>;

#[derive(Debug, Fail)]
enum PeerError {
    #[fail(display = "Received NACK from remote peer")]
    NackReceived,
    #[fail(display = "Failed to create precomputed key")]
    FailedToPrecomputeKey,
    #[fail(display = "Network error: {}", message)]
    NetworkError {
        error: Error,
        message: &'static str,
    },
    #[fail(display = "Message serialization error")]
    SerializationError {
        error: tezos_encoding::ser::Error
    },
    #[fail(display = "Message deserialization error")]
    DeserializationError {
        error: tezos_encoding::de::Error
    },
}

impl From<tezos_encoding::ser::Error> for PeerError {
    fn from(error: tezos_encoding::ser::Error) -> Self {
        PeerError::SerializationError { error }
    }
}

impl From<tezos_encoding::de::Error> for PeerError {
    fn from(error: tezos_encoding::de::Error) -> Self {
        PeerError::DeserializationError { error }
    }
}

impl From<std::io::Error> for PeerError {
    fn from(error: std::io::Error) -> Self {
        PeerError::NetworkError { error: error.into(), message: "Network error" }
    }
}

impl From<StreamError> for PeerError {
    fn from(error: StreamError) -> Self {
        PeerError::NetworkError { error: error.into(), message: "Stream error" }
    }
}


#[derive(Clone, Debug)]
pub struct Bootstrap {
    stream: Arc<Mutex<Option<TcpStream>>>,
    address: SocketAddr,
    incoming: bool,
}

impl Bootstrap {
    pub fn incoming(stream: Arc<Mutex<Option<TcpStream>>>, address: SocketAddr) -> Self {
        Bootstrap { stream, address, incoming: true }
    }

    pub fn outgoing(stream: TcpStream, address: SocketAddr) -> Self {
        Bootstrap { stream: Arc::new(Mutex::new(Some(stream))), address, incoming: false }
    }
}

#[derive(Clone, Debug)]
pub struct SendMessage {
    /// Message is wrapped in `Arc` to avoid excessive cloning.
    message: Arc<PeerMessageResponse>
}

impl SendMessage {
    pub fn new(msg: PeerMessageResponse) -> Self {
        SendMessage { message: Arc::new(msg) }
    }
}

#[derive(Clone)]
struct Network {
    /// Message receiver boolean indicating whether
    /// more messages should be received from network
    rx_run: Arc<AtomicBool>,
    /// Message sender
    tx: Arc<Mutex<Option<EncryptedMessageWriter>>>,
}

/// Local node info
pub struct Local {
    /// port where remote node can establish new connection
    listener_port: u16,
    /// our public key
    public_key: String,
    /// our secret key
    secret_key: String,
    /// proof of work
    proof_of_work_stamp: String,
}

pub type PeerRef = ActorRef<PeerMsg>;

#[actor(Bootstrap, SendMessage)]
pub struct Peer {
    /// All events generated by the peer will end up in this channel
    event_channel: ChannelRef<NetworkChannelMsg>,
    /// Local node info
    local: Arc<Local>,
    /// Network IO
    net: Network,
    /// Tokio task executor
    tokio_executor: TaskExecutor,
}

impl Peer {
    pub fn new(sys: &impl ActorRefFactory,
               event_channel: ChannelRef<NetworkChannelMsg>,
               listener_port: u16,
               public_key: &String,
               secret_key: &String,
               proof_of_work_stamp: &String,
               tokio_executor: TaskExecutor) -> Result<PeerRef, CreateError>
    {
        let info = Local {
            listener_port: listener_port.clone(),
            proof_of_work_stamp: proof_of_work_stamp.clone(),
            public_key: public_key.clone(),
            secret_key: secret_key.clone(),
        };
        let props = Props::new_args(Peer::actor, (event_channel, Arc::new(info), tokio_executor));
        let actor_id = ACTOR_ID_GENERATOR.fetch_add(1, Ordering::SeqCst);
        sys.actor_of(props, &format!("peer-{}", actor_id))
    }

    fn actor((event_channel, info, tokio_executor): (ChannelRef<NetworkChannelMsg>, Arc<Local>, TaskExecutor)) -> Self {
        Peer {
            event_channel,
            local: info,
            net: Network {
                rx_run: Arc::new(AtomicBool::new(false)),
                tx: Arc::new(Mutex::new(None)),
            },
            tokio_executor,
        }
    }
}

impl Actor for Peer {
    type Msg = PeerMsg;

    fn pre_start(&mut self, ctx: &Context<Self::Msg>) {
        self.event_channel.tell(Publish { msg: PeerCreated { peer: ctx.myself() }.into(), topic: NetworkChannelTopic::NetworkEvents.into() }, None);
    }

    fn post_stop(&mut self) {
        self.net.rx_run.store(false, Ordering::Relaxed);
    }

    fn recv(&mut self, ctx: &Context<Self::Msg>, msg: Self::Msg, sender: Sender) {
        // Use the respective Receive<T> implementation
        self.receive(ctx, msg, sender);
    }
}

impl Receive<Bootstrap> for Peer {
    type Msg = PeerMsg;

    fn receive(&mut self, ctx: &Context<Self::Msg>, msg: Bootstrap, _sender: Sender) {
        let info = self.local.clone();
        let myself = ctx.myself();
        let system = ctx.system.clone();
        let net = self.net.clone();
        let event_channel = self.event_channel.clone();

        self.tokio_executor.spawn(async move {
            async fn setup_net(net: &Network, tx: EncryptedMessageWriter) {
                net.rx_run.store(true, Ordering::Relaxed);
                *net.tx.lock().await = Some(tx);
            }

            match bootstrap(msg, info).await {
                Ok(BootstrapOutput(rx, tx)) => {
                    setup_net(&net, tx).await;

                    event_channel.tell(Publish { msg: PeerBootstrapped { peer: myself.clone() }.into(), topic: NetworkChannelTopic::NetworkEvents.into() }, Some(myself.clone().into()));
                    // begin to process incoming messages in a loop
                    begin_process_incoming(rx, net.rx_run, myself.clone(), event_channel).await;
                    // connection to peer was closed, stop this actor
                    system.stop(myself);
                }
                Err(e) => {
                    warn!("Connection to peer failed: {}", e);
                    system.stop(myself);
                }
            }
        });
    }
}

impl Receive<SendMessage> for Peer {
    type Msg = PeerMsg;

    fn receive(&mut self, ctx: &Context<Self::Msg>, msg: SendMessage, _sender: Sender) {
        let system = ctx.system.clone();
        let myself = ctx.myself();
        let tx = self.net.tx.clone();
        self.tokio_executor.spawn(async move {
            let mut tx_lock = tx.lock().await;
            let tx = tx_lock.as_mut();
            if let Err(e) = tx.unwrap().write_message(&*msg.message).await {
                warn!("Failed to send message. {:?}", e);
                system.stop(myself);
            }
        });
    }
}

/// Output values of the successful bootstrap process
struct BootstrapOutput(EncryptedMessageReader, EncryptedMessageWriter);

async fn bootstrap(msg: Bootstrap, info: Arc<Local>) -> Result<BootstrapOutput, PeerError> {
    let (mut msg_rx, mut msg_tx) = {
        let stream = msg.stream.lock().await.take().expect("Someone took ownership of the socket before the Peer");
        let msg_reader: MessageStream = stream.into();
        msg_reader.split()
    };

    // send connection message
    let connection_message = ConnectionMessage::new(
        info.listener_port,
        &info.public_key,
        &info.proof_of_work_stamp,
        &Nonce::random().get_bytes(),
        vec![supported_version()]);
    let connection_message_sent = {
        let as_bytes = connection_message.as_bytes()?;
        match msg_tx.write_message(&as_bytes).await {
            Ok(bytes) => bytes,
            Err(e) => return Err(PeerError::NetworkError { error: e.into(), message: "Failed to transfer connection message" })
        }
    };

    // receive connection message
    let received_connection_msg = match msg_rx.read_message().await {
        Ok(msg) => msg,
        Err(e) => return Err(PeerError::NetworkError { error: e.into(), message: "Receive no response to our connection message" })
    };
    // generate local and remote nonce
    let NoncePair { local: nonce_local, remote: nonce_remote } = generate_nonces(&connection_message_sent, &received_connection_msg, msg.incoming);

    // convert received bytes from remote peer into `ConnectionMessage`
    let received_connection_msg: ConnectionMessage = ConnectionMessage::try_from(received_connection_msg)?;
    let peer_public_key = received_connection_msg.get_public_key();
    let peer_id = hex::encode(&peer_public_key);
    debug!("Received peer_public_key: {}", &peer_id);

    // pre-compute encryption key
    let precomputed_key = match precompute(&hex::encode(peer_public_key), &info.secret_key) {
        Ok(key) => key,
        Err(_) => return Err(PeerError::FailedToPrecomputeKey)
    };

    // from now on all messages will be encrypted
    let mut msg_tx = EncryptedMessageWriter::new(msg_tx, precomputed_key.clone(), nonce_local, peer_id.clone());
    let mut msg_rx = EncryptedMessageReader::new(msg_rx, precomputed_key, nonce_remote, peer_id);

    // send metadata
    let metadata = MetadataMessage::new(false, false);
    msg_tx.write_message(&metadata).await?;

    // receive metadata
    let metadata_received = MetadataMessage::from_bytes(msg_rx.read_message().await?)?;
    debug!("Received remote peer metadata - disable_mempool: {}, private_node: {}", metadata_received.disable_mempool, metadata_received.private_node);

    // send ack
    msg_tx.write_message(&AckMessage::Ack).await?;

    // receive ack
    let ack_received = AckMessage::from_bytes(msg_rx.read_message().await?)?;

    match ack_received {
        AckMessage::Ack => {
            debug!("Received ACK");
            Ok(BootstrapOutput(msg_rx, msg_tx))
        }
        AckMessage::Nack => {
            debug!("Received NACK");
            Err(PeerError::NackReceived)
        }
    }
}


/// Generate nonces (sent and recv encoding must be with length bytes also)
///
/// local_nonce is used for writing crypto messages to other peers
/// remote_nonce is used for reading crypto messages from other peers
fn generate_nonces(sent_msg: &RawBinaryMessage, recv_msg: &RawBinaryMessage, incoming: bool) -> NoncePair {
    nonce::generate_nonces(sent_msg.get_raw(), recv_msg.get_raw(), incoming)
}

/// Return supported network protocol version
fn supported_version() -> Version {
    Version::new("TEZOS_ALPHANET_2018-11-30T15:30:56Z".into(), 0, 0)
}

/// Start to process incoming data
async fn begin_process_incoming(mut rx: EncryptedMessageReader, rx_run: Arc<AtomicBool>, myself: PeerRef, event_channel: ChannelRef<NetworkChannelMsg>) {
    info!("Starting accepting messages from peer: {}", rx.peer_id());

    while rx_run.load(Ordering::Relaxed) {
        match rx.read_message().await {
            Ok(msg) => {
                trace!("Msg: {}", hex::encode(&msg));
                match PeerMessageResponse::from_bytes(msg) {
                    Ok(msg) => {
                        let broadcast_message = rx_run.load(Ordering::Relaxed);
                        if broadcast_message {
                            debug!("Message parsed successfully");
                            event_channel.tell(
                                Publish {
                                    msg: PeerMessageReceived {
                                        peer: myself.clone(),
                                        message: Arc::new(msg),
                                    }.into(),
                                    topic: NetworkChannelTopic::NetworkEvents.into(),
                                }, Some(myself.clone().into()));
                        }
                    }
                    Err(e) => {
                        warn!("Failed to process received message: {:?}", e);
                        break;
                    }
                }
            }
            Err(e) => {
                warn!("Failed to read message: {:?}", e);
                break;
            }
        }
    }
}
