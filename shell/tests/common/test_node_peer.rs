// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

//! Test node peer, which simulates p2p remote peer, communicates through real p2p socket

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::channel;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};

use futures::lock::Mutex;
use slog::{crit, debug, error, info, trace, warn, Logger};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::runtime::{Handle, Runtime};
use tokio::time::timeout;

use crypto::hash::OperationHash;
use networking::p2p::peer;
use networking::p2p::peer::{Bootstrap, BootstrapOutput};
use networking::p2p::stream::{EncryptedMessageReader, EncryptedMessageWriter};
use networking::{LocalPeerInfo, ShellCompatibilityVersion};
use tezos_identity::Identity;
use tezos_messages::p2p::encoding::prelude::{Mempool, PeerMessage, PeerMessageResponse};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(8);
const READ_TIMEOUT_LONG: Duration = Duration::from_secs(30);

pub struct TestNodePeer {
    pub identity: Arc<Identity>,
    pub name: String,
    log: Logger,
    pub connected: Arc<AtomicBool>,
    /// Tokio task executor
    tokio_executor: Handle,
    /// Message sender
    tx: Arc<Mutex<Option<EncryptedMessageWriter>>>,

    /// Client mempool for testing
    pub test_mempool: Arc<RwLock<Mempool>>,
}

impl TestNodePeer {
    pub fn connect(
        name: String,
        connect_to_node_port: u16,
        shell_compatibility_version: ShellCompatibilityVersion,
        identity: Identity,
        pow_target: f64,
        log: Logger,
        tokio_runtime: &Runtime,
        handle_message_callback: fn(
            PeerMessageResponse,
        ) -> Result<Vec<PeerMessageResponse>, anyhow::Error>,
    ) -> TestNodePeer {
        let server_address = format!("0.0.0.0:{}", connect_to_node_port)
            .parse::<SocketAddr>()
            .expect("Failed to parse server address");
        let tokio_executor = tokio_runtime.handle().clone();
        let connected = Arc::new(AtomicBool::new(false));
        let tx = Arc::new(Mutex::new(None));
        let identity = Arc::new(identity);
        let test_mempool = Arc::new(RwLock::new(Mempool::default()));
        {
            let test_mempool = test_mempool.clone();
            let identity = identity.clone();
            let connected = connected.clone();
            let tx = tx.clone();
            let log = log.clone();
            let name = name.clone();
            tokio_executor.spawn(async move {
                // init socket connection to server node
                match timeout(CONNECT_TIMEOUT, TcpStream::connect(&server_address)).await {
                    Ok(Ok(stream)) => {
                        // authenticate
                        let local = Arc::new(LocalPeerInfo::new(
                            1235,
                            identity,
                            Arc::new(shell_compatibility_version),
                            pow_target,
                        ));
                        let bootstrap = Bootstrap::outgoing(
                            stream,
                            server_address,
                            false,
                            false,
                        );

                        match peer::bootstrap(bootstrap, local, &log).await {
                            Ok(BootstrapOutput(rx, txw, ..)) => {
                                info!(log, "[{}] Connection successful", name; "ip" => server_address);

                                *tx.lock().await = txw.lock().await.take();
                                connected.store(true, Ordering::Release);

                                // process messages
                                Self::begin_process_incoming(name, rx, tx.clone(), connected, log, server_address, test_mempool, handle_message_callback).await;
                            }
                            Err(e) => {
                                error!(log, "[{}] Connection bootstrap failed", name; "ip" => server_address, "reason" => format!("{:?}", e));
                            }
                        }
                    }
                    Ok(Err(e)) => {
                        error!(log, "[{}] Connection failed", name; "ip" => server_address, "reason" => format!("{:?}", e));
                    }
                    Err(_) => {
                        error!(log, "[{}] Connection timed out", name; "ip" => server_address);
                    }
                }
            });
        }

        TestNodePeer {
            identity,
            name,
            log,
            connected,
            tokio_executor,
            tx,
            test_mempool,
        }
    }

    pub fn try_connect(
        name: &'static str,
        connect_to_node_port: u16,
        shell_compatibility_version: ShellCompatibilityVersion,
        identity: Identity,
        pow_target: f64,
        log: Logger,
        tokio_runtime: &Runtime,
    ) -> Result<(), anyhow::Error> {
        let server_address = format!("0.0.0.0:{}", connect_to_node_port)
            .parse::<SocketAddr>()
            .expect("Failed to parse server address");
        let tokio_executor = tokio_runtime.handle().clone();
        let identity = Arc::new(identity);
        let (tx, rx) = channel();
        {
            let log = log.clone();
            tokio_executor.spawn(async move {
                // init socket connection to server node
                let result = match timeout(CONNECT_TIMEOUT, TcpStream::connect(&server_address)).await {
                    Ok(Ok(stream)) => {
                        // authenticate
                        let local = Arc::new(LocalPeerInfo::new(
                            1235,
                            identity,
                            Arc::new(shell_compatibility_version),
                            pow_target,
                        ));
                        let bootstrap = Bootstrap::outgoing(
                            stream,
                            server_address,
                            false,
                            false,
                        );

                        match peer::bootstrap(bootstrap, local, &log).await {
                            Ok(BootstrapOutput(..)) => {
                                info!(log, "[{}] Connection successful", name; "ip" => server_address);

                                Ok(())
                            }
                            Err(e) => {
                                error!(log, "[{}] Connection bootstrap failed", name; "ip" => server_address, "reason" => format!("{:?}", e));
                                Err(e)
                            }
                        }
                    }
                    Ok(Err(e)) => {
                        error!(log, "[{}] Connection failed", name; "ip" => server_address, "reason" => format!("{:?}", e));
                        Err(e.into())
                    }
                    Err(e) => {
                        error!(log, "[{}] Connection timed out", name; "ip" => server_address);
                        Err(e.into())
                    }
                };
                tx.send(result).expect("Failed to send the result")
            });
            Ok(rx.recv().expect("Failed to receive the result")?)
        }
    }

    /// Start to process incoming data
    async fn begin_process_incoming(
        name: String,
        rx: Arc<Mutex<Option<EncryptedMessageReader>>>,
        tx: Arc<Mutex<Option<EncryptedMessageWriter>>>,
        connected: Arc<AtomicBool>,
        log: Logger,
        peer_address: SocketAddr,
        test_mempool: Arc<RwLock<Mempool>>,
        handle_message_callback: fn(
            PeerMessageResponse,
        ) -> Result<Vec<PeerMessageResponse>, anyhow::Error>,
    ) {
        info!(log, "[{}] Starting to accept messages", name; "ip" => format!("{:?}", &peer_address));

        let mut rx = rx.lock().await;
        let mut rx = rx.take().unwrap();
        while connected.load(Ordering::Acquire) {
            match timeout(READ_TIMEOUT_LONG, rx.read_message::<PeerMessageResponse>()).await {
                Ok(res) => match res {
                    Ok(msg) => {
                        let msg_type = msg_type(&msg);
                        trace!(log, "[{}] Handle message", name; "ip" => format!("{:?}", &peer_address), "msg_type" => msg_type.clone());

                        // we collect here simple Mempool
                        if let PeerMessage::CurrentHead(current_head) = msg.message() {
                            let mut test_mempool = test_mempool.write().expect("Failed to lock");
                            let mut known_valid: Vec<OperationHash> =
                                test_mempool.known_valid().clone();
                            let mut pending = test_mempool.pending().clone();

                            for op in current_head.current_mempool().known_valid() {
                                if !known_valid.contains(op) {
                                    known_valid.push(op.clone());
                                }
                            }
                            for op in current_head.current_mempool().pending() {
                                if !pending.contains(op) {
                                    pending.push(op.clone());
                                }
                            }

                            *test_mempool = Mempool::new(known_valid, pending);
                        }

                        // apply callback
                        match handle_message_callback(msg) {
                            Ok(responses) => {
                                trace!(log, "[{}] Message handled({})", name, !responses.is_empty(); "msg_type" => msg_type);
                                for response in responses {
                                    // send back response
                                    let mut tx_lock = tx.lock().await;
                                    if let Some(tx) = tx_lock.as_mut() {
                                        tx.write_message(&response).await.unwrap_or_else(|_| {
                                            panic!("[{}] Failed to send message", name)
                                        });
                                        drop(tx_lock);
                                    }
                                }
                            }
                            Err(e) => {
                                error!(log, "[{}] Failed to handle message", name; "reason" => format!("{:?}", e), "msg_type" => msg_type)
                            }
                        }
                    }
                    Err(e) => {
                        crit!(log, "[{}] Failed to read peer message", name; "reason" => e);
                        break;
                    }
                },
                Err(_) => {
                    warn!(log, "[{}] Peer message read timed out - lets next run", name; "secs" => READ_TIMEOUT_LONG.as_secs());
                }
            }
        }

        debug!(log, "[{}] Shutting down peer connection", name; "ip" => format!("{:?}", &peer_address));
        connected.store(false, Ordering::Release);
        if let Some(tx) = tx.lock().await.take() {
            let mut socket = rx.unsplit(tx);
            match socket.shutdown().await {
                Ok(()) => {
                    debug!(log, "[{}] Connection shutdown successful", name; "socket" => format!("{:?}", socket))
                }
                Err(err) => {
                    error!(log, "[{}] Failed to shutdown connection", name; "err" => format!("{:?}", err), "socket" => format!("{:?}", socket))
                }
            }
        }
        info!(log, "[{}] Stopped to accept messages", name; "ip" => format!("{:?}", &peer_address));
    }

    const IO_TIMEOUT: Duration = Duration::from_secs(6);

    pub fn send_msg<Msg: Into<PeerMessage>>(&mut self, msg: Msg) -> Result<(), anyhow::Error> {
        // need to at first wait for tx to be initialized in bootstrap
        if !self.connected.load(Ordering::Acquire) {
            assert!(self
                .wait_for_connection((Duration::from_secs(5), Duration::from_millis(100)))
                .is_ok());
        }

        // lets send message to open tx channel
        let msg: PeerMessageResponse = msg.into().into();
        let tx = self.tx.clone();
        let name = self.name.to_string();
        let log = self.log.clone();
        self.tokio_executor.spawn(async move {
            let mut tx_lock = tx.lock().await;
            if let Some(tx) = tx_lock.as_mut() {
                match timeout(Self::IO_TIMEOUT, tx.write_message(&msg)).await {
                    Ok(Ok(())) => (),
                    Ok(Err(e)) => error!(log, "[{}] write_message - failed", name; "reason" => format!("{:?}", e)),
                    Err(e) => error!(log, "[{}] write_message - connection timed out", name; "reason" => format!("{:?}", e)),
                }
            }
            drop(tx_lock);
        });

        Ok(())
    }

    // TODO: refactor with async/condvar, not to block main thread
    pub fn wait_for_connection(
        &mut self,
        (timeout, delay): (Duration, Duration),
    ) -> Result<(), anyhow::Error> {
        let start = Instant::now();

        loop {
            if self.connected.load(Ordering::Acquire) {
                break Ok(());
            }

            // kind of simple retry policy
            if start.elapsed().le(&timeout) {
                std::thread::sleep(delay);
            } else {
                break Err(anyhow::format_err!("[{}] wait_for_connection - something is wrong - timeout (timeout: {:?}, delay: {:?}) exceeded!", self.name, timeout, delay));
            }
        }
    }

    // TODO: refactor with async/condvar, not to block main thread
    pub fn wait_for_mempool_contains_operations(
        &self,
        marker: &str,
        expected_operations: &HashSet<OperationHash>,
        (timeout, delay): (Duration, Duration),
    ) -> Result<(), anyhow::Error> {
        let start = Instant::now();

        let result = loop {
            let mempool_state = self.test_mempool.read().expect("Failed to obtain lock");

            let mut operations = HashSet::default();
            operations.extend(mempool_state.known_valid().clone());
            operations.extend(mempool_state.pending().clone());

            if contains_all_keys(&operations, expected_operations) {
                info!(self.log, "[{}] All expected operations found in mempool", self.name; "marker" => marker);
                break Ok(());
            }

            // kind of simple retry policy
            if start.elapsed().le(&timeout) {
                thread::sleep(delay);
            } else {
                break Err(anyhow::format_err!("[{}] wait_for_mempool_contains_operations() - timeout (timeout: {:?}, delay: {:?}) exceeded! marker: {}", self.name, timeout, delay, marker));
            }
        };
        result
    }

    pub fn clear_mempool(&mut self) {
        let mut test_mempool = self.test_mempool.write().expect("Failed to obtain lock");
        *test_mempool = Mempool::default();
    }

    pub fn stop(&mut self) {
        self.connected.store(false, Ordering::Release);
    }
}

impl Drop for TestNodePeer {
    fn drop(&mut self) {
        self.stop();
    }
}

fn msg_type(msg: &PeerMessageResponse) -> String {
    match msg.message() {
        PeerMessage::Disconnect => "Disconnect",
        PeerMessage::Advertise(_) => "Advertise",
        PeerMessage::SwapRequest(_) => "SwapRequest",
        PeerMessage::SwapAck(_) => "SwapAck",
        PeerMessage::Bootstrap => "Bootstrap",
        PeerMessage::GetCurrentBranch(_) => "GetCurrentBranch",
        PeerMessage::CurrentBranch(_) => "CurrentBranch",
        PeerMessage::Deactivate(_) => "Deactivate",
        PeerMessage::GetCurrentHead(_) => "GetCurrentHead",
        PeerMessage::CurrentHead(_) => "CurrentHead",
        PeerMessage::GetBlockHeaders(_) => "GetBlockHeaders",
        PeerMessage::BlockHeader(_) => "BlockHeader",
        PeerMessage::GetOperations(_) => "GetOperations",
        PeerMessage::Operation(_) => "Operation",
        PeerMessage::GetProtocols(_) => "GetProtocols",
        PeerMessage::Protocol(_) => "Protocol",
        PeerMessage::GetOperationsForBlocks(_) => "GetOperationsForBlocks",
        PeerMessage::OperationsForBlocks(_) => "OperationsForBlocks",
    }
    .to_string()
}

fn contains_all_keys(set: &HashSet<OperationHash>, keys: &HashSet<OperationHash>) -> bool {
    let mut contains_counter = 0;
    for key in keys {
        if set.contains(key) {
            contains_counter += 1;
        }
    }
    contains_counter == keys.len()
}
