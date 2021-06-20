// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT
#![forbid(unsafe_code)]

//! Provides IPC communication.
//!
//! The IPC is implemented as unix domain sockets. Functionality is similar to how network sockets work.
//!
//! TODO: TE-292 - investigate/reimplement

use std::fs;
use std::io;
use std::io::prelude::*;
use std::iter;
use std::marker::PhantomData;
use std::net::Shutdown;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use std::{env, thread};

use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// IPC communication errors
#[derive(Debug, Error)]
pub enum IpcError {
    #[error("Receive message length error: {reason}")]
    ReceiveMessageLengthError { reason: io::Error },
    #[error("Receive message error: {reason}")]
    ReceiveMessageError { reason: io::Error },
    #[error("Send error: {reason}")]
    SendError { reason: io::Error },
    #[error("Accept connection timed out, timout: {timeout:?}")]
    AcceptTimeout { timeout: Duration },
    #[error("Receive message timed out - handle WouldBlock scenario if needed")]
    ReceiveMessageTimeouted,
    #[error("Connection error: {reason}")]
    ConnectionError { reason: io::Error },
    #[error("Serialization error: {reason}")]
    SerializationError { reason: String },
    #[error("Deserialization error: {reason}")]
    DeserializationError { reason: String },
    #[error("Split stream error: {reason}")]
    SplitError { reason: io::Error },
    #[error("Socker configuration error: {reason}")]
    SocketConfigurationError { reason: io::Error },
    #[error("IPC error: {reason}")]
    OtherError { reason: String },
}

/// Represents sending end of the IPC channel.
pub struct IpcSender<S>(UnixStream, PhantomData<S>);

impl<S> IpcSender<S> {
    /// Close IPC channel and release associated resources.
    ///
    /// This closes only the sending part of the IPC channel.
    fn shutdown(&self) -> Result<(), io::Error> {
        self.0.shutdown(Shutdown::Write)
    }

    pub fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.0.set_write_timeout(timeout)
    }

    pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.0.set_nonblocking(nonblocking)
    }
}

impl<S: Serialize> IpcSender<S> {
    /// Serialize and sent `value` through IPC channel.
    ///
    /// This is a blocking operation,
    pub fn send(&mut self, value: &S) -> Result<(), IpcError> {
        let msg_buf = bincode::serialize(value).map_err(|err| IpcError::SerializationError {
            reason: format!("{:?}", err),
        })?;
        let msg_len_buf = msg_buf.len().to_be_bytes();
        self.0
            .write_all(&msg_len_buf)
            .map_err(|err| IpcError::SendError { reason: err })?;
        self.0
            .write_all(&msg_buf)
            .map_err(|err| IpcError::SendError { reason: err })?;
        self.0
            .flush()
            .map_err(|err| IpcError::SendError { reason: err })
    }
}

impl<S> Drop for IpcSender<S> {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

/// Represents receiving end of the IPC channel.
pub struct IpcReceiver<R>(UnixStream, PhantomData<R>);

impl<R> IpcReceiver<R> {
    /// Close IPC channel and release associated resources.
    ///
    /// This closes only the receiving part of the IPC channel.
    fn shutdown(&self) -> Result<(), io::Error> {
        self.0.shutdown(Shutdown::Read)
    }

    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.0.set_read_timeout(timeout)
    }

    pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.0.set_nonblocking(nonblocking)
    }
}

impl<R> IpcReceiver<R>
where
    R: for<'de> Deserialize<'de>,
{
    /// Try to receive message with read_timeout,
    /// In case of timeout, can be IpcError::ReceiveMessageTimeouted handled
    ///
    /// `read_timeout` - set read timeout before receive
    /// `reset_read_timeout` - set read timeout after receive
    pub fn try_receive(
        &mut self,
        read_timeout: Option<Duration>,
        reset_read_timeout: Option<Duration>,
    ) -> Result<R, IpcError> {
        self.set_read_timeout(read_timeout)
            .map_err(|err| IpcError::SocketConfigurationError { reason: err })?;
        let receive_result = self.receive();
        self.set_read_timeout(reset_read_timeout)
            .map_err(|err| IpcError::SocketConfigurationError { reason: err })?;
        receive_result
    }

    /// Read bytes from established IPC channel and deserialize into a rust type.
    pub fn receive(&mut self) -> Result<R, IpcError> {
        let mut msg_len_buf = [0; 8];
        self.0.read_exact(&mut msg_len_buf).map_err(|err| {
            if err.kind() == io::ErrorKind::WouldBlock {
                IpcError::ReceiveMessageTimeouted
            } else {
                IpcError::ReceiveMessageLengthError { reason: err }
            }
        })?;

        let msg_len = usize::from_be_bytes(msg_len_buf);

        let mut msg_buf = vec![0u8; msg_len];
        self.0
            .read_exact(&mut msg_buf)
            .map_err(|err| IpcError::ReceiveMessageError { reason: err })?;

        bincode::deserialize(&msg_buf).map_err(|err| IpcError::DeserializationError {
            reason: format!("{:?}", err),
        })
    }
}

impl<R> Drop for IpcReceiver<R> {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

/// Listens for incoming IPC connections.
pub struct IpcServer<R, S> {
    listener: UnixListener,
    pub path: PathBuf,
    _phantom_r: PhantomData<R>,
    _phantom_s: PhantomData<S>,
}

impl<R, S> Drop for IpcServer<R, S> {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

impl<R, S> IpcServer<R, S>
where
    R: for<'de> Deserialize<'de>,
    S: Serialize,
{
    /// Bind IpcServer to random socket in temp folder
    pub fn bind() -> Result<Self, IpcError> {
        let path = temp_sock();
        Self::bind_path(&path)
    }

    /// Bind IpcServer to specific path
    ///
    /// # Arguments
    /// * `path` - path to the unix socket
    pub fn bind_path<P: AsRef<Path>>(path: P) -> Result<Self, IpcError> {
        let path_buf = path.as_ref().into();
        let listener =
            UnixListener::bind(path).map_err(|err| IpcError::ConnectionError { reason: err })?;

        Ok(IpcServer {
            listener,
            path: path_buf,
            _phantom_r: PhantomData,
            _phantom_s: PhantomData,
        })
    }

    /// Try to accept new connection a return sender/receiver for it
    /// In case of timeout, can be IpcError::AcceptTimeout handled
    ///
    /// `timeout` - timeout for wairing to a new connection
    ///
    /// Returns `blocking` receiver a `blocking` sender
    pub fn try_accept(
        &mut self,
        timeout: Duration,
    ) -> Result<(IpcReceiver<R>, IpcSender<S>), IpcError> {
        // allow non-blocking, because we dont want to wait forever, so we handle WouldBlock case
        self.listener
            .set_nonblocking(true)
            .map_err(|err| IpcError::ConnectionError { reason: err })?;

        // TODO: TE-292 - investigate/reimplement
        // timeout_io::Acceptor was used here, but it caused occasioanly very strange errors:
        // - in debug mode - "Other { desc: "Os {\n    code: 9,\n    kind: Other,\n    message: \"Bad file descriptor\",\n}"
        // - in release mode - Segmentation fault
        //
        // probably, because it sets non_blocking differentlly:
        // https://github.com/KizzyCode/timeout_io/blob/master/libselect/libselect_unix.c
        //
        // so this was removed, and used direct listener.set_nonblocking, which handles, like this:
        // https://github.com/rust-lang/rust/blob/master/library/std/src/sys/unix/net.rs

        // very simple retry logic
        let deadline = Instant::now() + timeout;
        let stream = loop {
            match self.listener.accept() {
                Ok(connection) => break connection,
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    if deadline.checked_duration_since(Instant::now()).is_some() {
                        // ok, lets retry
                        thread::sleep(Duration::from_millis(50))
                    } else {
                        // deadline is over
                        return Err(IpcError::AcceptTimeout { timeout });
                    }
                }
                Err(e) => {
                    return Err(IpcError::ConnectionError { reason: e });
                }
            }
        };

        // return back blocking mode (maybe not needed)
        self.listener
            .set_nonblocking(false)
            .map_err(|err| IpcError::ConnectionError { reason: err })?;

        // splitted receiver/sender are supposed to be blocking
        // maybe it is enought to set non_blocking to the [`stream`], but we make sure,
        // also On macOS and FreeBSD new sockets inherit flags from accepting fd,
        // but we expect this to be in blocking by default.
        split(stream.0, false, false)
    }

    /// Accept new connection a return sender/receiver for it
    pub fn accept(&mut self) -> Result<(IpcReceiver<R>, IpcSender<S>), IpcError> {
        let stream = self
            .listener
            .accept()
            .map_err(|e| IpcError::ConnectionError { reason: e })?;

        // see explaination at `try_accept`.
        split(stream.0, false, false)
    }

    /// Create new IpcClient for this server
    pub fn client(&self) -> IpcClient<R, S> {
        IpcClient::new(&self.path)
    }
}

/// Connects to a listening IPC endpoint.
#[derive(Debug)]
pub struct IpcClient<R, S> {
    path: PathBuf,
    _phantom_r: PhantomData<R>,
    _phantom_s: PhantomData<S>,
}

impl<R, S> IpcClient<R, S> {
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl<R, S> IpcClient<R, S>
where
    R: for<'de> Deserialize<'de>,
    S: Serialize,
{
    /// Create new client instance.
    ///
    /// # Arguments
    /// * `path` - path to existing unix socket
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        IpcClient {
            path: path.as_ref().into(),
            _phantom_r: PhantomData,
            _phantom_s: PhantomData,
        }
    }

    /// Try to open new connection.
    pub fn connect(&self) -> Result<(IpcReceiver<R>, IpcSender<S>), IpcError> {
        let stream = UnixStream::connect(&self.path)
            .map_err(|err| IpcError::ConnectionError { reason: err })?;
        split(stream, false, false)
    }
}

/// Crate new randomly named unix domain socket file in temp directory.
pub fn temp_sock() -> PathBuf {
    let mut rng = thread_rng();
    let temp_dir = env::temp_dir();
    let chars = iter::repeat(())
        .map(|()| rng.sample(Alphanumeric))
        .take(7)
        .collect::<String>();

    temp_dir.join(chars + ".sock")
}

fn split<R, S>(
    stream: UnixStream,
    receiver_non_blocking: bool,
    sender_non_blocking: bool,
) -> Result<(IpcReceiver<R>, IpcSender<S>), IpcError>
where
    R: for<'de> Deserialize<'de>,
    S: Serialize,
{
    let receiver = IpcReceiver(
        stream
            .try_clone()
            .map_err(|err| IpcError::SplitError { reason: err })?,
        PhantomData,
    );
    receiver
        .set_nonblocking(receiver_non_blocking)
        .map_err(|err| IpcError::SocketConfigurationError { reason: err })?;

    let sender = IpcSender(stream, PhantomData);
    sender
        .set_nonblocking(sender_non_blocking)
        .map_err(|err| IpcError::SocketConfigurationError { reason: err })?;

    Ok((receiver, sender))
}
