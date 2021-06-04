// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::cell::RefCell;
use std::convert::AsRef;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use failure::Fail;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use slog::{debug, warn, Logger};
use strum_macros::IntoStaticStr;

use crypto::hash::{ChainId, ContextHash, ProtocolHash};
use ipc::*;
use tezos_api::environment::TezosEnvironmentConfiguration;
use tezos_api::ffi::*;
use tezos_context::channel::{context_receive, context_send, ContextAction};
use tezos_messages::p2p::encoding::operation::Operation;

use crate::protocol::*;
use crate::runner::{ProtocolRunner, ProtocolRunnerError};
use crate::ProtocolEndpointConfiguration;

lazy_static! {
    /// Ww need to have multiple multiple FFI runtimes, and runtime needs to have initialized protocol context,
    /// but there are some limitations,
    /// e.g.: if we start readonly context with empty context directory, it fails in FFI runtime in irmin initialization,
    /// so we need to control this initialization on application level,
    ///
    /// in application we can have multiple threads, which tries to call init_protocol (read or write),
    /// so this lock ensures, that in the whole application at least one 'write init_protocol_context' was successfull, which means,
    /// that FFI context has created required files, so after that we can let continue other threads to initialize readonly context
    ///
    /// see also: test_mutliple_protocol_runners_with_one_write_multiple_read_init_context
    static ref AT_LEAST_ONE_WRITE_PROTOCOL_CONTEXT_WAS_SUCCESS_AT_FIRST_LOCK: Arc<(Mutex<bool>, Condvar)> = Arc::new((Mutex::new(false), Condvar::new()));
}

/// This command message is generated by tezedge node and is received by the protocol runner.
#[derive(Serialize, Deserialize, Debug, IntoStaticStr)]
enum ProtocolMessage {
    ApplyBlockCall(ApplyBlockRequest),
    AssertEncodingForProtocolDataCall(ProtocolHash, RustBytes),
    BeginApplicationCall(BeginApplicationRequest),
    BeginConstructionCall(BeginConstructionRequest),
    ValidateOperationCall(ValidateOperationRequest),
    ProtocolRpcCall(ProtocolRpcRequest),
    HelpersPreapplyOperationsCall(ProtocolRpcRequest),
    HelpersPreapplyBlockCall(HelpersPreapplyBlockRequest),
    ComputePathCall(ComputePathRequest),
    ChangeRuntimeConfigurationCall(TezosRuntimeConfiguration),
    InitProtocolContextCall(InitProtocolContextParams),
    GenesisResultDataCall(GenesisResultDataParams),
    JSONEncodeApplyBlockResultMetadata {
        context_hash: ContextHash,
        metadata_bytes: RustBytes,
        max_operations_ttl: i32,
        protocol_hash: ProtocolHash,
        next_protocol_hash: ProtocolHash,
    },
    JSONEncodeApplyBlockOperationsMetadata {
        chain_id: ChainId,
        operations: Vec<Vec<Operation>>,
        operations_metadata_bytes: Vec<Vec<RustBytes>>,
        protocol_hash: ProtocolHash,
        next_protocol_hash: ProtocolHash,
    },
    ShutdownCall,
}

#[derive(Serialize, Deserialize, Debug)]
struct InitProtocolContextParams {
    storage_data_dir: String,
    genesis: GenesisChain,
    genesis_max_operations_ttl: u16,
    protocol_overrides: ProtocolOverrides,
    commit_genesis: bool,
    enable_testchain: bool,
    readonly: bool,
    turn_off_context_raw_inspector: bool,
    patch_context: Option<PatchContext>,
}

#[derive(Serialize, Deserialize, Debug)]
struct GenesisResultDataParams {
    genesis_context_hash: ContextHash,
    chain_id: ChainId,
    genesis_protocol_hash: ProtocolHash,
    genesis_max_operations_ttl: u16,
}

/// This event message is generated as a response to the `ProtocolMessage` command.
#[derive(Serialize, Deserialize, Debug, IntoStaticStr)]
enum NodeMessage {
    ApplyBlockResult(Result<ApplyBlockResponse, ApplyBlockError>),
    AssertEncodingForProtocolDataResult(Result<(), ProtocolDataError>),
    BeginApplicationResult(Result<BeginApplicationResponse, BeginApplicationError>),
    BeginConstructionResult(Result<PrevalidatorWrapper, BeginConstructionError>),
    ValidateOperationResponse(Result<ValidateOperationResponse, ValidateOperationError>),
    RpcResponse(Result<ProtocolRpcResponse, ProtocolRpcError>),
    HelpersPreapplyResponse(Result<HelpersPreapplyResponse, HelpersPreapplyError>),
    ChangeRuntimeConfigurationResult(Result<(), TezosRuntimeConfigurationError>),
    InitProtocolContextResult(Result<InitProtocolContextResult, TezosStorageInitError>),
    CommitGenesisResultData(Result<CommitGenesisResult, GetDataError>),
    ComputePathResponse(Result<ComputePathResponse, ComputePathError>),
    JsonEncodeApplyBlockResultMetadataResponse(Result<String, FfiJsonEncoderError>),
    JsonEncodeApplyBlockOperationsMetadata(Result<String, FfiJsonEncoderError>),
    ShutdownResult,
}

/// Empty message
#[derive(Serialize, Deserialize, Debug)]
struct NoopMessage;

pub fn process_protocol_events<P: AsRef<Path>>(socket_path: P) -> Result<(), IpcError> {
    let ipc_client: IpcClient<NoopMessage, ContextAction> = IpcClient::new(socket_path);
    let (_, mut tx) = ipc_client.connect()?;
    while let Ok(action) = context_receive() {
        tx.send(&action)?;
        if let ContextAction::Shutdown = action {
            break;
        }
    }

    Ok(())
}

/// Establish connection to existing IPC endpoint (which was created by tezedge node).
/// Begin receiving commands from the tezedge node until `ShutdownCall` command is received.
pub fn process_protocol_commands<Proto: ProtocolApi, P: AsRef<Path>, SDC: Fn(&Logger)>(
    socket_path: P,
    log: &Logger,
    shutdown_callback: SDC,
) -> Result<(), IpcError> {
    let ipc_client: IpcClient<ProtocolMessage, NodeMessage> = IpcClient::new(socket_path);
    let (mut rx, mut tx) = ipc_client.connect()?;
    while let Ok(cmd) = rx.receive() {
        match cmd {
            ProtocolMessage::ApplyBlockCall(request) => {
                let res = Proto::apply_block(request);
                tx.send(&NodeMessage::ApplyBlockResult(res))?;
            }
            ProtocolMessage::AssertEncodingForProtocolDataCall(protocol_hash, protocol_data) => {
                let res = Proto::assert_encoding_for_protocol_data(protocol_hash, protocol_data);
                tx.send(&NodeMessage::AssertEncodingForProtocolDataResult(res))?;
            }
            ProtocolMessage::BeginConstructionCall(request) => {
                let res = Proto::begin_construction(request);
                tx.send(&NodeMessage::BeginConstructionResult(res))?;
            }
            ProtocolMessage::BeginApplicationCall(request) => {
                let res = Proto::begin_application(request);
                tx.send(&NodeMessage::BeginApplicationResult(res))?;
            }
            ProtocolMessage::ValidateOperationCall(request) => {
                let res = Proto::validate_operation(request);
                tx.send(&NodeMessage::ValidateOperationResponse(res))?;
            }
            ProtocolMessage::ProtocolRpcCall(request) => {
                let res = Proto::call_protocol_rpc(request);
                tx.send(&NodeMessage::RpcResponse(res))?;
            }
            ProtocolMessage::HelpersPreapplyOperationsCall(request) => {
                let res = Proto::helpers_preapply_operations(request);
                tx.send(&NodeMessage::HelpersPreapplyResponse(res))?;
            }
            ProtocolMessage::HelpersPreapplyBlockCall(request) => {
                let res = Proto::helpers_preapply_block(request);
                tx.send(&NodeMessage::HelpersPreapplyResponse(res))?;
            }
            ProtocolMessage::ComputePathCall(request) => {
                let res = Proto::compute_path(request);
                tx.send(&NodeMessage::ComputePathResponse(res))?;
            }
            ProtocolMessage::ChangeRuntimeConfigurationCall(params) => {
                let res = Proto::change_runtime_configuration(params);
                tx.send(&NodeMessage::ChangeRuntimeConfigurationResult(res))?;
            }
            ProtocolMessage::InitProtocolContextCall(params) => {
                let res = Proto::init_protocol_context(
                    params.storage_data_dir,
                    params.genesis,
                    params.protocol_overrides,
                    params.commit_genesis,
                    params.enable_testchain,
                    params.readonly,
                    params.turn_off_context_raw_inspector,
                    params.patch_context,
                );
                tx.send(&NodeMessage::InitProtocolContextResult(res))?;
            }
            ProtocolMessage::GenesisResultDataCall(params) => {
                let res = Proto::genesis_result_data(
                    &params.genesis_context_hash,
                    &params.chain_id,
                    &params.genesis_protocol_hash,
                    params.genesis_max_operations_ttl,
                );
                tx.send(&NodeMessage::CommitGenesisResultData(res))?;
            }
            ProtocolMessage::JSONEncodeApplyBlockResultMetadata {
                context_hash,
                metadata_bytes,
                max_operations_ttl,
                protocol_hash,
                next_protocol_hash,
            } => {
                let res = Proto::apply_block_result_metadata(
                    context_hash,
                    metadata_bytes,
                    max_operations_ttl,
                    protocol_hash,
                    next_protocol_hash,
                );
                tx.send(&NodeMessage::JsonEncodeApplyBlockResultMetadataResponse(
                    res,
                ))?;
            }
            ProtocolMessage::JSONEncodeApplyBlockOperationsMetadata {
                chain_id,
                operations,
                operations_metadata_bytes,
                protocol_hash,
                next_protocol_hash,
            } => {
                let res = Proto::apply_block_operations_metadata(
                    chain_id,
                    operations,
                    operations_metadata_bytes,
                    protocol_hash,
                    next_protocol_hash,
                );
                tx.send(&NodeMessage::JsonEncodeApplyBlockOperationsMetadata(res))?;
            }
            ProtocolMessage::ShutdownCall => {
                // send shutdown event to context listener, that we dont need it anymore
                if let Err(e) = context_send(ContextAction::Shutdown) {
                    warn!(log, "Failed to send shutdown command to context channel"; "reason" => format!("{}", e));
                }

                // we trigger shutdown callback before, returning response
                shutdown_callback(log);

                // return result
                if let Err(e) = tx.send(&NodeMessage::ShutdownResult) {
                    warn!(log, "Failed to send shutdown response"; "reason" => format!("{}", e));
                }

                // drop socket
                drop(tx);
                drop(rx);
                break;
            }
        }
    }

    Ok(())
}

/// Error types generated by a tezos protocol.
#[derive(Fail, Debug)]
pub enum ProtocolError {
    /// Protocol rejected to apply a block.
    #[fail(display = "Apply block error: {}", reason)]
    ApplyBlockError { reason: ApplyBlockError },
    #[fail(display = "Assert encoding for protocol data error: {}", reason)]
    AssertEncodingForProtocolDataError { reason: ProtocolDataError },
    #[fail(display = "Begin construction error: {}", reason)]
    BeginApplicationError { reason: BeginApplicationError },
    #[fail(display = "Begin construction error: {}", reason)]
    BeginConstructionError { reason: BeginConstructionError },
    #[fail(display = "Validate operation error: {}", reason)]
    ValidateOperationError { reason: ValidateOperationError },
    #[fail(display = "Protocol rpc call error: {}", reason)]
    ProtocolRpcError { reason: ProtocolRpcError },
    #[fail(display = "Helper Preapply call error: {}", reason)]
    HelpersPreapplyError { reason: HelpersPreapplyError },
    #[fail(display = "Compute path call error: {}", reason)]
    ComputePathError { reason: ComputePathError },
    /// Error in configuration.
    #[fail(display = "OCaml runtime configuration error: {}", reason)]
    TezosRuntimeConfigurationError {
        reason: TezosRuntimeConfigurationError,
    },
    /// OCaml part failed to initialize tezos storage.
    #[fail(display = "OCaml storage init error: {}", reason)]
    OcamlStorageInitError { reason: TezosStorageInitError },
    /// OCaml part failed to get genesis data.
    #[fail(display = "Failed to get genesis data: {}", reason)]
    GenesisResultDataError { reason: GetDataError },
}

/// Errors generated by `protocol_runner`.
#[derive(Fail, Debug)]
pub enum ProtocolServiceError {
    /// Generic IPC communication error. See `reason` for more details.
    #[fail(display = "IPC error: {}", reason)]
    IpcError { reason: IpcError },
    /// Tezos protocol error.
    #[fail(display = "Protocol error: {}", reason)]
    ProtocolError { reason: ProtocolError },
    /// Unexpected message was received from IPC channel
    #[fail(display = "Received unexpected message: {}", message)]
    UnexpectedMessage { message: &'static str },
    /// Invalid data error
    #[fail(display = "Invalid data error: {}", message)]
    InvalidDataError { message: String },
    /// Lock error
    #[fail(display = "Lock error: {:?}", message)]
    LockPoisonError { message: String },
}

impl<T> From<std::sync::PoisonError<T>> for ProtocolServiceError {
    fn from(source: std::sync::PoisonError<T>) -> Self {
        Self::LockPoisonError {
            message: source.to_string(),
        }
    }
}

impl slog::Value for ProtocolServiceError {
    fn serialize(
        &self,
        _record: &slog::Record,
        key: slog::Key,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        serializer.emit_arguments(key, &format_args!("{}", self))
    }
}

impl From<IpcError> for ProtocolServiceError {
    fn from(error: IpcError) -> Self {
        ProtocolServiceError::IpcError { reason: error }
    }
}

impl From<ProtocolError> for ProtocolServiceError {
    fn from(error: ProtocolError) -> Self {
        ProtocolServiceError::ProtocolError { reason: error }
    }
}

pub fn handle_protocol_service_error<LC: Fn(ProtocolServiceError)>(
    error: ProtocolServiceError,
    log_callback: LC,
) -> Result<(), ProtocolServiceError> {
    match error {
        ProtocolServiceError::IpcError { .. } | ProtocolServiceError::UnexpectedMessage { .. } => {
            // we need to refresh protocol runner endpoint, so propagate error
            Err(error)
        }
        _ => {
            // just log error
            log_callback(error);
            Ok(())
        }
    }
}

/// IPC command server is listening for incoming IPC connections.
pub struct IpcCmdServer(
    IpcServer<NodeMessage, ProtocolMessage>,
    ProtocolEndpointConfiguration,
);

/// Difference between `IpcCmdServer` and `IpcEvtServer` is:
/// * `IpcCmdServer` is used to create IPC channel over which commands from node are transferred to the protocol runner.
/// * `IpcEvtServer` is used to create IPC channel over which events are transmitted from protocol runner to the tezedge node.
impl IpcCmdServer {
    const IO_TIMEOUT: Duration = Duration::from_secs(10);

    /// Create new IPC endpoint
    pub fn try_new(configuration: ProtocolEndpointConfiguration) -> Result<Self, IpcError> {
        Ok(IpcCmdServer(
            IpcServer::bind_path(&temp_sock())?,
            configuration,
        ))
    }

    /// Start accepting incoming IPC connection.
    ///
    /// Returns a [`protocol controller`](ProtocolController) if new IPC channel is successfully created.
    /// This is a blocking operation.
    pub fn try_accept(&mut self, timeout: Duration) -> Result<ProtocolController, IpcError> {
        let (rx, tx) = self.0.try_accept(timeout)?;
        // configure default IO timeouts
        rx.set_read_timeout(Some(Self::IO_TIMEOUT))
            .and(tx.set_write_timeout(Some(Self::IO_TIMEOUT)))
            .map_err(|err| IpcError::SocketConfigurationError { reason: err })?;

        Ok(ProtocolController {
            io: RefCell::new(IpcIO { rx, tx }),
            configuration: self.1.clone(),
            shutting_down: false,
        })
    }
}

/// IPC event server is listening for incoming IPC connections.
pub struct IpcEvtServer(IpcServer<ContextAction, NoopMessage>);

/// Difference between `IpcCmdServer` and `IpcEvtServer` is:
/// * `IpcCmdServer` is used to create IPC channel over which commands from node are transferred to the protocol runner.
/// * `IpcEvtServer` is used to create IPC channel over which events are transmitted from protocol runner to the tezedge node.
impl IpcEvtServer {
    pub fn try_bind_new() -> Result<Self, IpcError> {
        Ok(IpcEvtServer(IpcServer::bind_path(&temp_sock())?))
    }

    /// Synchronously wait for new incoming IPC connection.
    pub fn try_accept(
        &mut self,
        timeout: Duration,
    ) -> Result<IpcReceiver<ContextAction>, IpcError> {
        let (rx, _) = self.0.try_accept(timeout)?;
        Ok(rx)
    }

    /// Returns socket path
    pub fn server_path(&self) -> PathBuf {
        self.0.path.clone()
    }
}

struct IpcIO {
    rx: IpcReceiver<NodeMessage>,
    tx: IpcSender<ProtocolMessage>,
}

/// Encapsulate IPC communication.
pub struct ProtocolController {
    io: RefCell<IpcIO>,
    configuration: ProtocolEndpointConfiguration,
    /// Indicates that was triggered shutting down
    shutting_down: bool,
}

/// Provides convenience methods for IPC communication.
///
/// Instead of manually sending and receiving messages over IPC channel use provided methods.
/// Methods also handle things such as timeouts and also checks is correct response type is received.
impl ProtocolController {
    const APPLY_BLOCK_TIMEOUT: Duration = Duration::from_secs(60 * 60 * 2);
    const INIT_PROTOCOL_CONTEXT_TIMEOUT: Duration = Duration::from_secs(60);
    const BEGIN_APPLICATION_TIMEOUT: Duration = Duration::from_secs(120);
    const BEGIN_CONSTRUCTION_TIMEOUT: Duration = Duration::from_secs(120);
    const VALIDATE_OPERATION_TIMEOUT: Duration = Duration::from_secs(120);
    const CALL_PROTOCOL_RPC_TIMEOUT: Duration = Duration::from_secs(30);
    const COMPUTE_PATH_TIMEOUT: Duration = Duration::from_secs(30);
    const ASSERT_ENCODING_FOR_PROTOCOL_DATA_TIMEOUT: Duration = Duration::from_secs(15);

    /// Apply block
    pub fn apply_block(
        &self,
        request: ApplyBlockRequest,
    ) -> Result<ApplyBlockResponse, ProtocolServiceError> {
        let mut io = self.io.borrow_mut();
        io.tx.send(&ProtocolMessage::ApplyBlockCall(request))?;

        // this might take a while, so we will use unusually long timeout
        match io.rx.try_receive(
            Some(Self::APPLY_BLOCK_TIMEOUT),
            Some(IpcCmdServer::IO_TIMEOUT),
        )? {
            NodeMessage::ApplyBlockResult(result) => {
                result.map_err(|err| ProtocolError::ApplyBlockError { reason: err }.into())
            }
            message => Err(ProtocolServiceError::UnexpectedMessage {
                message: message.into(),
            }),
        }
    }

    /// Begin application
    pub fn assert_encoding_for_protocol_data(
        &self,
        protocol_hash: ProtocolHash,
        protocol_data: RustBytes,
    ) -> Result<(), ProtocolServiceError> {
        let mut io = self.io.borrow_mut();
        io.tx
            .send(&ProtocolMessage::AssertEncodingForProtocolDataCall(
                protocol_hash,
                protocol_data,
            ))?;

        // this might take a while, so we will use unusually long timeout
        match io.rx.try_receive(
            Some(Self::ASSERT_ENCODING_FOR_PROTOCOL_DATA_TIMEOUT),
            Some(IpcCmdServer::IO_TIMEOUT),
        )? {
            NodeMessage::AssertEncodingForProtocolDataResult(result) => result.map_err(|err| {
                ProtocolError::AssertEncodingForProtocolDataError { reason: err }.into()
            }),
            message => Err(ProtocolServiceError::UnexpectedMessage {
                message: message.into(),
            }),
        }
    }

    /// Begin application
    pub fn begin_application(
        &self,
        request: BeginApplicationRequest,
    ) -> Result<BeginApplicationResponse, ProtocolServiceError> {
        let mut io = self.io.borrow_mut();
        io.tx
            .send(&ProtocolMessage::BeginApplicationCall(request))?;

        // this might take a while, so we will use unusually long timeout
        match io.rx.try_receive(
            Some(Self::BEGIN_APPLICATION_TIMEOUT),
            Some(IpcCmdServer::IO_TIMEOUT),
        )? {
            NodeMessage::BeginApplicationResult(result) => {
                result.map_err(|err| ProtocolError::BeginApplicationError { reason: err }.into())
            }
            message => Err(ProtocolServiceError::UnexpectedMessage {
                message: message.into(),
            }),
        }
    }

    /// Begin construction
    pub fn begin_construction(
        &self,
        request: BeginConstructionRequest,
    ) -> Result<PrevalidatorWrapper, ProtocolServiceError> {
        let mut io = self.io.borrow_mut();
        io.tx
            .send(&ProtocolMessage::BeginConstructionCall(request))?;

        // this might take a while, so we will use unusually long timeout
        match io.rx.try_receive(
            Some(Self::BEGIN_CONSTRUCTION_TIMEOUT),
            Some(IpcCmdServer::IO_TIMEOUT),
        )? {
            NodeMessage::BeginConstructionResult(result) => {
                result.map_err(|err| ProtocolError::BeginConstructionError { reason: err }.into())
            }
            message => Err(ProtocolServiceError::UnexpectedMessage {
                message: message.into(),
            }),
        }
    }

    /// Validate operation
    pub fn validate_operation(
        &self,
        request: ValidateOperationRequest,
    ) -> Result<ValidateOperationResponse, ProtocolServiceError> {
        let mut io = self.io.borrow_mut();
        io.tx
            .send(&ProtocolMessage::ValidateOperationCall(request))?;

        // this might take a while, so we will use unusually long timeout
        match io.rx.try_receive(
            Some(Self::VALIDATE_OPERATION_TIMEOUT),
            Some(IpcCmdServer::IO_TIMEOUT),
        )? {
            NodeMessage::ValidateOperationResponse(result) => {
                result.map_err(|err| ProtocolError::ValidateOperationError { reason: err }.into())
            }
            message => Err(ProtocolServiceError::UnexpectedMessage {
                message: message.into(),
            }),
        }
    }

    /// ComputePath
    pub fn compute_path(
        &self,
        request: ComputePathRequest,
    ) -> Result<ComputePathResponse, ProtocolServiceError> {
        let mut io = self.io.borrow_mut();
        io.tx.send(&ProtocolMessage::ComputePathCall(request))?;

        // this might take a while, so we will use unusually long timeout
        match io.rx.try_receive(
            Some(Self::COMPUTE_PATH_TIMEOUT),
            Some(IpcCmdServer::IO_TIMEOUT),
        )? {
            NodeMessage::ComputePathResponse(result) => {
                result.map_err(|err| ProtocolError::ComputePathError { reason: err }.into())
            }
            message => Err(ProtocolServiceError::UnexpectedMessage {
                message: message.into(),
            }),
        }
    }

    /// Call protocol  rpc - internal
    fn call_protocol_rpc_internal(
        &self,
        msg: ProtocolMessage,
    ) -> Result<ProtocolRpcResponse, ProtocolServiceError> {
        let mut io = self.io.borrow_mut();
        io.tx.send(&msg)?;

        // this might take a while, so we will use unusually long timeout
        match io.rx.try_receive(
            Some(Self::CALL_PROTOCOL_RPC_TIMEOUT),
            Some(IpcCmdServer::IO_TIMEOUT),
        )? {
            NodeMessage::RpcResponse(result) => {
                result.map_err(|err| ProtocolError::ProtocolRpcError { reason: err }.into())
            }
            message => Err(ProtocolServiceError::UnexpectedMessage {
                message: message.into(),
            }),
        }
    }

    /// Call protocol rpc
    pub fn call_protocol_rpc(
        &self,
        request: ProtocolRpcRequest,
    ) -> Result<ProtocolRpcResponse, ProtocolServiceError> {
        self.call_protocol_rpc_internal(ProtocolMessage::ProtocolRpcCall(request))
    }

    /// Call helpers_preapply_* shell service - internal
    fn call_helpers_preapply_internal(
        &self,
        msg: ProtocolMessage,
    ) -> Result<HelpersPreapplyResponse, ProtocolServiceError> {
        let mut io = self.io.borrow_mut();
        io.tx.send(&msg)?;

        // this might take a while, so we will use unusually long timeout
        match io.rx.try_receive(
            Some(Self::CALL_PROTOCOL_RPC_TIMEOUT),
            Some(IpcCmdServer::IO_TIMEOUT),
        )? {
            NodeMessage::HelpersPreapplyResponse(result) => {
                result.map_err(|err| ProtocolError::HelpersPreapplyError { reason: err }.into())
            }
            message => Err(ProtocolServiceError::UnexpectedMessage {
                message: message.into(),
            }),
        }
    }

    /// Call helpers_preapply_operations shell service
    pub fn helpers_preapply_operations(
        &self,
        request: ProtocolRpcRequest,
    ) -> Result<HelpersPreapplyResponse, ProtocolServiceError> {
        self.call_helpers_preapply_internal(ProtocolMessage::HelpersPreapplyOperationsCall(request))
    }

    /// Call helpers_preapply_block shell service
    pub fn helpers_preapply_block(
        &self,
        request: HelpersPreapplyBlockRequest,
    ) -> Result<HelpersPreapplyResponse, ProtocolServiceError> {
        self.call_helpers_preapply_internal(ProtocolMessage::HelpersPreapplyBlockCall(request))
    }

    /// Change tezos runtime configuration
    pub fn change_runtime_configuration(
        &self,
        settings: TezosRuntimeConfiguration,
    ) -> Result<(), ProtocolServiceError> {
        let mut io = self.io.borrow_mut();
        io.tx
            .send(&ProtocolMessage::ChangeRuntimeConfigurationCall(settings))?;

        match io.rx.try_receive(
            Some(IpcCmdServer::IO_TIMEOUT),
            Some(IpcCmdServer::IO_TIMEOUT),
        )? {
            NodeMessage::ChangeRuntimeConfigurationResult(result) => result.map_err(|err| {
                ProtocolError::TezosRuntimeConfigurationError { reason: err }.into()
            }),
            message => Err(ProtocolServiceError::UnexpectedMessage {
                message: message.into(),
            }),
        }
    }

    /// Command tezos ocaml code to initialize context and protocol.
    /// CommitGenesisResult is returned only if commit_genesis is set to true
    fn init_protocol_context(
        &self,
        storage_data_dir: String,
        tezos_environment: &TezosEnvironmentConfiguration,
        commit_genesis: bool,
        enable_testchain: bool,
        readonly: bool,
        patch_context: Option<PatchContext>,
    ) -> Result<InitProtocolContextResult, ProtocolServiceError> {
        // try to check if was at least one write success, other words, if context was already created on file system
        {
            // lock
            let lock: Arc<(Mutex<bool>, Condvar)> =
                AT_LEAST_ONE_WRITE_PROTOCOL_CONTEXT_WAS_SUCCESS_AT_FIRST_LOCK.clone();
            let &(ref lock, ref cvar) = &*lock;
            let was_one_write_success = lock.lock()?;

            // if we have already one write, we can just continue, if not we put thread to sleep and wait
            if !(*was_one_write_success) {
                if readonly {
                    // release lock here and wait
                    let _lock = cvar.wait(was_one_write_success)?;
                }
                // TODO: handle situation, thah more writes - we cannot allowed to do so, just one write can exists
            }
        }

        // call init
        let mut io = self.io.borrow_mut();
        io.tx.send(&ProtocolMessage::InitProtocolContextCall(
            InitProtocolContextParams {
                storage_data_dir,
                genesis: tezos_environment.genesis.clone(),
                genesis_max_operations_ttl: tezos_environment
                    .genesis_additional_data()
                    .max_operations_ttl,
                protocol_overrides: tezos_environment.protocol_overrides.clone(),
                commit_genesis,
                enable_testchain,
                readonly,
                turn_off_context_raw_inspector: self.configuration.event_server_path.is_none(),
                patch_context,
            },
        ))?;

        // wait for response
        // this might take a while, so we will use unusually long timeout
        match io.rx.try_receive(
            Some(Self::INIT_PROTOCOL_CONTEXT_TIMEOUT),
            Some(IpcCmdServer::IO_TIMEOUT),
        )? {
            NodeMessage::InitProtocolContextResult(result) => {
                if result.is_ok() {
                    // if context is initialized, and is not readonly, means is write, for wich we wait
                    // we check if it is the first one, if it is the first one, we can notify other threads to continue
                    if !readonly {
                        // check if first write success
                        let lock: Arc<(Mutex<bool>, Condvar)> =
                            AT_LEAST_ONE_WRITE_PROTOCOL_CONTEXT_WAS_SUCCESS_AT_FIRST_LOCK.clone();
                        let &(ref lock, ref cvar) = &*lock;
                        let mut was_one_write_success =
                            lock.lock()
                                .map_err(|error| ProtocolServiceError::LockPoisonError {
                                    message: format!("{:?}", error),
                                })?;
                        if !(*was_one_write_success) {
                            *was_one_write_success = true;
                            cvar.notify_all();
                        }
                    }
                }
                result.map_err(|err| ProtocolError::OcamlStorageInitError { reason: err }.into())
            }
            message => Err(ProtocolServiceError::UnexpectedMessage {
                message: message.into(),
            }),
        }
    }

    /// Gracefully shutdown protocol runner
    pub fn shutdown(&mut self) -> Result<(), ProtocolServiceError> {
        if self.shutting_down {
            // shutdown was already triggered before
            return Ok(());
        }
        self.shutting_down = true;

        let mut io = self.io.borrow_mut();
        io.tx.send(&ProtocolMessage::ShutdownCall)?;

        match io.rx.try_receive(
            Some(IpcCmdServer::IO_TIMEOUT),
            Some(IpcCmdServer::IO_TIMEOUT),
        )? {
            NodeMessage::ShutdownResult => Ok(()),
            message => Err(ProtocolServiceError::UnexpectedMessage {
                message: message.into(),
            }),
        }
    }

    /// Initialize protocol environment from default configuration (writeable).
    pub fn init_protocol_for_write(
        &self,
        commit_genesis: bool,
        patch_context: &Option<PatchContext>,
    ) -> Result<InitProtocolContextResult, ProtocolServiceError> {
        self.change_runtime_configuration(self.configuration.runtime_configuration().clone())?;
        self.init_protocol_context(
            self.configuration
                .data_dir()
                .to_str()
                .ok_or_else(|| ProtocolServiceError::InvalidDataError {
                    message: format!("Invalid data dir: {:?}", self.configuration.data_dir()),
                })?
                .to_string(),
            self.configuration.environment(),
            commit_genesis,
            self.configuration.enable_testchain(),
            false,
            patch_context.clone(),
        )
    }

    /// Initialize protocol environment from default configuration (readonly).
    pub fn init_protocol_for_read(
        &self,
    ) -> Result<InitProtocolContextResult, ProtocolServiceError> {
        self.change_runtime_configuration(self.configuration.runtime_configuration().clone())?;
        self.init_protocol_context(
            self.configuration
                .data_dir()
                .to_str()
                .ok_or_else(|| ProtocolServiceError::InvalidDataError {
                    message: format!("Invalid data dir: {:?}", self.configuration.data_dir()),
                })?
                .to_string(),
            self.configuration.environment(),
            false,
            self.configuration.enable_testchain(),
            true,
            None,
        )
    }

    /// Gets data for genesis.
    pub fn genesis_result_data(
        &self,
        genesis_context_hash: &ContextHash,
    ) -> Result<CommitGenesisResult, ProtocolServiceError> {
        let tezos_environment = self.configuration.environment();
        let main_chain_id = tezos_environment.main_chain_id().map_err(|e| {
            ProtocolServiceError::InvalidDataError {
                message: format!("{:?}", e),
            }
        })?;
        let protocol_hash = tezos_environment.genesis_protocol().map_err(|e| {
            ProtocolServiceError::InvalidDataError {
                message: format!("{:?}", e),
            }
        })?;

        let mut io = self.io.borrow_mut();
        io.tx.send(&ProtocolMessage::GenesisResultDataCall(
            GenesisResultDataParams {
                genesis_context_hash: genesis_context_hash.clone(),
                chain_id: main_chain_id,
                genesis_protocol_hash: protocol_hash,
                genesis_max_operations_ttl: tezos_environment
                    .genesis_additional_data()
                    .max_operations_ttl,
            },
        ))?;

        match io.rx.try_receive(
            Some(IpcCmdServer::IO_TIMEOUT),
            Some(IpcCmdServer::IO_TIMEOUT),
        )? {
            NodeMessage::CommitGenesisResultData(result) => {
                result.map_err(|err| ProtocolError::GenesisResultDataError { reason: err }.into())
            }
            message => Err(ProtocolServiceError::UnexpectedMessage {
                message: message.into(),
            }),
        }
    }
}

impl Drop for ProtocolController {
    fn drop(&mut self) {
        // try to gracefully shutdown protocol runner
        if !self.shutting_down {
            let _ = self.shutdown();
        }
    }
}

/// Endpoint consists of a protocol runner and IPC communication (command and event channels).
pub struct ProtocolRunnerEndpoint<Runner: ProtocolRunner> {
    runner: Runner,
    log: Logger,

    pub commands: IpcCmdServer,
}

impl<Runner: ProtocolRunner + 'static> ProtocolRunnerEndpoint<Runner> {
    pub fn try_new(
        endpoint_name: &str,
        configuration: ProtocolEndpointConfiguration,
        log: Logger,
    ) -> Result<ProtocolRunnerEndpoint<Runner>, IpcError> {
        let cmd_server = IpcCmdServer::try_new(configuration.clone())?;

        Ok(ProtocolRunnerEndpoint {
            runner: Runner::new(
                configuration,
                cmd_server.0.client().path(),
                endpoint_name.to_string(),
            ),
            commands: cmd_server,
            log,
        })
    }

    /// Starts protocol runner sub-process just once and you can take care of it
    pub fn start(&self) -> Result<Runner::Subprocess, ProtocolRunnerError> {
        debug!(self.log, "Starting protocol runner process");
        self.runner.spawn()
    }
}
