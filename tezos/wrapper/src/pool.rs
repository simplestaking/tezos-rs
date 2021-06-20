// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::fmt::Formatter;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use std::{error, fmt};

use core::marker::PhantomData;
use r2d2::{CustomizeConnection, HandleError, ManageConnection};
use slog::{debug, error, info, o, warn, Logger};

use ipc::IpcError;

use crate::runner::{ProtocolRunner, ProtocolRunnerError};
use crate::service::{ProtocolController, ProtocolRunnerEndpoint, ProtocolServiceError};
use crate::ProtocolEndpointConfiguration;

/// Possible errors for storage
#[derive(Debug)]
pub enum PoolError {
    SpawnRunnerError {
        error: ProtocolRunnerError,
    },
    IpcError {
        reason: String,
        error: IpcError,
    },
    InitContextError {
        message: String,
        error: ProtocolServiceError,
    },
}

impl std::error::Error for PoolError {}

impl fmt::Display for PoolError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        match *self {
            PoolError::SpawnRunnerError { ref error } => write!(f, "Create pool connection error - fail to spawn sub-process, reason: {:?}", error),
            PoolError::IpcError { ref reason, ref error } => write!(f, "Create pool connection IPC error - {}, error: {:?}", reason, error),
            PoolError::InitContextError { ref message, ref error } => write!(f, "Create pool connection error - fail to initialize context, message: {}, reason: {:?}", message, error),
        }
    }
}

/// Protocol runner sub-process wrapper which acts as connection
pub struct ProtocolRunnerConnection<Runner: ProtocolRunner> {
    pub api: ProtocolController,
    subprocess: Runner::Subprocess,
    log: Logger,
    pub name: String,

    /// Indicates that we want to release this connection on return to pool (used for gracefull shutdown)
    release_on_return_to_pool: bool,
}

impl<Runner: ProtocolRunner + 'static> ProtocolRunnerConnection<Runner> {
    pub fn is_valid(&mut self) -> Result<(), PoolError> {
        // TODO: check if unix socket is connected?
        Ok(())
    }

    fn has_broken(&mut self) -> bool {
        if self.release_on_return_to_pool {
            return true;
        }
        let is_subprocess_running = Runner::is_running(&mut self.subprocess);
        !is_subprocess_running
    }

    pub fn terminate_subprocess(&mut self) {
        // try shutdown gracefully
        if let Err(e) = self.api.shutdown() {
            warn!(self.log, "Failed to shutdown protocol runner gracefully"; "reason" => e);
        };

        // try terminate sub-process (if running)
        if let Err(e) = Runner::wait_and_terminate_ref(
            &mut self.subprocess,
            Runner::PROCESS_TERMINATE_WAIT_TIMEOUT,
        ) {
            warn!(self.log, "Failed to terminate/kill protocol runner"; "reason" => e);
        }
    }

    /// Mark connection as "destroy" when return back to pool
    pub fn set_release_on_return_to_pool(&mut self) {
        self.release_on_return_to_pool = true;
    }
}

/// Connection manager, which creates new connections:
/// - runs new sub-process
/// - starts IPC accept
pub struct ProtocolRunnerManager<Runner: ProtocolRunner> {
    pool_name: String,
    pool_name_counter: AtomicUsize,
    pool_connection_timeout: Duration,

    pub endpoint_cfg: ProtocolEndpointConfiguration,
    pub log: Logger,
    _phantom: PhantomData<Runner>,
}

impl<Runner: ProtocolRunner + 'static> ProtocolRunnerManager<Runner> {
    const MIN_ACCEPT_TIMEOUT: Duration = Duration::from_secs(3);

    pub fn new(
        pool_name: String,
        pool_connection_timeout: Duration,
        endpoint_cfg: ProtocolEndpointConfiguration,
        log: Logger,
    ) -> Self {
        Self {
            pool_name,
            pool_name_counter: AtomicUsize::new(1),
            pool_connection_timeout,
            endpoint_cfg,
            log,
            _phantom: PhantomData,
        }
    }

    pub fn create_connection(&self) -> Result<ProtocolRunnerConnection<Runner>, PoolError> {
        let endpoint_name = format!(
            "{}_{:?}",
            &self.pool_name,
            self.pool_name_counter.fetch_add(1, Ordering::SeqCst)
        );

        // crate protocol runner endpoint
        let protocol_endpoint = ProtocolRunnerEndpoint::<Runner>::try_new(
            &endpoint_name,
            self.endpoint_cfg.clone(),
            self.log.new(o!("endpoint" => endpoint_name.clone())),
        )
        .map_err(|error| PoolError::IpcError {
            reason: "fail to setup protocol runner endpoint".to_string(),
            error,
        })?;

        // start sub-process
        let (mut subprocess, mut protocol_commands) = match protocol_endpoint.start() {
            Ok(subprocess) => {
                let ProtocolRunnerEndpoint { commands, .. } = protocol_endpoint;
                (subprocess, commands)
            }
            Err(e) => {
                error!(self.log, "Failed to spawn protocol runner process"; "endpoint" => endpoint_name, "reason" => &e);
                return Err(PoolError::SpawnRunnerError { error: e });
            }
        };

        // count accept_timeout according to connection_timeout
        let accept_timeout = match self.pool_connection_timeout.checked_div(3) {
            Some(conn_timeout) => std::cmp::max(Self::MIN_ACCEPT_TIMEOUT, conn_timeout),
            None => Self::MIN_ACCEPT_TIMEOUT,
        };

        // start IPC connection
        let api = match protocol_commands.try_accept(accept_timeout) {
            Ok(controller) => controller,
            Err(e) => {
                error!(self.log, "Failed to accept IPC connection on sub-process (so terminate sub-process)"; "endpoint" => endpoint_name, "reason" => format!("{:?}", &e));
                // try terminate sub-process (if running)
                if let Err(e) = Runner::wait_and_terminate_ref(
                    &mut subprocess,
                    Runner::PROCESS_TERMINATE_WAIT_TIMEOUT,
                ) {
                    warn!(self.log, "Failed to terminate/kill protocol runner (create connection)"; "reason" => e);
                }
                return Err(PoolError::IpcError {
                    reason: "fail to accept IPC for sub-process".to_string(),
                    error: e,
                });
            }
        };

        debug!(self.log, "Connection for protocol runner was created successfully"; "endpoint" => endpoint_name.clone());
        Ok(ProtocolRunnerConnection {
            api,
            subprocess,
            log: self.log.new(o!("endpoint" => endpoint_name.clone())),
            name: endpoint_name,
            release_on_return_to_pool: false,
        })
    }
}

impl<Runner: ProtocolRunner + 'static> ManageConnection for ProtocolRunnerManager<Runner> {
    type Connection = ProtocolRunnerConnection<Runner>;
    type Error = PoolError;

    fn connect(&self) -> Result<ProtocolRunnerConnection<Runner>, PoolError> {
        self.create_connection()
    }

    fn is_valid(&self, conn: &mut ProtocolRunnerConnection<Runner>) -> Result<(), PoolError> {
        conn.is_valid()
    }

    fn has_broken(&self, conn: &mut ProtocolRunnerConnection<Runner>) -> bool {
        conn.has_broken()
    }
}

/// Custom connection initializer, which tries to initialize readonly context
#[derive(Debug)]
pub struct InitReadonlyContextProtocolRunnerConnectionCustomizer;

impl<Runner: ProtocolRunner + 'static>
    CustomizeConnection<ProtocolRunnerConnection<Runner>, PoolError>
    for InitReadonlyContextProtocolRunnerConnectionCustomizer
{
    fn on_acquire(&self, conn: &mut ProtocolRunnerConnection<Runner>) -> Result<(), PoolError> {
        match conn.api.init_protocol_for_read() {
            Ok(_) => {
                info!(conn.log, "Connection for protocol runner was successfully initialized with readonly context");
                Ok(())
            }
            Err(e) => {
                error!(conn.log, "Failed to initialize readonly context (so terminate sub-process)"; "reason" => format!("{:?}", &e));
                conn.terminate_subprocess();
                Err(PoolError::InitContextError {
                    message: String::from("readonly_context"),
                    error: e,
                })
            }
        }
    }

    fn on_release(&self, mut conn: ProtocolRunnerConnection<Runner>) {
        debug!(conn.log, "Closing connection for protocol runner");
        conn.terminate_subprocess();
        info!(
            conn.log,
            "Connection for protocol runner was successfully released"
        );
    }
}

/// Custom connection customizer, which just kills protocol_runner sub-process
#[derive(Debug)]
pub struct NoopProtocolRunnerConnectionCustomizer;

impl<Runner: ProtocolRunner + 'static>
    CustomizeConnection<ProtocolRunnerConnection<Runner>, PoolError>
    for NoopProtocolRunnerConnectionCustomizer
{
    fn on_acquire(&self, conn: &mut ProtocolRunnerConnection<Runner>) -> Result<(), PoolError> {
        info!(
            conn.log,
            "Connection for protocol runner was successfully initialized"
        );
        Ok(())
    }

    fn on_release(&self, mut conn: ProtocolRunnerConnection<Runner>) {
        debug!(conn.log, "Closing connection for protocol runner");
        conn.terminate_subprocess();
        info!(
            conn.log,
            "Connection for protocol runner was successfully released"
        );
    }
}

/// Custom error logging handler, which logs errors with slog logger
#[derive(Clone, Debug)]
pub struct SlogErrorHandler(Logger, String);

impl SlogErrorHandler {
    pub fn new(logger: Logger, pool_name: String) -> Self {
        Self(logger, pool_name)
    }
}

impl<E> HandleError<E> for SlogErrorHandler
where
    E: error::Error,
{
    fn handle_error(&self, error: E) {
        error!(self.0, "Connection pool error";
                       "error" => format!("{}", error),
                       "pool_name" => self.1.clone());
    }
}
