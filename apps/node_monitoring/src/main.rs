use crate::monitors::resource::ResourceMonitor;

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::{Arc, RwLock};

use slog::{info, error, Drain, Level, Logger};
use tokio::signal;
use tokio::time::{sleep, Duration};

use procfs::ProcResult;
use procfs::process;
use procfs::process::Process;
use procfs::net::{tcp, tcp6, TcpState};

mod monitors;
mod configuration;
mod constants;
mod slack;
mod display_info;
mod node;
mod rpc;

use crate::monitors::alerts::Alerts;
use crate::monitors::resource::ResourceUtilization;
use crate::configuration::DeployMonitoringEnvironment;
use crate::constants::MEASUREMENTS_MAX_CAPACITY;


// TODO: get this from a config, and default to this
// const TEZEDGE_PORT: u16 = 18732;
const PROCESS_LOOKUP_INTERVAL: Duration = Duration::from_secs(10);

#[tokio::main]
async fn main() {
    let env = configuration::DeployMonitoringEnvironment::from_args();

    // TODO: config
    // create an slog logger
    let log = create_logger(Level::Info);

    let DeployMonitoringEnvironment {
        slack_configuration,
        tezedge_alert_thresholds,
        ocaml_alert_thresholds,
        resource_monitor_interval,
        tezedge_volume_path,
        ..
    } = env.clone();

    let slack_server = slack_configuration.map(|cfg| {
        slack::SlackServer::new(
            cfg.slack_url,
            cfg.slack_token,
            cfg.slack_channel_name,
            log.clone(),
        )
    });

    // let tezedge_node_pid = find_node_process_id();
    let pid = loop {
        if let Some(pid) = find_node_process_id() {
            break pid;
        } else {
            println!("No tezedge process found... Trying again in {}s", PROCESS_LOOKUP_INTERVAL.as_secs());
            sleep(PROCESS_LOOKUP_INTERVAL).await;
        }
    };

    println!("Found process! PID: {}", pid);

    let mut storage_map = HashMap::new();
    storage_map.insert(
        "tezedge",
        Arc::new(RwLock::new(VecDeque::<ResourceUtilization>::with_capacity(
            MEASUREMENTS_MAX_CAPACITY,
        ))),
    );

    let alerts = Alerts::new(
        tezedge_alert_thresholds,
        ocaml_alert_thresholds,
        tezedge_volume_path.to_string(),
    );
    let mut resource_monitor = ResourceMonitor::new(
        storage_map.clone(),
        HashMap::new(),
        alerts,
        log.clone(),
        slack_server,
        tezedge_volume_path.to_string(),
    );

    let thread_log = log.clone();
    let handle = tokio::spawn(async move {
        loop {
            if let Err(e) = resource_monitor.take_measurement().await {
                error!(thread_log, "Resource monitoring error: {}", e);
            }
            sleep(Duration::from_secs(resource_monitor_interval)).await;
        }
    });

    info!(log, "Starting rpc server on port {}", &env.rpc_port);
    let rpc_server_handle = rpc::spawn_rpc_server(env.rpc_port, log.clone(), storage_map);

    // wait for SIGINT
    signal::ctrl_c()
        .await
        .expect("Failed to listen for ctrl-c event");
    info!(log, "Ctrl-c or SIGINT received!");


    // drop the looping thread handles (forces exit)
    drop(handle);
    drop(rpc_server_handle);
}

/// Find the tezedge node to monitor
fn find_node_process_id() -> Option<i32> {
    let all_procs = process::all_processes().expect("No processes found on system");

    let mut process_map: HashMap<u32, &Process> = HashMap::new();

    // create mapping between processes and inodes
    for process in &all_procs {
        if let ProcResult::Ok(fds) = process.fd() {
            for fd in fds {
                if let process::FDTarget::Socket(inode) = fd.target {
                    process_map.insert(inode, process);
                }
            }
        }
    }

    // get the tcp table
    let tcp = tcp().expect("Cannot get the tcp table");
    let tcp6 = tcp6().expect("Cannot get the tcp6 table");

    for entry in tcp.into_iter().chain(tcp6) {
        if entry.local_address.port() == TEZEDGE_PORT && entry.state == TcpState::Listen {
            if let Some(process) = process_map.get(&entry.inode) {
                return Some(process.pid)
            }
        }
    }
    None
}

/// Creates a slog Logger
fn create_logger(level: Level) -> Logger {
    let drain = slog_async::Async::new(
        slog_term::FullFormat::new(slog_term::TermDecorator::new().build())
            .build()
            .fuse(),
    )
    .chan_size(32768)
    .overflow_strategy(slog_async::OverflowStrategy::Block)
    .build()
    .filter_level(level)
    .fuse();
    Logger::root(drain, slog::o!())
}
