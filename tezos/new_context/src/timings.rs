// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crypto::hash::{BlockHash, ContextHash, OperationHash};
use ocaml_interop::*;
use tezos_api::ocaml_conv::{OCamlBlockHash, OCamlContextHash, OCamlOperationHash};
use tezos_timing::{Action, ActionKind, TimingMessage, TIMING_CHANNEL};

pub fn set_block(rt: &OCamlRuntime, block_hash: OCamlRef<Option<OCamlBlockHash>>) {
    let instant = Instant::now();
    let block_hash: Option<BlockHash> = block_hash.to_rust(rt);

    let timestamp = if block_hash.is_some() {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::new(0, 0));
        Some(timestamp)
    } else {
        None
    };

    if let Err(e) = TIMING_CHANNEL.send(TimingMessage::SetBlock {
        block_hash,
        timestamp,
        instant,
    }) {
        eprintln!("Timing set_block hook error = {:?}", e);
    }
}

pub fn set_operation(rt: &OCamlRuntime, operation_hash: OCamlRef<Option<OCamlOperationHash>>) {
    let operation_hash: Option<OperationHash> = operation_hash.to_rust(rt);

    if let Err(e) = TIMING_CHANNEL.send(TimingMessage::SetOperation(operation_hash)) {
        eprintln!("Timing set_operation hook error = {:?}", e);
    }
}

pub fn checkout(
    rt: &OCamlRuntime,
    context_hash: OCamlRef<OCamlContextHash>,
    irmin_time: f64,
    tezedge_time: f64,
) {
    let context_hash: ContextHash = context_hash.to_rust(rt);
    let irmin_time = get_time(irmin_time);
    let tezedge_time = get_time(tezedge_time);

    if let Err(e) = TIMING_CHANNEL.send(TimingMessage::Checkout {
        context_hash,
        irmin_time,
        tezedge_time,
    }) {
        eprintln!("Timing checkout hook error = {:?}", e);
    }
}

pub fn commit(
    _rt: &OCamlRuntime,
    _new_context_hash: OCamlRef<OCamlContextHash>,
    irmin_time: f64,
    tezedge_time: f64,
) {
    let irmin_time = get_time(irmin_time);
    let tezedge_time = get_time(tezedge_time);

    if let Err(e) = TIMING_CHANNEL.send(TimingMessage::Commit {
        irmin_time,
        tezedge_time,
    }) {
        eprintln!("Timing commit hook error = {:?}", e);
    }
}

pub fn context_action(
    rt: &OCamlRuntime,
    action_name: OCamlRef<String>,
    key: OCamlRef<OCamlList<String>>,
    irmin_time: f64,
    tezedge_time: f64,
) {
    let action_name = rt.get(action_name);
    let action_name = match action_name.as_bytes() {
        b"mem" => ActionKind::Mem,
        b"mem_tree" => ActionKind::MemTree,
        b"find" => ActionKind::Find,
        b"find_tree" => ActionKind::FindTree,
        b"add" => ActionKind::Add,
        b"add_tree" => ActionKind::AddTree,
        b"remove" => ActionKind::Remove,
        _ => return,
    };
    let irmin_time = get_time(irmin_time);
    let tezedge_time = get_time(tezedge_time);

    let key: Vec<String> = key.to_rust(rt);

    let action = Action {
        action_name,
        key,
        irmin_time,
        tezedge_time,
    };

    if let Err(e) = TIMING_CHANNEL.send(TimingMessage::Action(action)) {
        eprintln!("Timing context_action hook error = {:?}", e);
    }
}

pub fn init_timing(db_path: String) {
    if let Err(e) = TIMING_CHANNEL.send(TimingMessage::InitTiming {
        db_path: Some(db_path.into()),
    }) {
        eprintln!("Timing init_timing hook error = {:?}", e);
    }
}

fn get_time(time: f64) -> Option<f64> {
    match time {
        t if t < 0.0 => None,
        t => Some(t),
    }
}
