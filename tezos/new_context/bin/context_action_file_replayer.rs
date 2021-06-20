// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::{collections::HashMap, io::prelude::*};
use std::{convert::TryFrom, fs::OpenOptions};
use std::{fs, path::PathBuf};

use anyhow::Error;
use clap::{App, Arg};
use crypto::hash::{BlockHash, ContextHash};
use slog::{debug, info, warn, Drain, Level, Logger};

use tezos_api::ffi::TezosContextTezEdgeStorageConfiguration;
use tezos_new_context::initializer::initialize_tezedge_context;
use tezos_new_context::kv_store::SupportedContextKeyValueStore;
use tezos_new_context::working_tree::working_tree::WorkingTree;
use tezos_new_context::working_tree::working_tree_stats::MerkleStorageAction;
use tezos_new_context::working_tree::working_tree_stats::OperationLatencyStats;
use tezos_new_context::{
    actions::action_file::ActionsFileReader, initializer::ContextKvStoreConfiguration,
};
use tezos_new_context::{
    actions::{get_new_tree_hash, get_tree_id, ContextAction},
    TreeId,
};
use tezos_new_context::{IndexApi, ProtocolContextApi, ShellContextApi, TezedgeContext};

struct Args {
    blocks_per_cycle: usize,
    blocks_limit: Option<usize>,
    input: PathBuf,
    output: PathBuf,
    context_kv_store: ContextKvStoreConfiguration,
}

impl Args {
    pub fn read_args() -> Self {
        let app = App::new("storage-stats")
            .about("Replay context action file and generate statistics for merkle storage")
            .arg(Arg::with_name("input")
                .long("input")
                .takes_value(true)
                .required(true)
                .help("Path to the actions.bin"))
            .arg(Arg::with_name("cycle-size")
                .long("cycle_size")
                .takes_value(true)
                .required(true)
                .default_value("2048")
                .help("Number of blocks in cycle"))
            .arg(Arg::with_name("blocks_limit")
                .takes_value(true)
                .long("blocks_limit")
                .help("Limits number of processed blocks"))
            .arg(Arg::with_name("output")
                .takes_value(true)
                .long("output")
                .required(true)
                .help("Output path for temp data and generated result statistics"))
            .arg(Arg::with_name("context-kv-store")
                .long("context-kv-store")
                .takes_value(true)
                .value_name("STRING")
                .required(true)
                .default_value("inmem-gc")
                .possible_values(&SupportedContextKeyValueStore::possible_values())
                .help("Choose the merkle storege backend - supported backends: 'inmem', 'inmem-gc', 'btree'"));

        let matches = app.get_matches();

        let out_dir = matches
            .value_of("output")
            .unwrap()
            .parse::<PathBuf>()
            .expect("Provided value cannot be converted to path");

        Self {
            blocks_per_cycle: matches
                .value_of("cycle-size")
                .map(|s| s.parse::<usize>().unwrap())
                .unwrap(),
            context_kv_store: matches
                .value_of("context-kv-store")
                .unwrap()
                .parse::<SupportedContextKeyValueStore>()
                .map(|v| match v {
                    SupportedContextKeyValueStore::InMem => ContextKvStoreConfiguration::InMem,
                    SupportedContextKeyValueStore::InMemGC => ContextKvStoreConfiguration::InMemGC,
                    SupportedContextKeyValueStore::BTreeMap => {
                        ContextKvStoreConfiguration::BTreeMap
                    }
                })
                .unwrap_or_else(|e| {
                    panic!(
                        "Expecting one value from {:?}, error: {:?}",
                        SupportedContextKeyValueStore::possible_values(),
                        e
                    )
                }),
            blocks_limit: matches
                .value_of("blocks_limit")
                .map(|s| s.parse::<usize>().unwrap()),
            output: out_dir,
            input: matches
                .value_of("input")
                .unwrap()
                .parse::<PathBuf>()
                .expect("Provided value cannot be converted to path"),
        }
    }
}

pub fn get_tree_action(action: &ContextAction) -> String {
    match action {
        ContextAction::Get { .. } => "ContextAction::Get".to_string(),
        ContextAction::Mem { .. } => "ContextAction::Mem".to_string(),
        ContextAction::DirMem { .. } => "ContextAction::DirMem".to_string(),
        ContextAction::Set { .. } => "ContextAction::Set".to_string(),
        ContextAction::Copy { .. } => "ContextAction::Copy".to_string(),
        ContextAction::Delete { .. } => "ContextAction::Delete".to_string(),
        ContextAction::RemoveRecursively { .. } => "ContextAction::RemoveRecursively".to_string(),
        ContextAction::Commit { .. } => "ContextAction::Commit".to_string(),
        ContextAction::Fold { .. } => "ContextAction::Fold".to_string(),
        ContextAction::Checkout { .. } => "ContextAction::Checkout".to_string(),
        ContextAction::Shutdown { .. } => "ContextAction::Shutdown".to_string(),
    }
}

fn create_logger() -> Logger {
    let drain = slog_async::Async::new(
        slog_term::FullFormat::new(slog_term::TermDecorator::new().build())
            .build()
            .fuse(),
    )
    .chan_size(32768)
    .overflow_strategy(slog_async::OverflowStrategy::Block)
    .build()
    .filter_level(Level::Debug)
    .fuse();

    Logger::root(drain, slog::o!())
}

// process actionfile without deselializing blocks
// in order to get count of blocks
fn get_blocks_count(log: &Logger, path: PathBuf) -> Result<u32, Error> {
    let file = OpenOptions::new()
        .write(false)
        .create(false)
        .read(true)
        .open(path)?;

    let mut reader = BufReader::new(file);
    let mut pos = 0_u64;
    let mut block_size = [0_u8; 4];
    let mut counter = 0;
    loop {
        if reader.seek(SeekFrom::Start(pos)).is_err() {
            warn!(log, "missing block operations information");
            break;
        }
        if reader.read_exact(&mut block_size).is_err() {
            break;
        }
        // skips header
        pos += block_size.len() as u64;
        // skips block
        pos += u32::from_be_bytes(block_size) as u64;
        counter += 1;
    }

    Ok(counter)
}

struct StatsWriter {
    output: File,
    block_latencies_total: usize,
    merkle_actions: Vec<MerkleStorageAction>,
}

impl StatsWriter {
    fn new(output: File) -> Self {
        let mut rv = Self {
            output,
            block_latencies_total: 0,
            merkle_actions: vec![
                MerkleStorageAction::Set,
                MerkleStorageAction::Get,
                MerkleStorageAction::GetByPrefix,
                MerkleStorageAction::GetKeyValuesByPrefix,
                MerkleStorageAction::GetContextTreeByPrefix,
                MerkleStorageAction::GetHistory,
                MerkleStorageAction::Mem,
                MerkleStorageAction::DirMem,
                MerkleStorageAction::Copy,
                MerkleStorageAction::Delete,
                MerkleStorageAction::DeleteRecursively,
                MerkleStorageAction::Commit,
                MerkleStorageAction::Checkout,
                MerkleStorageAction::BlockApplied,
            ],
        };
        rv.write_header();
        rv
    }

    fn generate_stats_for_merkle_action(
        &self,
        action: MerkleStorageAction,
        stats: &OperationLatencyStats,
    ) -> String {
        match stats.get(&action) {
            Some(v) => {
                format!(
                    "{} {} {} {}",
                    v.cumul_op_exec_time, v.avg_exec_time, v.op_exec_time_min, v.op_exec_time_max
                )
            }
            None => {
                format!("{} {} {} {}", 0, 0, 0, 0)
            }
        }
    }

    fn write_header(&mut self) {
        let header: String = self
            .merkle_actions
            .iter()
            .map(|action| match action {
                MerkleStorageAction::Set => "Set",
                MerkleStorageAction::Get => "Get",
                MerkleStorageAction::GetByPrefix => "GetByPrefix",
                MerkleStorageAction::GetKeyValuesByPrefix => "GetKeyValuesByPrefix",
                MerkleStorageAction::GetContextTreeByPrefix => "GetContextTreeByPrefix",
                MerkleStorageAction::GetHistory => "GetHistory",
                MerkleStorageAction::Mem => "Mem",
                MerkleStorageAction::DirMem => "DirMem",
                MerkleStorageAction::Copy => "Copy",
                MerkleStorageAction::Delete => "Delete",
                MerkleStorageAction::DeleteRecursively => "DeleteRecursively",
                MerkleStorageAction::Commit => "Commit",
                MerkleStorageAction::Checkout => "Checkout",
                MerkleStorageAction::BlockApplied => "BlockApplied",
            })
            .map(|action| {
                format!(
                    "{}_total {}_avg {}_min {}_max ",
                    action, action, action, action
                )
            })
            .collect();

        writeln!(&mut self.output, "block mem block_latency time {}", header).unwrap();
        self.output.flush().unwrap();
    }

    fn update(&mut self, block_nr: usize, _tree: &WorkingTree) {
        // TODO: this will not do anything as it is now
        let report =
            tezos_new_context::working_tree::working_tree_stats::MerkleStoragePerfReport::default();
        let usage = report.kv_store_stats;
        let block_latency = 0;
        self.block_latencies_total += block_latency as usize;

        let stats: String = format!(
            "{} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} ",
            block_nr,
            usage,
            block_latency,
            self.block_latencies_total,
            self.generate_stats_for_merkle_action(
                MerkleStorageAction::Set,
                &report.perf_stats.global,
            ),
            self.generate_stats_for_merkle_action(
                MerkleStorageAction::Get,
                &report.perf_stats.global,
            ),
            self.generate_stats_for_merkle_action(
                MerkleStorageAction::GetByPrefix,
                &report.perf_stats.global,
            ),
            self.generate_stats_for_merkle_action(
                MerkleStorageAction::GetKeyValuesByPrefix,
                &report.perf_stats.global,
            ),
            self.generate_stats_for_merkle_action(
                MerkleStorageAction::GetContextTreeByPrefix,
                &report.perf_stats.global,
            ),
            self.generate_stats_for_merkle_action(
                MerkleStorageAction::GetHistory,
                &report.perf_stats.global,
            ),
            self.generate_stats_for_merkle_action(
                MerkleStorageAction::Mem,
                &report.perf_stats.global,
            ),
            self.generate_stats_for_merkle_action(
                MerkleStorageAction::DirMem,
                &report.perf_stats.global,
            ),
            self.generate_stats_for_merkle_action(
                MerkleStorageAction::Copy,
                &report.perf_stats.global,
            ),
            self.generate_stats_for_merkle_action(
                MerkleStorageAction::Delete,
                &report.perf_stats.global,
            ),
            self.generate_stats_for_merkle_action(
                MerkleStorageAction::DeleteRecursively,
                &report.perf_stats.global,
            ),
            self.generate_stats_for_merkle_action(
                MerkleStorageAction::Commit,
                &report.perf_stats.global,
            ),
            self.generate_stats_for_merkle_action(
                MerkleStorageAction::Checkout,
                &report.perf_stats.global,
            ),
            self.generate_stats_for_merkle_action(
                MerkleStorageAction::BlockApplied,
                &report.perf_stats.global,
            )
        );

        writeln!(&mut self.output, "{}", stats).unwrap();
    }
}

pub trait ActionKeySlices {
    fn as_slices(&self) -> Vec<&str>;
}

impl ActionKeySlices for Vec<String> {
    fn as_slices(&self) -> Vec<&str> {
        self.iter().map(String::as_str).collect()
    }
}

/// Resolve name and store path (if supports)
fn resolve_context_kv_store(
    context_kv_store_configuration: &ContextKvStoreConfiguration,
) -> (String, Option<PathBuf>) {
    match context_kv_store_configuration {
        ContextKvStoreConfiguration::InMem => ("inmem".to_string(), None),
        ContextKvStoreConfiguration::InMemGC => ("inmem-gc".to_string(), None),
        ContextKvStoreConfiguration::BTreeMap => ("btree".to_string(), None),
        ContextKvStoreConfiguration::ReadOnlyIpc => ("readonly-ipc".to_string(), None),
    }
}

fn main() -> Result<(), Error> {
    let params = Args::read_args();
    let log = create_logger();

    // prepare files
    let (context_kv_storage_name, context_kv_storage_path) =
        resolve_context_kv_store(&params.context_kv_store);

    // check actions file
    let actions_file_path = params.input;
    if !actions_file_path.exists() {
        return Err(anyhow::format_err!(
            "Input action file does not exists: {:?}",
            actions_file_path.to_str().unwrap(),
        ));
    }

    // prepare storage path (if needed)
    if let Some(context_kv_storage_path) = context_kv_storage_path {
        if context_kv_storage_path.exists() {
            let _ = fs::remove_dir_all(&context_kv_storage_path)?;
        }
        let _ = fs::create_dir_all(&context_kv_storage_path)?;
    }

    // prepare stats output file
    let stats_output_file = {
        if !params.output.exists() {
            let _ = fs::create_dir_all(&params.output)?;
        }
        let stats_output_file = params
            .output
            .join(&format!("{}.stats.txt", context_kv_storage_name));
        if stats_output_file.exists() {
            let _ = fs::remove_file(&stats_output_file)?;
        }
        stats_output_file
    };

    info!(log, "Context actions replayer starts...";
               "input_file" => actions_file_path.to_str().unwrap(),
               "output_stats_file" => stats_output_file.to_str().unwrap(),
               "target_context_kv_store_path" => params.output.to_str().unwrap(),
               "target_context_kv_store" => context_kv_storage_name);

    let configuration = TezosContextTezEdgeStorageConfiguration {
        backend: params.context_kv_store.clone(),
        ipc_socket_path: None,
    };
    let mut context = initialize_tezedge_context(&configuration)?;
    let mut contexts: HashMap<TreeId, TezedgeContext> = Default::default();
    let mut stat_writer = StatsWriter::new(File::create(stats_output_file)?);

    let mut counter = 0;
    let mut cycle_counter = 0;
    let blocks_count = get_blocks_count(&log, actions_file_path.clone())?;

    info!(log, "{} blocks found", blocks_count);

    let actions_reader = ActionsFileReader::new(&actions_file_path)?;

    for messages in actions_reader.take(params.blocks_limit.unwrap_or(blocks_count as usize)) {
        counter += 1;
        let progress = counter as f64 / blocks_count as f64 * 100.0;

        for action in messages.iter() {
            // evaluate context action to context
            let (new_context, new_tree_id) =
                perform_context_action(context, &mut contexts, action.clone())?;

            context = new_context.clone();

            // Checkout happened, reset tree id cache
            if let Some(0) = new_tree_id {
                contexts = Default::default();
            }

            // New tree was produced, add to contexts map for backtracking
            if let Some(tree_id) = new_tree_id {
                contexts.insert(tree_id, new_context);
            }

            // verify state of the storage after action has been applied
            match action {
                ContextAction::Checkout { context_hash, .. } => {
                    assert!(!context_hash.is_empty());
                    assert_eq!(
                        context_hash.clone(),
                        context.get_last_commit_hash()?.unwrap()
                    );
                }
                ContextAction::Get { key, value, .. } => {
                    // FIXME: value can be `None`, fix ContextAction::Get
                    assert_eq!(
                        value.clone(),
                        context.find(&key.as_slices()).unwrap().unwrap()
                    );
                }
                ContextAction::Mem { key, value, .. } => {
                    assert_eq!(*value, context.mem(&key.as_slices()).unwrap());
                }
                ContextAction::DirMem { key, value, .. } => {
                    assert_eq!(*value, context.mem_tree(&key.as_slices()));
                }
                _ => {}
            };

            // verify context hashes after each block
            if let Some(expected_hash) = get_new_tree_hash(&action)? {
                assert_eq!(context.get_merkle_root()?, expected_hash);
            }

            if let ContextAction::Commit {
                block_hash,
                //new_context_hash,
                ..
            } = &action
            {
                debug!(
                    log,
                    "progress {:.7}% - cycle nr: {} block nr {} [{}] with {} messages processed - {} mb",
                    progress,
                    cycle_counter,
                    counter,
                    hex::encode(&block_hash.clone().unwrap().clone()),
                    messages.len(),
                    context.get_memory_usage()
                        .unwrap()
                        / 1024
                        / 1024
                );

                // TODO: probably not needed now that the commit takes care of it, remove
                //let commit_hash =
                //    ContextHash::try_from(new_context_hash.clone()).expect("Invalid commit hash");

                //context.index.block_applied(commit_hash).unwrap();
                if counter > 0 && counter % params.blocks_per_cycle == 0 {
                    context.index.cycle_started().unwrap();
                    cycle_counter += 1;
                }
            }
        }

        stat_writer.update(counter, &context.tree);
    }

    info!(log, "Context was successfully evaluated");

    Ok(())
}

fn perform_context_action(
    mut context: TezedgeContext,
    contexts: &mut HashMap<TreeId, TezedgeContext>,
    action: ContextAction,
) -> Result<(TezedgeContext, Option<TreeId>), Error> {
    let tree_id = get_tree_id(&action);
    let new_tree_hash = get_new_tree_hash(&action)?;
    let current_context = if contexts.is_empty() || tree_id.is_none() {
        &mut context
    } else {
        contexts.get_mut(&tree_id.unwrap()).expect(&format!(
            "Context with tree_id={} not found",
            tree_id.unwrap()
        ))
    };

    // Write actions produce a new context, read actions return the original context
    let (context, new_tree_id) = match action {
        ContextAction::Get { key, .. } => {
            current_context.find(&key.as_slices())?;
            (context, None)
        }
        ContextAction::Mem { key, .. } => {
            current_context.mem(&key.as_slices())?;
            (context, None)
        }
        ContextAction::DirMem { key, .. } => {
            current_context.mem_tree(&key.as_slices());
            (context, None)
        }
        ContextAction::Set {
            key,
            value,
            new_tree_id,
            ..
        } => (
            current_context.add(&key.as_slices(), value)?,
            Some(new_tree_id),
        ),
        ContextAction::Copy {
            to_key: key,
            from_key,
            new_tree_id,
            ..
        } => (
            current_context
                .copy(&from_key.as_slices(), &key.as_slices())?
                .unwrap_or(context),
            Some(new_tree_id),
        ),
        ContextAction::Delete {
            key, new_tree_id, ..
        } => (current_context.delete(&key.as_slices())?, Some(new_tree_id)),
        ContextAction::RemoveRecursively {
            key, new_tree_id, ..
        } => (current_context.delete(&key.as_slices())?, Some(new_tree_id)),
        ContextAction::Commit {
            new_context_hash,
            block_hash: Some(block_hash),
            author,
            message,
            date,
            ..
        } => {
            // NOTE: parent_commit_hash is not used, it is in the context already
            let block_hash = BlockHash::try_from(block_hash)?;
            let hash = current_context.commit(author, message, date)?;
            let new_context_hash = ContextHash::try_from(new_context_hash)?;
            assert_eq!(
                &hash,
                &new_context_hash,
                "Invalid context_hash for block: {}, expected: {}, but was: {}",
                block_hash.to_base58_check(),
                new_context_hash.to_base58_check(),
                hash.to_base58_check(),
            );
            (context, None)
        }

        ContextAction::Checkout { context_hash, .. } => (
            context
                .index
                .checkout(&ContextHash::try_from(context_hash.clone())?)?
                .expect(&format!("Failed checkout of commit: {:?}", context_hash)),
            Some(0),
        ),

        ContextAction::Commit { .. } => (context, None), // Ignored (no block_hash)

        ContextAction::Fold { .. } => (context, None), // Ignored

        ContextAction::Shutdown => (context, None), // Ignored
    };

    if let Some(post_hash) = new_tree_hash {
        assert_eq!(
            context.get_merkle_root()?,
            post_hash,
            "Invalid tree_hash context: {:?}, post_hash: {:?}, tree_id: {:? }",
            context.get_merkle_root()?,
            post_hash,
            tree_id,
        );
    }

    Ok((context, new_tree_id))
}
