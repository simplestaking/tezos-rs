// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT
#![forbid(unsafe_code)]
#![feature(const_fn)]
#![feature(allocator_api)]

use std::convert::{TryFrom, TryInto};
use std::path::Path;
use std::sync::{Arc, RwLock};

use failure::Fail;
use rocksdb::{Cache, DB};
use serde::{Deserialize, Serialize};
use slog::{info, Logger};

use crypto::{
    base58::FromBase58CheckError,
    hash::{BlockHash, ChainId, ContextHash, FromBytesError, HashType},
};
use tezos_api::environment::{
    get_empty_operation_list_list_hash, TezosEnvironmentConfiguration, TezosEnvironmentError,
};
use tezos_api::ffi::{ApplyBlockResponse, CommitGenesisResult, PatchContext};
use tezos_messages::p2p::binary_message::{BinaryMessage, MessageHash, MessageHashError};
use tezos_messages::p2p::encoding::prelude::BlockHeader;
use tezos_messages::Head;

pub use crate::block_meta_storage::{BlockMetaStorage, BlockMetaStorageKV, BlockMetaStorageReader};
pub use crate::block_storage::{
    BlockAdditionalData, BlockAdditionalDataBuilder, BlockJsonData, BlockJsonDataBuilder,
    BlockStorage, BlockStorageReader,
};
pub use crate::chain_meta_storage::ChainMetaStorage;
use crate::context::merkle::merkle_storage::MerkleStorage;
pub use crate::mempool_storage::{MempoolStorage, MempoolStorageKV};
pub use crate::operations_meta_storage::{OperationsMetaStorage, OperationsMetaStorageKV};
pub use crate::operations_storage::{
    OperationKey, OperationsStorage, OperationsStorageKV, OperationsStorageReader,
};
pub use crate::persistent::database::{Direction, IteratorMode};
use crate::persistent::sequence::{SequenceError, Sequences};
use crate::persistent::{
    CommitLogError, CommitLogs, DBError, Decoder, Encoder, Flushable, SchemaError,
};
pub use crate::predecessor_storage::PredecessorStorage;
pub use crate::system_storage::SystemStorage;


pub mod block_meta_storage;
pub mod block_storage;
pub mod chain_meta_storage;
pub mod context;
pub mod mempool_storage;
pub mod operations_meta_storage;
pub mod operations_storage;
pub mod persistent;
pub mod predecessor_storage;
pub mod system_storage;
pub mod database;

use crate::database::db::MainDB;


/// Extension of block header with block hash
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct BlockHeaderWithHash {
    pub hash: BlockHash,
    pub header: Arc<BlockHeader>,
}

impl BlockHeaderWithHash {
    /// Create block header extensions from plain block header
    pub fn new(block_header: BlockHeader) -> Result<Self, MessageHashError> {
        Ok(BlockHeaderWithHash {
            hash: block_header.message_hash()?.try_into()?,
            header: Arc::new(block_header),
        })
    }
}

impl TryFrom<BlockHeader> for BlockHeaderWithHash {
    type Error = MessageHashError;

    fn try_from(value: BlockHeader) -> Result<Self, Self::Error> {
        BlockHeaderWithHash::new(value)
    }
}

impl Encoder for BlockHeaderWithHash {
    #[inline]
    fn encode(&self) -> Result<Vec<u8>, SchemaError> {
        let mut result = vec![];
        result.extend(self.hash.as_ref());
        result.extend(
            self.header
                .as_bytes()
                .map_err(|_| SchemaError::EncodeError)?,
        );
        Ok(result)
    }
}

impl Decoder for BlockHeaderWithHash {
    #[inline]
    fn decode(bytes: &[u8]) -> Result<Self, SchemaError> {
        if bytes.len() < HashType::BlockHash.size() {
            return Err(SchemaError::DecodeError);
        }
        let hash = bytes[0..HashType::BlockHash.size()].to_vec();
        let header = BlockHeader::from_bytes(&bytes[HashType::BlockHash.size()..])
            .map_err(|_| SchemaError::DecodeError)?;
        Ok(BlockHeaderWithHash {
            hash: hash.try_into()?,
            header: Arc::new(header),
        })
    }
}

/// Possible errors for storage
#[derive(Debug, Fail)]
pub enum StorageError {
    #[fail(display = "Database error: {}", error)]
    DBError { error: DBError },
    #[fail(display = "Database error: {}", error)]
    MainDBError { error: database::error::Error },
    #[fail(display = "Commit log error: {}", error)]
    CommitLogError { error: CommitLogError },
    #[fail(display = "Key is missing in storage")]
    MissingKey,
    #[fail(display = "Column is not valid")]
    InvalidColumn,
    #[fail(display = "Sequence generator failed: {}", error)]
    SequenceError { error: SequenceError },
    #[fail(display = "Tezos environment configuration error: {}", error)]
    TezosEnvironmentError { error: TezosEnvironmentError },
    #[fail(display = "Message hash error: {}", error)]
    MessageHashError { error: MessageHashError },
    #[fail(display = "Predecessor lookup failed")]
    PredecessorLookupError,
    #[fail(display = "Error constructing hash: {}", error)]
    HashError { error: FromBytesError },
    #[fail(display = "Error decoding hash: {}", error)]
    HashDecodeError { error: FromBase58CheckError },
}

impl From<DBError> for StorageError {
    fn from(error: DBError) -> Self {
        StorageError::DBError { error }
    }
}

impl From<database::error::Error> for StorageError {
    fn from(error: database::error::Error) -> Self {
        StorageError::MainDBError { error }
    }
}

impl From<MessageHashError> for StorageError {
    fn from(error: MessageHashError) -> Self {
        StorageError::MessageHashError { error }
    }
}

impl From<CommitLogError> for StorageError {
    fn from(error: CommitLogError) -> Self {
        StorageError::CommitLogError { error }
    }
}

impl From<SchemaError> for StorageError {
    fn from(error: SchemaError) -> Self {
        StorageError::DBError {
            error: error.into(),
        }
    }
}

impl From<SequenceError> for StorageError {
    fn from(error: SequenceError) -> Self {
        StorageError::SequenceError { error }
    }
}

impl From<TezosEnvironmentError> for StorageError {
    fn from(error: TezosEnvironmentError) -> Self {
        StorageError::TezosEnvironmentError { error }
    }
}

impl From<FromBytesError> for StorageError {
    fn from(error: FromBytesError) -> Self {
        StorageError::HashError { error }
    }
}

impl From<FromBase58CheckError> for StorageError {
    fn from(error: FromBase58CheckError) -> Self {
        StorageError::HashDecodeError { error }
    }
}

impl slog::Value for StorageError {
    fn serialize(
        &self,
        _record: &slog::Record,
        key: slog::Key,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        serializer.emit_arguments(key, &format_args!("{}", self))
    }
}

/// Struct represent init information about storage on startup
#[derive(Clone, Serialize, Deserialize)]
pub struct StorageInitInfo {
    pub chain_id: ChainId,
    pub genesis_block_header_hash: BlockHash,
    pub patch_context: Option<PatchContext>,

    // TODO: TE-447 - remove one_context when integration done
    pub one_context: bool,
}

/// Resolve main chain id and genesis header from configuration
pub fn resolve_storage_init_chain_data(
    tezos_env: &TezosEnvironmentConfiguration,
    storage_db_path: &Path,
    context_db_path: &Path,
    patch_context: &Option<PatchContext>,
    one_context: bool,
    log: &Logger,
) -> Result<StorageInitInfo, StorageError> {
    let init_data = StorageInitInfo {
        chain_id: tezos_env.main_chain_id()?,
        genesis_block_header_hash: tezos_env.genesis_header_hash()?,
        patch_context: patch_context.clone(),
        one_context,
    };

    info!(
        log,
        "Storage based on data";
        "chain_name" => &tezos_env.version,
        "init_data.chain_id" => format!("{:?}", init_data.chain_id.to_base58_check()),
        "init_data.genesis_header" => format!("{:?}", init_data.genesis_block_header_hash.to_base58_check()),
        "storage_db_path" => format!("{:?}", storage_db_path),
        "context_db_path" => format!("{:?}", context_db_path),
        "one_context" => one_context,
        "patch_context" => match patch_context {
                Some(pc) => format!("{:?}", pc),
                None => "-none-".to_string()
        },
    );
    Ok(init_data)
}

/// Stores apply result to storage and mark block as applied, if everythnig is ok.
pub fn store_applied_block_result(
    block_storage: &BlockStorage,
    block_meta_storage: &BlockMetaStorage,
    block_hash: &BlockHash,
    block_result: ApplyBlockResponse,
    block_metadata: &mut block_meta_storage::Meta,
) -> Result<(BlockJsonData, BlockAdditionalData), StorageError> {
    // store result data - json and additional data
    let block_json_data = BlockJsonDataBuilder::default()
        .block_header_proto_json(block_result.block_header_proto_json)
        .block_header_proto_metadata_json(block_result.block_header_proto_metadata_json)
        .operations_proto_metadata_json(block_result.operations_proto_metadata_json)
        .build()
        .unwrap();
    block_storage.put_block_json_data(&block_hash, block_json_data.clone())?;

    // store additional data
    let block_additional_data = BlockAdditionalDataBuilder::default()
        .max_operations_ttl(block_result.max_operations_ttl.try_into().unwrap())
        .last_allowed_fork_level(block_result.last_allowed_fork_level)
        .block_metadata_hash(block_result.block_metadata_hash)
        .ops_metadata_hash({
            // Note: Ocaml introduces this two attributes (block_metadata_hash, ops_metadata_hash) in 008 edo
            //       So, we need to add the same handling, because this attributes contributes to context_hash
            //       They, store it, only if [`validation_passes > 0`], this measn that we have some operations
            match &block_result.ops_metadata_hashes {
                Some(hashes) => {
                    if hashes.is_empty() {
                        None
                    } else {
                        block_result.ops_metadata_hash
                    }
                }
                None => None,
            }
        })
        .ops_metadata_hashes(block_result.ops_metadata_hashes)
        .build()
        .unwrap();
    block_storage.put_block_additional_data(&block_hash, block_additional_data.clone())?;

    // TODO: check context checksum or context_hash

    // if everything is stored and ok, we can considere this block as applied
    // mark current head as applied
    block_metadata.set_is_applied(true);
    block_meta_storage.put(&block_hash, &block_metadata)?;
    // populate predecessor storage
    block_meta_storage.store_predecessors(&block_hash, &block_metadata)?;

    Ok((block_json_data, block_additional_data))
}

/// Stores commit_genesis result to storage and mark genesis block as applied, if everythnig is ok.
/// !Important, this rewrites context_hash on stored genesis - because in initialize_storage_with_genesis_block we stored wiht Context_hash_zero
/// And context hash of block is used for appling of successor
pub fn store_commit_genesis_result(
    block_storage: &BlockStorage,
    block_meta_storage: &BlockMetaStorage,
    chain_meta_storage: &ChainMetaStorage,
    operations_meta_storage: &OperationsMetaStorage,
    init_storage_data: &StorageInitInfo,
    bock_result: CommitGenesisResult,
) -> Result<BlockJsonData, StorageError> {
    // store data for genesis
    let genesis_block_hash = &init_storage_data.genesis_block_header_hash;
    let chain_id = &init_storage_data.chain_id;

    // if everything is stored and ok, we can considere genesis block as applied
    // if storage is empty, initialize with genesis
    block_meta_storage.put(
        &genesis_block_hash,
        &block_meta_storage::Meta::genesis_meta(&genesis_block_hash, chain_id, true),
    )?;
    operations_meta_storage.put(
        &genesis_block_hash,
        &operations_meta_storage::Meta::genesis_meta(chain_id),
    )?;

    // store result data - json and additional data
    let block_json_data = BlockJsonDataBuilder::default()
        .block_header_proto_json(bock_result.block_header_proto_json)
        .block_header_proto_metadata_json(bock_result.block_header_proto_metadata_json)
        .operations_proto_metadata_json(bock_result.operations_proto_metadata_json)
        .build()
        .unwrap();
    block_storage.put_block_json_data(&genesis_block_hash, block_json_data.clone())?;

    // set genesis as current head - it is empty storage
    match block_storage.get(&genesis_block_hash)? {
        Some(genesis) => {
            let head = Head::new(
                genesis.hash,
                genesis.header.level(),
                genesis.header.fitness().clone(),
            );

            // init chain data
            chain_meta_storage.set_genesis(&chain_id, head.clone())?;
            chain_meta_storage.set_caboose(&chain_id, head.clone())?;
            chain_meta_storage.set_current_head(&chain_id, head)?;

            Ok(block_json_data)
        }
        None => Err(StorageError::MissingKey),
    }
}

/// Genesis block needs extra handling because predecessor of the genesis block is genesis itself.
/// Which means that successor of the genesis block is also genesis block. By combining those
/// two statements we get cyclic relationship and everything breaks..
///
/// Genesis header is also "applied", but with special case "commit_genesis", which initialize protocol context.
/// We must ensure this, because applying next blocks depends on Context (identified by ContextHash) of predecessor.
///
/// So when "commit_genesis" event occurrs, we must use Context_hash and create genesis header within.
/// Commit_genesis also updates block json/additional metadata resolved by protocol.
pub fn initialize_storage_with_genesis_block(
    block_storage: &BlockStorage,
    init_storage_data: &StorageInitInfo,
    tezos_env: &TezosEnvironmentConfiguration,
    context_hash: &ContextHash,
    log: &Logger,
) -> Result<BlockHeaderWithHash, StorageError> {
    // TODO: check context checksum or context_hash

    // store genesis
    let genesis_with_hash = BlockHeaderWithHash {
        hash: init_storage_data.genesis_block_header_hash.clone(),
        header: Arc::new(
            tezos_env
                .genesis_header(context_hash.clone(), get_empty_operation_list_list_hash()?)?,
        ),
    };
    let _ = block_storage.put_block_header(&genesis_with_hash)?;

    // store additional data
    let genesis_additional_data = tezos_env.genesis_additional_data();
    let block_additional_data = BlockAdditionalDataBuilder::default()
        .max_operations_ttl(genesis_additional_data.max_operations_ttl)
        .last_allowed_fork_level(genesis_additional_data.last_allowed_fork_level)
        .block_metadata_hash(None)
        .ops_metadata_hash(None)
        .ops_metadata_hashes(None)
        .build()
        .unwrap();
    block_storage.put_block_additional_data(&genesis_with_hash.hash, block_additional_data)?;

    // context assign
    block_storage.assign_to_context(&genesis_with_hash.hash, &context_hash)?;

    info!(log,
        "Storage initialized with genesis block";
        "genesis" => genesis_with_hash.hash.to_base58_check(),
        "context_hash" => context_hash.to_base58_check(),
    );
    Ok(genesis_with_hash)
}

/// Helper module to easily initialize databases
pub mod initializer {
    use std::path::{PathBuf, Path};
    use std::sync::Arc;

    use rocksdb::{Cache, ColumnFamilyDescriptor, DB};
    use slog::{error, Logger};

    use crypto::hash::ChainId;

    use crate::context::merkle::merkle_storage::MerkleStorage;
    use crate::persistent::database::{open_kv, RocksDbKeyValueSchema};
    use crate::persistent::{DBError, DbConfiguration, open_sled_db};
    use crate::{StorageError, SystemStorage};
    use crate::database::db::MainDB;
    use crate::database::error::Error as DatabaseError;


    // IMPORTANT: Cache object must live at least as long as DB (returned by open_kv)
    pub type GlobalRocksDbCacheHolder = Vec<RocksDbCache>;
    pub type RocksDbCache = Cache;

    /// Factory for creation of grouped column family descriptors
    pub trait RocksDbColumnFactory {
        fn create(&self, cache: &RocksDbCache) -> Vec<ColumnFamilyDescriptor>;
    }

    /// Tables initializer for all operational datbases
    #[derive(Debug, Clone)]
    pub struct DbsRocksDbTableInitializer;

    /// Tables initializer for Context Rocksdb k-v store (if configured)
    #[derive(Debug, Clone)]
    pub struct ContextRocksDbTableInitializer;

    /// Tables initializer for context action rocksdb k-v store (if configured)
    #[derive(Debug, Clone)]
    pub struct ContextActionsRocksDbTableInitializer;

    impl RocksDbColumnFactory for ContextRocksDbTableInitializer {
        fn create(&self, cache: &RocksDbCache) -> Vec<ColumnFamilyDescriptor> {
            vec![
                crate::context::kv_store::rocksdb_backend::RocksDBBackend::descriptor(cache),
            ]
        }
    }

    impl RocksDbColumnFactory for DbsRocksDbTableInitializer {
        fn create(&self, cache: &RocksDbCache) -> Vec<ColumnFamilyDescriptor> {
            vec![
            ]
        }
    }

    impl RocksDbColumnFactory for ContextActionsRocksDbTableInitializer {
        fn create(&self, cache: &RocksDbCache) -> Vec<ColumnFamilyDescriptor> {
            vec![
                crate::context::actions::context_action_storage::ContextActionByBlockHashIndex::descriptor(cache),
                crate::context::actions::context_action_storage::ContextActionByContractIndex::descriptor(cache),
                crate::context::actions::context_action_storage::ContextActionByTypeIndex::descriptor(cache),
                crate::context::actions::context_action_storage::ContextActionStorage::descriptor(cache),
            ]
        }
    }

    #[derive(Debug, Clone)]
    pub struct RocksDbConfig<C: RocksDbColumnFactory> {
        pub cache_size: usize,
        pub expected_db_version: i64,
        pub db_path: PathBuf,
        pub system_storage_path : PathBuf,
        pub columns: C,
        pub threads: Option<usize>,
    }

    #[derive(Debug, Clone)]
    pub enum ContextKvStoreConfiguration {
        RocksDb(RocksDbConfig<ContextRocksDbTableInitializer>),
        Sled { path: PathBuf },
        InMem,
        BTreeMap,
    }

    pub fn initialize_rocksdb<Factory: RocksDbColumnFactory>(
        log: &Logger,
        cache: &Cache,
        config: &RocksDbConfig<Factory>,
        expected_main_chain: &MainChain,
    ) -> Result<Arc<DB>, DBError> {
        let db = open_kv(
            &config.db_path,
            config.columns.create(cache),
            &DbConfiguration {
                max_threads: config.threads,
            },
        )
            .map(Arc::new)?;
        Ok(db)
    }

    pub fn initialize_maindb<P : AsRef<Path>>(
        log: &Logger,
        path : P,
        db_version: i64,
        expected_main_chain: &MainChain,
    ) -> Result<Arc<MainDB>, DatabaseError> {
        let db = Arc::new(open_sled_db(
            path.as_ref()
        )?);

        match check_database_compatibility(
            db.clone(),
            db_version,
            expected_main_chain,
            &log,
        ) {
            Ok(false) => Err(DatabaseError::DatabaseIncompatibility {
                name: format!(
                    "Database is incompatible with version {}",
                    db_version
                ),
            }),
            Err(e) => Err(DatabaseError::DatabaseIncompatibility {
                name: format!("Failed to verify database compatibility reason: '{}'", e),
            }),
            _ => Ok(db),
        }
    }

    pub struct MainChain {
        chain_id: ChainId,
        chain_name: String,
    }

    impl MainChain {
        pub fn new(chain_id: ChainId, chain_name: String) -> Self {
            Self {
                chain_id,
                chain_name,
            }
        }
    }

    fn check_database_compatibility(
        db: Arc<MainDB>,
        expected_database_version: i64,
        expected_main_chain: &MainChain,
        log: &Logger,
    ) -> Result<bool, StorageError> {
        let mut system_info = SystemStorage::new(db);
        let db_version_ok = match system_info.get_db_version()? {
            Some(db_version) => db_version == expected_database_version,
            None => {
                system_info.set_db_version(expected_database_version)?;
                true
            }
        };
        if !db_version_ok {
            error!(log, "Incompatible database version found. Please re-sync your node to empty storage - see configuration!");
        }

        let tezos_env_main_chain_id = &expected_main_chain.chain_id;
        let tezos_env_main_chain_name = &expected_main_chain.chain_name;

        let (chain_id_ok, previous_chain_name, requested_chain_name) =
            match system_info.get_chain_id()? {
                Some(chain_id) => {
                    let previous_chain_name = match system_info.get_chain_name()? {
                        Some(chn) => chn,
                        None => "-unknown-".to_string(),
                    };

                    if chain_id == *tezos_env_main_chain_id
                        && previous_chain_name.eq(tezos_env_main_chain_name.as_str())
                    {
                        (true, previous_chain_name, tezos_env_main_chain_name)
                    } else {
                        (false, previous_chain_name, tezos_env_main_chain_name)
                    }
                }
                None => {
                    system_info.set_chain_id(&tezos_env_main_chain_id)?;
                    system_info.set_chain_name(&tezos_env_main_chain_name)?;
                    (true, "-none-".to_string(), tezos_env_main_chain_name)
                }
            };

        if !chain_id_ok {
            error!(log, "Current database was previously created for another chain. Please re-sync your node to empty storage - see configuration!";
                        "requested_chain" => requested_chain_name,
                        "previous_chain" => previous_chain_name
            );
        }

        Ok(db_version_ok && chain_id_ok)
    }


    pub fn initialize_merkle(
        context_kv_store: &ContextKvStoreConfiguration,
        expected_main_chain: &MainChain,
        log: &Logger,
        caches: &mut GlobalRocksDbCacheHolder,
    ) -> Result<MerkleStorage, failure::Error> {
        Ok(MerkleStorage::new(match context_kv_store {
            ContextKvStoreConfiguration::RocksDb(cfg) => {
                let kv_context_cache = Cache::new_lru_cache(cfg.cache_size)
                    .expect("Failed to initialize RocksDB cache (db_context)");
                let kv_context =
                    initialize_rocksdb(&log, &kv_context_cache, cfg, expected_main_chain)
                        .expect("Failed to create/initialize RocksDB database (db_context)");
                caches.push(kv_context_cache);
                Box::new(crate::context::kv_store::rocksdb_backend::RocksDBBackend::new(kv_context))
            }
            ContextKvStoreConfiguration::Sled { path } => {
                let sled = sled::Config::new()
                    .path(path)
                    .open()
                    .expect("Failed to create/initialize Sled database (db_context)");
                Box::new(crate::context::kv_store::sled_backend::SledBackend::new(
                    sled,
                ))
            }
            ContextKvStoreConfiguration::InMem => {
                Box::new(crate::context::kv_store::in_memory_backend::InMemoryBackend::new())
            }
            ContextKvStoreConfiguration::BTreeMap => {
                Box::new(crate::context::kv_store::btree_map::BTreeMapBackend::new())
            }
        }))
    }
}

#[derive(Clone)]
pub struct PersistentStorage {
    /// key-value store for main db
    main_db: Arc<MainDB>,
    /// commit log store for storing plain block header data
    clog: Arc<CommitLogs>,
    /// autoincrement  id generators
    seq: Arc<Sequences>,
    /// merkle-tree based context storage
    merkle: Arc<RwLock<MerkleStorage>>,
    /// persistent context actions storage
    merkle_context_actions: Option<Arc<DB>>,
}

impl PersistentStorage {
    pub fn new(
        main_db: Arc<MainDB>,
        clog: Arc<CommitLogs>,
        seq: Arc<Sequences>,
        merkle: Arc<RwLock<MerkleStorage>>,
        merkle_context_actions: Option<Arc<DB>>,
    ) -> Self {
        Self {
            clog,
            seq,
            merkle,
            merkle_context_actions,
            main_db,
        }
    }

    #[inline]
    pub fn main_db(&self) -> Arc<MainDB> {
        self.main_db.clone()
    }

    #[inline]
    pub fn clog(&self) -> Arc<CommitLogs> {
        self.clog.clone()
    }

    #[inline]
    pub fn seq(&self) -> Arc<Sequences> {
        self.seq.clone()
    }

    #[inline]
    pub fn merkle(&self) -> Arc<RwLock<MerkleStorage>> {
        self.merkle.clone()
    }

    #[inline]
    pub fn merkle_context_actions(&self) -> Option<Arc<DB>> {
        self.merkle_context_actions.clone()
    }

    pub fn flush_dbs(&mut self) {
        let clog = self.clog.flush();
        let db = self.main_db.flush();
        let merkle = match self.merkle.write() {
            Ok(merkle) => merkle.flush(),
            Err(e) => Err(failure::format_err!(
                "Failed to write/lock for flush, reason: {:?}",
                e
            )),
        };
        let merkle_context_actions = match self.merkle_context_actions.as_ref() {
            Some(merkle_context_actions) => merkle_context_actions.flush(),
            None => Ok(()),
        };

        if clog.is_err() || db.is_err() || merkle.is_err() || merkle_context_actions.is_err() {
            println!(
                "Failed to flush DBs. clog_err: {:?}, kv_err: {:?}, merkle_err: {:?}, merkle_context_actions_err: {:?}",
                clog, db, merkle, merkle_context_actions
            );
        }
    }
}

impl Drop for PersistentStorage {
    fn drop(&mut self) {
        self.flush_dbs();
    }
}

pub mod tests_common {
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::{env, fs};

    use failure::Error;

    
    
    use crate::context::actions::context_action_storage;
    use crate::context::kv_store::rocksdb_backend::RocksDBBackend;
    use crate::context::merkle::merkle_storage::MerkleStorage;
    
    use crate::persistent::database::{open_kv, RocksDbKeyValueSchema};
    use crate::persistent::sequence::Sequences;
    use crate::persistent::{open_cl, CommitLogSchema, DbConfiguration};

    use super::*;
    use crate::database::db::MainDB;
    use crate::database::DBSubtreeKeyValueSchema;

    pub struct TmpStorage {
        persistent_storage: PersistentStorage,
        path: PathBuf,
        remove_on_destroy: bool,
    }

    impl TmpStorage {
        pub fn create_to_out_dir(dir_name: &str) -> Result<Self, Error> {
            let out_dir = env::var("OUT_DIR").expect("OUT_DIR is not defined - check build.rs");
            let path = Path::new(out_dir.as_str()).join(Path::new(dir_name));
            Self::create(path)
        }

        pub fn create<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
            Self::initialize(path, true, true)
        }

        pub fn initialize<P: AsRef<Path>>(
            path: P,
            remove_if_exists: bool,
            remove_on_destroy: bool,
        ) -> Result<Self, Error> {
            let path = path.as_ref().to_path_buf();
            // remove previous data if exists
            if Path::new(&path).exists() && remove_if_exists {
                fs::remove_dir_all(&path).unwrap();
            }

            let cfg = DbConfiguration::default();

            // create common RocksDB block cache to be shared among column families
            let db_cache = Cache::new_lru_cache(128 * 1024 * 1024)?; // 128 MB

            //Sled DB storage
            let maindb = Arc::new(MainDB::initialize(path.join("database"), vec![
                OperationsStorage::sub_tree_name().to_string(),
                OperationsMetaStorage::sub_tree_name().to_string(),
            ], true)?);

            // context
            let db_context_cache = Cache::new_lru_cache(64 * 1024 * 1024)?; // 64 MB
            let context_kv_store = RocksDBBackend::new(Arc::new(open_kv(
                path.join("context"),
                vec![RocksDBBackend::descriptor(&db_context_cache)],
                &cfg,
            )?));
            let merkle = MerkleStorage::new(Box::new(context_kv_store));

            // context actions storage
            let db_context_actions_cache = Cache::new_lru_cache(16 * 1024 * 1024)?; // 16 MB
            let kv_context_action = open_kv(
                path.join("context_actions"),
                vec![
                    context_action_storage::ContextActionStorage::descriptor(
                        &db_context_actions_cache,
                    ),
                    context_action_storage::ContextActionByBlockHashIndex::descriptor(
                        &db_context_actions_cache,
                    ),
                    context_action_storage::ContextActionByContractIndex::descriptor(
                        &db_context_actions_cache,
                    ),
                    context_action_storage::ContextActionByTypeIndex::descriptor(
                        &db_context_actions_cache,
                    ),
                ],
                &cfg,
            )?;

            // commit log storage
            let clog = open_cl(&path, vec![BlockStorage::descriptor()])?;

            Ok(Self {
                persistent_storage: PersistentStorage::new(
                    maindb.clone(),
                    Arc::new(clog),
                    Arc::new(Sequences::new(maindb, 1000)),
                    Arc::new(RwLock::new(merkle)),
                    Some(Arc::new(kv_context_action)),
                ),
                path,
                remove_on_destroy,
            })
        }

        pub fn storage(&self) -> &PersistentStorage {
            &self.persistent_storage
        }

        pub fn path(&self) -> &PathBuf {
            &self.path
        }
    }

    impl Drop for TmpStorage {
        fn drop(&mut self) {
            let _ = rocksdb::DB::destroy(&rocksdb::Options::default(), &self.path);
            if self.remove_on_destroy {
                let _ = fs::remove_dir_all(&self.path);
            }
        }
    }
}
