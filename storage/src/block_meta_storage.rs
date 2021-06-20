// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::convert::TryFrom;
use std::{convert::TryInto, sync::Arc};

use getset::{CopyGetters, Getters, Setters};
use rocksdb::{Cache, ColumnFamilyDescriptor, MergeOperands};
use serde::{Deserialize, Serialize};
use slog::{warn, Logger};

use crypto::hash::{
    BlockHash, BlockMetadataHash, ChainId, HashType, OperationMetadataHash,
    OperationMetadataListListHash, ProtocolHash,
};
use tezos_messages::p2p::encoding::block_header::Level;

use crate::persistent::database::{
    default_table_options, IteratorMode, IteratorWithSchema, RocksDbKeyValueSchema,
};
use crate::persistent::{
    BincodeEncoded, Decoder, Encoder, KeyValueSchema, KeyValueStoreWithSchema, SchemaError,
};
use crate::predecessor_storage::{PredecessorKey, PredecessorStorage};
use crate::{num_from_slice, PersistentStorage};
use crate::{BlockHeaderWithHash, StorageError};

pub type BlockMetaStorageKV = dyn KeyValueStoreWithSchema<BlockMetaStorage> + Sync + Send;
pub type BlockAdditionalDataStorageKV =
    dyn KeyValueStoreWithSchema<BlockAdditionalData> + Sync + Send;

pub trait BlockMetaStorageReader: Sync + Send {
    fn get(&self, block_hash: &BlockHash) -> Result<Option<Meta>, StorageError>;

    fn contains(&self, block_hash: &BlockHash) -> Result<bool, StorageError>;

    fn is_applied(&self, block_hash: &BlockHash) -> Result<bool, StorageError>;

    /// Returns n-th predecessor for block_hash
    fn find_block_at_distance(
        &self,
        block_hash: BlockHash,
        distance: i32,
    ) -> Result<Option<BlockHash>, StorageError>;

    /// Return ancestors of requested [block_hash] according to max_ttl (something like limit) sorted by [BlockHash] bytes
    fn get_live_blocks(
        &self,
        block_hash: BlockHash,
        max_ttl: usize,
    ) -> Result<Vec<BlockHash>, StorageError>;

    fn get_additional_data(
        &self,
        block_hash: &BlockHash,
    ) -> Result<Option<BlockAdditionalData>, StorageError>;
}

#[derive(Clone)]
pub struct BlockMetaStorage {
    kv: Arc<BlockMetaStorageKV>,
    predecessors_index: PredecessorStorage,
    additional_data_index: Arc<BlockAdditionalDataStorageKV>,
}

impl BlockMetaStorage {
    const STORED_PREDECESSORS_SIZE: u32 = 12;

    pub fn new(persistent_storage: &PersistentStorage) -> Self {
        BlockMetaStorage {
            kv: persistent_storage.db(),
            predecessors_index: PredecessorStorage::new(persistent_storage),
            additional_data_index: persistent_storage.db(),
        }
    }

    /// Creates/updates metadata record in storage from given block header
    /// Returns block metadata
    pub fn put_block_header(
        &self,
        block_header: &BlockHeaderWithHash,
        chain_id: &ChainId,
        log: &Logger,
    ) -> Result<Meta, StorageError> {
        // create/update record for block
        let block_metadata = match self.get(&block_header.hash)? {
            Some(mut meta) => {
                let block_predecessor = block_header.header.predecessor().clone();

                // log if predecessor should be changed - cannot happen
                match &meta.predecessor {
                    None => (),
                    Some(stored_predecessor) => {
                        if *stored_predecessor != block_predecessor {
                            warn!(
                                log, "Detected rewriting predecessor - not allowed (change is just ignored)";
                                "block_hash" => block_header.hash.to_base58_check(),
                                "stored_predecessor" => stored_predecessor.to_base58_check(),
                                "new_predecessor" => block_predecessor.to_base58_check()
                            );
                        }
                    }
                };

                // predecessor cannot be rewriten (just from None to Some) - see see merge_meta_value
                if meta.predecessor.is_none() {
                    meta.predecessor = Some(block_predecessor);
                    self.put(&block_header.hash, &meta)?;
                }

                meta
            }
            None => {
                let meta = Meta {
                    is_applied: false,
                    predecessor: Some(block_header.header.predecessor().clone()),
                    successors: vec![],
                    level: block_header.header.level(),
                    chain_id: chain_id.clone(),
                };
                self.put(&block_header.hash, &meta)?;
                meta
            }
        };

        // create/update record for block predecessor
        match self.get(&block_header.header.predecessor())?.as_mut() {
            Some(meta) => {
                let block_hash = &block_header.hash;

                // log if successor was changed on the predecessor - can happen, means reorg
                let need_change = match meta.successors.is_empty() {
                    true => true,
                    false => {
                        // here we have some previous successors
                        // if does not contains block_hash, means that we detected reorg or new branch
                        if !meta.successors.contains(&block_hash) {
                            warn!(
                                log, "Extending successors - means detected reorg or new branch";
                                "block_hash_predecessor" => block_header.header.predecessor().to_base58_check(),
                                "stored_successors" => {
                                    meta.successors
                                        .iter()
                                        .map(|bh| bh.to_base58_check())
                                        .collect::<Vec<String>>()
                                        .join(", ")
                                },
                                "new_successor" => block_hash.to_base58_check()
                            );
                            true
                        } else {
                            false
                        }
                    }
                };

                if need_change {
                    meta.successors.push(block_hash.clone());
                    self.put(block_header.header.predecessor(), &meta)?;
                }
            }
            None => {
                let meta = Meta {
                    is_applied: false,
                    predecessor: None,
                    successors: vec![block_header.hash.clone()],
                    level: block_header.header.level() - 1,
                    chain_id: chain_id.clone(),
                };
                self.put(block_header.header.predecessor(), &meta)?;
            }
        }

        Ok(block_metadata)
    }

    pub fn store_predecessors(
        &self,
        block_hash: &BlockHash,
        block_meta: &Meta,
    ) -> Result<(), StorageError> {
        self.predecessors_index.store_predecessors(
            block_hash,
            block_meta,
            Self::STORED_PREDECESSORS_SIZE,
        )?;
        Ok(())
    }

    pub fn put_block_additional_data(
        &self,
        block_hash: &BlockHash,
        additional_data: &BlockAdditionalData,
    ) -> Result<(), StorageError> {
        self.additional_data_index
            .put(block_hash, additional_data)
            .map_err(StorageError::from)
    }

    #[inline]
    pub fn put(&self, block_hash: &BlockHash, meta: &Meta) -> Result<(), StorageError> {
        self.kv.merge(block_hash, meta).map_err(StorageError::from)
    }

    #[inline]
    pub fn get(&self, block_hash: &BlockHash) -> Result<Option<Meta>, StorageError> {
        self.kv.get(block_hash).map_err(StorageError::from)
    }

    #[inline]
    pub fn iter(&self, mode: IteratorMode<Self>) -> Result<IteratorWithSchema<Self>, StorageError> {
        self.kv.iterator(mode).map_err(StorageError::from)
    }
}

impl BlockMetaStorageReader for BlockMetaStorage {
    fn get(&self, block_hash: &BlockHash) -> Result<Option<Meta>, StorageError> {
        self.kv.get(block_hash).map_err(StorageError::from)
    }

    // TODO: change this do contains, when fixed
    // TODO: at first, fix put_block_header, just to store actual block metadata and not empty predecessor's data
    fn contains(&self, block_hash: &BlockHash) -> Result<bool, StorageError> {
        self.kv.contains(block_hash).map_err(StorageError::from)
    }

    fn is_applied(&self, block_hash: &BlockHash) -> Result<bool, StorageError> {
        match self.get(block_hash) {
            Ok(Some(meta)) => Ok(meta.is_applied),
            Ok(None) => Ok(false),
            Err(e) => Err(e),
        }
    }

    // NOTE: implemented in a way to mirro the ocaml code, should be refactored to me more rusty
    fn find_block_at_distance(
        &self,
        block_hash: BlockHash,
        requested_distance: i32,
    ) -> Result<Option<BlockHash>, StorageError> {
        if requested_distance == 0 {
            return Ok(Some(block_hash));
        }
        if requested_distance < 0 {
            unimplemented!("TODO: TE-238 - Not yet implemented block header parsing for '+' - means we need to go forwards throught successors, this could be tricky and should be related to current head branch - reorg");
        }

        let mut distance = requested_distance;
        let mut block_hash = block_hash;
        const BASE: i32 = 2;
        loop {
            if distance == 1 {
                // distance is 1, return the direct prdecessor
                let key = PredecessorKey::new(block_hash, 0);
                return self.predecessors_index.get(&key);
            } else {
                let (mut power, mut rest) = closest_power_two_and_rest(distance)?;

                if power >= Self::STORED_PREDECESSORS_SIZE {
                    power = Self::STORED_PREDECESSORS_SIZE - 1;
                    rest = distance - BASE.pow(power);
                }

                let key = PredecessorKey::new(block_hash.clone(), power);
                if let Some(pred) = self.predecessors_index.get(&key)? {
                    if rest == 0 {
                        return Ok(Some(pred));
                    } else {
                        block_hash = pred;
                        distance = rest;
                    }
                } else {
                    return Ok(None); // reached genesis
                }
            }
        }
    }

    fn get_live_blocks(
        &self,
        block_hash: BlockHash,
        max_ttl: usize,
    ) -> Result<Vec<BlockHash>, StorageError> {
        // Note: tezos ocaml for max_ttl=60 returns 61 blocks
        let mut live_blocks_counter = max_ttl + 1;
        let mut live_blocks = Vec::with_capacity(live_blocks_counter);

        if self.get(&block_hash)?.is_some() {
            // add requested header (if found)
            live_blocks.push(block_hash.clone());
            live_blocks_counter -= 1;

            // lets find ancestors of requested header - (predecessors at distance 1)
            let mut current = block_hash;
            for _ in 0..live_blocks_counter {
                match self.find_block_at_distance(current, 1)? {
                    Some(predecessor) => {
                        live_blocks.push(predecessor.clone());
                        current = predecessor
                    }
                    None => {
                        // Note: if not predecessor, means, that we are done, genesis does not have predecessor
                        break;
                    }
                }
            }
        }

        // sort by bytes
        live_blocks.sort_by(|left, right| Ord::cmp(left, right));

        Ok(live_blocks)
    }

    fn get_additional_data(
        &self,
        block_hash: &BlockHash,
    ) -> Result<Option<BlockAdditionalData>, StorageError> {
        self.additional_data_index
            .get(block_hash)
            .map_err(StorageError::from)
    }
}

/// Function to find the closest power of 2 value to the distance. Returns the closest power
/// and the rest (distance = 2^closest_power + rest)
fn closest_power_two_and_rest(distance: i32) -> Result<(u32, i32), StorageError> {
    if distance < 0 {
        return Err(StorageError::PredecessorLookupError);
    }

    let base: i32 = 2;

    let mut closest_power: u32 = 0;
    let mut rest: i32 = 0;
    let mut distance: i32 = distance;

    while distance > 1 {
        rest += base.pow(closest_power) * (distance % 2);
        distance /= 2;
        closest_power += 1;
    }
    Ok((closest_power, rest))
}

const LEN_BLOCK_HASH: usize = HashType::BlockHash.size();
const LEN_CHAIN_ID: usize = HashType::ChainId.size();

const MASK_IS_APPLIED: u8 = 0b0000_0001;
const MASK_HAS_SUCCESSOR: u8 = 0b0000_0010;
const MASK_HAS_PREDECESSOR: u8 = 0b0000_0100;

const IDX_MASK: usize = 0;
const IDX_PREDECESSOR: usize = IDX_MASK + 1;
const IDX_LEVEL: usize = IDX_PREDECESSOR + LEN_BLOCK_HASH;
const IDX_CHAIN_ID: usize = IDX_LEVEL + std::mem::size_of::<i32>();
const IDX_SUCCESSOR_COUNT: usize = IDX_CHAIN_ID + LEN_CHAIN_ID;
const IDX_SUCCESSOR: usize = IDX_SUCCESSOR_COUNT + std::mem::size_of::<usize>();

const BLANK_BLOCK_HASH: [u8; LEN_BLOCK_HASH] = [0; LEN_BLOCK_HASH];
const LEN_FIXED_META: usize = std::mem::size_of::<u8>()
    + LEN_BLOCK_HASH
    + std::mem::size_of::<i32>()
    + LEN_CHAIN_ID
    + std::mem::size_of::<usize>();

const fn total_len(predecessors_count: usize) -> usize {
    LEN_FIXED_META + (predecessors_count * LEN_BLOCK_HASH)
}

macro_rules! is_applied {
    ($mask:expr) => {{
        ($mask & MASK_IS_APPLIED) != 0
    }};
}
macro_rules! has_predecessor {
    ($mask:expr) => {{
        ($mask & MASK_HAS_PREDECESSOR) != 0
    }};
}
macro_rules! has_successor {
    ($mask:expr) => {{
        ($mask & MASK_HAS_SUCCESSOR) != 0
    }};
}
macro_rules! successors_count {
    ($bytes:expr) => {{
        num_from_slice!($bytes, IDX_SUCCESSOR_COUNT, usize)
    }};
}

/// Meta information for the block
#[derive(Clone, Getters, CopyGetters, Setters, PartialEq, Debug)]
pub struct Meta {
    #[get = "pub"]
    predecessor: Option<BlockHash>,
    #[get = "pub"]
    successors: Vec<BlockHash>,
    #[get_copy = "pub"]
    #[set = "pub"]
    is_applied: bool,
    #[get_copy = "pub"]
    level: Level,
    #[get = "pub"]
    chain_id: ChainId,
}

impl Meta {
    pub const GENESIS_LEVEL: i32 = 0;

    /// Create Metadata for specific genesis block
    pub fn genesis_meta(
        genesis_hash: &BlockHash,
        genesis_chain_id: &ChainId,
        is_applied: bool,
    ) -> Self {
        Meta {
            is_applied,
            predecessor: Some(genesis_hash.clone()), // this is what we want
            successors: vec![], // we do not know (yet) successor of the genesis
            level: Self::GENESIS_LEVEL,
            chain_id: genesis_chain_id.clone(),
        }
    }

    pub fn new(
        is_applied: bool,
        predecessor: Option<BlockHash>,
        level: Level,
        chain_id: ChainId,
    ) -> Self {
        Self {
            is_applied,
            predecessor,
            successors: vec![],
            level,
            chain_id,
        }
    }

    pub fn is_downloaded(&self) -> bool {
        // if we updated predecessor, we can get this information just from block_header, so it measn it should be downloaded
        self.predecessor.is_some()
    }

    pub fn take_successors(self) -> Vec<BlockHash> {
        self.successors
    }

    pub fn take_predecessor(self) -> Option<BlockHash> {
        self.predecessor
    }
}

/// Codec for `Meta`
///
/// * bytes layout: `[mask(1)][predecessor(32)][level(4)][chain_id(4)][successors_count(8)][successors(successors_count*32)]`
impl Decoder for Meta {
    fn decode(bytes: &[u8]) -> Result<Self, SchemaError> {
        if LEN_FIXED_META <= bytes.len() {
            // mask
            let mask = bytes[IDX_MASK];
            let is_processed = is_applied!(mask);
            // predecessor
            let predecessor = if has_predecessor!(mask) {
                let block_hash = bytes[IDX_PREDECESSOR..IDX_LEVEL].to_vec();
                Some(block_hash.try_into()?)
            } else {
                None
            };
            // level
            let level = num_from_slice!(bytes, IDX_LEVEL, i32);
            // chain_id
            let chain_id = bytes[IDX_CHAIN_ID..IDX_SUCCESSOR_COUNT].to_vec();

            // successors
            let successors = {
                let count = successors_count!(bytes);
                let mut successors = Vec::with_capacity(count);
                if has_successor!(mask) {
                    for i in 0..count {
                        let next_successor_index = IDX_SUCCESSOR + (i * LEN_BLOCK_HASH);
                        let block_hash = bytes
                            [next_successor_index..(next_successor_index + LEN_BLOCK_HASH)]
                            .to_vec();
                        successors.push(BlockHash::try_from(block_hash)?);
                    }
                }
                successors
            };

            Ok(Meta {
                predecessor,
                successors,
                is_applied: is_processed,
                level,
                chain_id: chain_id.try_into()?,
            })
        } else {
            Err(SchemaError::DecodeError)
        }
    }
}

impl Encoder for Meta {
    fn encode(&self) -> Result<Vec<u8>, SchemaError> {
        let mut mask = 0u8;
        if self.is_applied {
            mask |= MASK_IS_APPLIED;
        }
        if self.predecessor.is_some() {
            mask |= MASK_HAS_PREDECESSOR;
        }
        if !self.successors.is_empty() {
            mask |= MASK_HAS_SUCCESSOR;
        }
        let successors_count = self.successors.len();
        let total_len = total_len(successors_count);

        let mut value = Vec::with_capacity(total_len);
        value.push(mask);
        match &self.predecessor {
            Some(predecessor) => value.extend(predecessor.as_ref()),
            None => value.extend(&BLANK_BLOCK_HASH),
        }
        value.extend(&self.level.to_be_bytes());
        value.extend(self.chain_id.as_ref());
        value.extend(&successors_count.to_be_bytes());
        if successors_count > 0 {
            self.successors.iter().for_each(|successor| {
                value.extend(successor.as_ref());
            });
        }
        debug_assert_eq!(
            total_len,
            value.len(),
            "Invalid size. mask={:?}, predecessor={:?}, successors={:?}, level={:?}, data={:?}",
            mask,
            &self.predecessor,
            &self.successors,
            self.level,
            &value
        );

        Ok(value)
    }
}

impl KeyValueSchema for BlockMetaStorage {
    type Key = BlockHash;
    type Value = Meta;
}

impl RocksDbKeyValueSchema for BlockMetaStorage {
    fn descriptor(cache: &Cache) -> ColumnFamilyDescriptor {
        let mut cf_opts = default_table_options(cache);
        cf_opts.set_merge_operator("block_meta_storage_merge_operator", merge_meta_value, None);
        ColumnFamilyDescriptor::new(Self::name(), cf_opts)
    }

    #[inline]
    fn name() -> &'static str {
        "block_meta_storage"
    }
}

fn merge_meta_value(
    _new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &mut MergeOperands,
) -> Option<Vec<u8>> {
    if let Some(val) = existing_val {
        if val.len() < LEN_FIXED_META {
            return None;
        }
    }

    let mut result = existing_val.map(|v| v.to_vec());

    for op in operands {
        match result {
            Some(ref mut val) => {
                if op.len() < LEN_FIXED_META {
                    return None;
                }

                let mask_val = val[IDX_MASK];
                let mask_op = op[IDX_MASK];

                // merge `mask(1)`
                val[IDX_MASK] = mask_val | mask_op;

                // if op has predecessor and val has not, copy it from op to val
                if has_predecessor!(mask_op) && !has_predecessor!(mask_val) {
                    val.splice(
                        IDX_PREDECESSOR..IDX_LEVEL,
                        op[IDX_PREDECESSOR..IDX_LEVEL].iter().cloned(),
                    );
                }

                // replace op (successors count + successors) to val
                let val_successors_count = successors_count!(val);
                let op_successors_count = successors_count!(op);
                if (has_successor!(mask_op) && !has_successor!(mask_val))
                    || (val_successors_count != op_successors_count)
                {
                    val.truncate(LEN_FIXED_META);
                    val.splice(
                        IDX_SUCCESSOR_COUNT..,
                        op[IDX_SUCCESSOR_COUNT..].iter().cloned(),
                    );
                }

                let total_len = total_len(op_successors_count);
                debug_assert_eq!(total_len, val.len(), "Invalid length after merge operator was applied. Was expecting {} but found {}.", total_len, val.len());
            }
            None => result = Some(op.to_vec()),
        }
    }

    result
}

/// Struct holds informations as a result from block apllication,
/// These data are used for appling next successor block
#[derive(Clone, CopyGetters, Getters, Serialize, Deserialize, Debug)]
pub struct BlockAdditionalData {
    #[get_copy = "pub"]
    max_operations_ttl: u16,
    #[get_copy = "pub"]
    last_allowed_fork_level: i32,
    #[get = "pub"]
    pub protocol_hash: ProtocolHash,
    #[get = "pub"]
    pub next_protocol_hash: ProtocolHash,
    #[get = "pub"]
    block_metadata_hash: Option<BlockMetadataHash>,
    // TODO: TE-238 - not needed, can be calculated from ops_metadata_hashes
    // TODO: TE-207 - not needed, can be calculated from ops_metadata_hashes
    #[get = "pub"]
    ops_metadata_hash: Option<OperationMetadataListListHash>,
    /// Note: This is calculated from ops_metadata_hashes - we need this in request
    ///       This is calculated as merkle tree hash, like operation paths
    #[get = "pub"]
    ops_metadata_hashes: Option<Vec<Vec<OperationMetadataHash>>>,
}

impl BlockAdditionalData {
    pub fn new(
        max_operations_ttl: u16,
        last_allowed_fork_level: i32,
        protocol_hash: ProtocolHash,
        next_protocol_hash: ProtocolHash,
        block_metadata_hash: Option<BlockMetadataHash>,
        ops_metadata_hash: Option<OperationMetadataListListHash>,
        ops_metadata_hashes: Option<Vec<Vec<OperationMetadataHash>>>,
    ) -> Self {
        Self {
            max_operations_ttl,
            last_allowed_fork_level,
            protocol_hash,
            next_protocol_hash,
            block_metadata_hash,
            ops_metadata_hash,
            ops_metadata_hashes,
        }
    }
}

impl
    Into<(
        Option<BlockMetadataHash>,
        Option<OperationMetadataListListHash>,
    )> for BlockAdditionalData
{
    fn into(
        self,
    ) -> (
        Option<BlockMetadataHash>,
        Option<OperationMetadataListListHash>,
    ) {
        (self.block_metadata_hash, self.ops_metadata_hash)
    }
}

impl
    Into<(
        Option<BlockMetadataHash>,
        Option<OperationMetadataListListHash>,
        u16,
    )> for BlockAdditionalData
{
    fn into(
        self,
    ) -> (
        Option<BlockMetadataHash>,
        Option<OperationMetadataListListHash>,
        u16,
    ) {
        (
            self.block_metadata_hash,
            self.ops_metadata_hash,
            self.max_operations_ttl,
        )
    }
}

impl KeyValueSchema for BlockAdditionalData {
    type Key = BlockHash;
    type Value = BlockAdditionalData;
}

impl BincodeEncoded for BlockAdditionalData {}

impl RocksDbKeyValueSchema for BlockAdditionalData {
    #[inline]
    fn name() -> &'static str {
        "block_additional_data"
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::convert::TryInto;
    use std::path::Path;

    use anyhow::Error;
    use rand::Rng;

    use crate::persistent::database::open_kv;
    use crate::persistent::DbConfiguration;
    use crate::tests_common::TmpStorage;

    use super::*;

    #[test]
    fn block_meta_encoded_equals_decoded() -> Result<(), Error> {
        let expected = Meta {
            is_applied: false,
            predecessor: Some(vec![98; 32].try_into().unwrap()),
            successors: vec![vec![21; 32].try_into().unwrap()],
            level: 34,
            chain_id: vec![44; 4].try_into().unwrap(),
        };
        let encoded_bytes = expected.encode()?;
        let decoded = Meta::decode(&encoded_bytes)?;
        assert_eq!(expected, decoded);
        Ok(())
    }

    #[test]
    fn genesis_block_initialized_success() -> Result<(), Error> {
        let tmp_storage = TmpStorage::create_to_out_dir("__blockmeta_genesistest")?;

        let k = "BLockGenesisGenesisGenesisGenesisGenesisb83baZgbyZe".try_into()?;
        let chain_id = "NetXgtSLGNJvNye".try_into()?;
        let v = Meta::genesis_meta(&k, &chain_id, true);
        let storage = BlockMetaStorage::new(tmp_storage.storage());
        storage.put(&k, &v)?;
        match storage.get(&k)? {
            Some(value) => {
                let expected = Meta {
                    is_applied: true,
                    predecessor: Some(k.clone()),
                    successors: vec![],
                    level: 0,
                    chain_id,
                };
                assert_eq!(expected, value);
            }
            _ => panic!("value not present"),
        }

        Ok(())
    }

    #[test]
    fn block_meta_storage_test() -> Result<(), Error> {
        let tmp_storage = TmpStorage::create_to_out_dir("__blockmeta_storagetest")?;

        let k = vec![44; 32].try_into().unwrap();
        let mut v = Meta {
            is_applied: false,
            predecessor: None,
            successors: vec![],
            level: 1_245_762,
            chain_id: vec![44; 4].try_into().unwrap(),
        };
        let storage = BlockMetaStorage::new(tmp_storage.storage());
        storage.put(&k, &v)?;
        assert!(storage.get(&k)?.is_some());

        // change applied to true and predecessor + add successor
        v.is_applied = true;
        v.predecessor = Some(vec![98; 32].try_into().unwrap());
        v.successors = vec![vec![21; 32].try_into().unwrap()];
        storage.put(&k, &v)?;

        // try change is_applied (cannot be overwritten - see merge_meta_value)
        v.is_applied = false;
        storage.put(&k, &v)?;

        // try change predecessor (cannot be overwritten - see merge_meta_value)
        v.predecessor = Some(vec![198; 32].try_into().unwrap());
        storage.put(&k, &v)?;

        // add successor
        v.successors = vec![
            vec![21; 32].try_into().unwrap(),
            vec![121; 32].try_into().unwrap(),
        ];
        storage.put(&k, &v)?;

        // check stored meta
        match storage.get(&k)? {
            Some(value) => {
                let expected = Meta {
                    is_applied: true,
                    predecessor: Some(vec![98; 32].try_into().unwrap()),
                    successors: vec![
                        vec![21; 32].try_into().unwrap(),
                        vec![121; 32].try_into().unwrap(),
                    ],
                    level: 1_245_762,
                    chain_id: vec![44; 4].try_into().unwrap(),
                };
                assert_eq!(expected, value);
            }
            _ => panic!("value not present"),
        }

        // try remove predecesor (cannot be overwritten - see merge_meta_value)
        v.predecessor = None;
        storage.put(&k, &v)?;

        // modify successors
        v.successors = vec![vec![121; 32].try_into().unwrap()];
        storage.put(&k, &v)?;

        // check stored meta
        match storage.get(&k)? {
            Some(value) => {
                let expected = Meta {
                    is_applied: true,
                    predecessor: Some(vec![98; 32].try_into().unwrap()),
                    successors: vec![vec![121; 32].try_into().unwrap()],
                    level: 1_245_762,
                    chain_id: vec![44; 4].try_into().unwrap(),
                };
                assert_eq!(expected, value);
            }
            _ => panic!("value not present"),
        }

        Ok(())
    }

    #[test]
    fn merge_meta_value_test() {
        use rocksdb::{Cache, Options, DB};

        let path = "__blockmeta_mergetest";
        if Path::new(path).exists() {
            std::fs::remove_dir_all(path).unwrap();
        }

        {
            let cache = Cache::new_lru_cache(32 * 1024 * 1024).unwrap();
            let db = open_kv(
                path,
                vec![BlockMetaStorage::descriptor(&cache)],
                &DbConfiguration::default(),
            )
            .unwrap();
            let k: BlockHash = vec![44; 32].try_into().unwrap();
            let mut v = Meta {
                is_applied: false,
                predecessor: None,
                successors: vec![],
                level: 2,
                chain_id: vec![44; 4].try_into().unwrap(),
            };
            let p = BlockMetaStorageKV::merge(&db, &k, &v);
            assert!(p.is_ok(), "p: {:?}", p.unwrap_err());
            v.is_applied = true;
            v.successors = vec![vec![21; 32].try_into().unwrap()];
            let _ = BlockMetaStorageKV::merge(&db, &k, &v);

            v.is_applied = false;
            v.predecessor = Some(vec![98; 32].try_into().unwrap());
            v.successors = vec![];
            let _ = BlockMetaStorageKV::merge(&db, &k, &v);

            v.predecessor = None;
            let m = BlockMetaStorageKV::merge(&db, &k, &v);
            assert!(m.is_ok());

            match BlockMetaStorageKV::get(&db, &k) {
                Ok(Some(value)) => {
                    let expected = Meta {
                        is_applied: true,
                        predecessor: Some(vec![98; 32].try_into().unwrap()),
                        successors: vec![],
                        level: 2,
                        chain_id: vec![44; 4].try_into().unwrap(),
                    };
                    assert_eq!(expected, value);
                }
                Err(_) => println!("error reading value"),
                _ => panic!("value not present"),
            }
        }
        assert!(DB::destroy(&Options::default(), path).is_ok());
    }

    /// Create and return a storage with [number_of_blocks] blocks and the last BlockHash in it
    fn init_mocked_storage(
        number_of_blocks: usize,
        storage_name: &str,
    ) -> Result<(BlockMetaStorage, BlockHash, Vec<BlockHash>), Error> {
        let tmp_storage = TmpStorage::create_to_out_dir(storage_name)?;
        let storage = BlockMetaStorage::new(tmp_storage.storage());
        let mut block_hash_set = HashSet::new();
        let mut rng = rand::thread_rng();

        let k: BlockHash = vec![0; 32].try_into().unwrap();
        let v = Meta::new(
            false,
            Some(vec![0; 32].try_into().unwrap()),
            0,
            vec![44; 4].try_into().unwrap(),
        );

        block_hash_set.insert(k.clone());

        storage.put(&k, &v)?;
        assert!(storage.get(&k)?.is_some());

        // save for the iteration
        let mut predecessor = k;

        // generate random block hashes, watch out for colissions, save them to a vector for later use
        // this vector is our reference point (It allows us to compare the retriebved block hash
        // from the storage with the block hash in vector which was insertd sequently thus the
        // we can  deduct its level from its index in the vector)
        let block_hashes: Vec<BlockHash> = (1..number_of_blocks)
            .map(|_| {
                let mut random_hash = (0..32)
                    .map(|_| rng.gen_range(0, 255))
                    .collect::<Vec<_>>()
                    .try_into()
                    .unwrap();
                // regenerate on collision
                while block_hash_set.contains(&random_hash) {
                    random_hash = (0..32)
                        .map(|_| rng.gen_range(0, 255))
                        .collect::<Vec<_>>()
                        .try_into()
                        .unwrap();
                }
                block_hash_set.insert(random_hash.clone());
                random_hash
            })
            .collect();

        // add them to the mocked storage
        for (idx, block_hash) in block_hashes.iter().enumerate() {
            let v = Meta::new(
                true,
                Some(predecessor.clone()),
                idx.try_into()?,
                vec![44; 4].try_into().unwrap(),
            );
            storage.put(&block_hash, &v)?;
            storage.store_predecessors(&block_hash, &v)?;
            predecessor = block_hash.clone();
        }

        Ok((storage, predecessor, block_hashes))
    }

    #[test]
    fn find_block_at_distance_test() -> Result<(), Error> {
        const BLOCK_COUNT: usize = 33_000;

        // block_hashes starts with level 1 (genesis not included)
        let (storage, last_block_hash, block_hashes) =
            init_mocked_storage(BLOCK_COUNT, "__find_block_at_distance_teststorage")?;

        // from the last block, go to distance BLOCK_COUNT - 2 -> should be level 1
        let res = storage.find_block_at_distance(
            block_hashes[BLOCK_COUNT - 2].clone(),
            BLOCK_COUNT as i32 - 2,
        )?;
        assert!(res.is_some());
        assert_eq!(block_hashes[0], res.unwrap());

        // from the last block, go to distance BLOCK_COUNT - BLOCK_COUNT / 2 -> should be hash on
        let res = storage.find_block_at_distance(
            block_hashes[BLOCK_COUNT - 2].clone(),
            BLOCK_COUNT as i32 / 2,
        )?;
        assert!(res.is_some());
        assert_eq!(block_hashes[BLOCK_COUNT / 2 - 2], res.unwrap());

        // from the bloc on level 10, go to distance BLOCK_COUNT - BLOCK_COUNT / 2 -> should be hash on
        let res = storage.find_block_at_distance(block_hashes[9].clone(), 5)?;
        assert!(res.is_some());
        assert_eq!(block_hashes[4], res.unwrap());

        // get the direct predecessor using find_block_at_distance
        let res = storage.find_block_at_distance(block_hashes[9].clone(), 1)?;
        assert!(res.is_some());
        assert_eq!(block_hashes[8], res.unwrap());
        for i in 1..BLOCK_COUNT - 2 {
            let res = storage.find_block_at_distance(last_block_hash.clone(), i as i32)?;
            assert_eq!(block_hashes[BLOCK_COUNT - i - 2], res.unwrap())
        }

        Ok(())
    }

    #[test]
    fn find_block_at_distance_heavy_test() -> Result<(), Error> {
        const BLOCK_COUNT: usize = 1_025;

        // block_hashes starts with level 1 (genesis not included)
        let (storage, _, block_hashes) =
            init_mocked_storage(BLOCK_COUNT, "__find_block_at_distance_heavy_teststorage")?;

        // So called 'ultimate' test, tests all blocks with all possible distances
        for (idx, hash) in block_hashes.iter().enumerate() {
            // block_hashes vector is offseted by 1 (index 0 refers to level 1)
            if (idx % 10) == 0 {
                println!("Checked {} blocks from {}", idx, BLOCK_COUNT);
            }
            for i in 1..idx {
                let res = storage.find_block_at_distance(hash.clone(), i as i32)?;
                assert_eq!(block_hashes[idx - i], res.unwrap())
            }
        }

        Ok(())
    }
}
