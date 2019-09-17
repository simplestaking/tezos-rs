use tezos_interop::ffi;
use tezos_interop::ffi::OcamlStorageInitInfo;

mod common;

pub const CHAIN_ID: &str = "8eceda2f";

#[test]
fn test_init_storage() {
    // init empty storage for test
    let OcamlStorageInitInfo {chain_id, genesis_block_header_hash, current_block_header_hash} = prepare_empty_storage("test_storage_01");
    assert_eq!(false, current_block_header_hash.is_empty());

    // has current head (genesis)
    let current_head = ffi::get_current_block_header(chain_id.to_string()).unwrap();
    assert_eq!(false, current_head.is_empty());

    // get header - genesis
    let block_header = ffi::get_block_header(genesis_block_header_hash).unwrap();

    // check header found
    assert!(block_header.is_some());
    assert_eq!(current_head, block_header.unwrap());
}

#[test]
fn test_fn_get_block_header_not_found_return_none() {
    // init empty storage for test
    let _ = prepare_empty_storage("test_storage_02");

    // get unknown header
    let block_header_hash = "3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a";
    let block_header = ffi::get_block_header(block_header_hash.to_string()).unwrap();

    // check not found
    assert!(block_header.is_none());
}

/// Initializes empty dir for ocaml storage
pub fn prepare_empty_storage(dir_name: &str) -> OcamlStorageInitInfo {
    // init empty storage for test
    let storage_data_dir_path = common::prepare_empty_dir(dir_name);
    let storage_init_info = ffi::init_storage(storage_data_dir_path.to_string()).unwrap();
    assert_eq!(CHAIN_ID, &storage_init_info.chain_id);
    storage_init_info
}