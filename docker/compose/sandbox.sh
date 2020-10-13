#!/bin/bash

# add rust to path
export HOME=/home/appuser
export PATH=/home/appuser/.cargo/bin:$PATH

# light node config
# NETWORK="sandbox"
# TEZOS_DIR="/tmp/tezedge/tezos-data"
# BOOTSTRAP_DIR="/tmp/tezedge/tezedge-data/"
# CONFIG_FILE="./light_node/etc/tezedge/tezedge.config"
# IDENTITY_FILE="/tmp/tezedge/identity.json"


# cleanup data directory
# rm -rf $BOOTSTRAP_DIR && mkdir $BOOTSTRAP_DIR 
# rm -rf $TEZOS_DIR && mkdir $TEZOS_DIR

# protocol_runner needs 'libtezos.so' to run
export LD_LIBRARY_PATH="/home/appuser/tezedge/tezos/interop/lib_tezos/artifacts:/home/appuser/tezedge/target/release"
export TEZOS_CLIENT_UNSAFE_DISABLE_DISCLAIMER="Y"

rm -rf ./light_node/etc/tezedge_sandbox/tezos-client/*

./target/release/sandbox -- \
                            --log-level "info" \
                            --sandbox-rpc-port "3030" \
                            --light-node-path "./target/release/light-node" \
                            --tezos-client-path "./sandbox/artifacts/tezos-client"

# start node
# cargo run --release --bin light-node -- \
#                             --config-file "$CONFIG_FILE" \
#                             --tezos-data-dir "$TEZOS_DIR" \
#                             --bootstrap-db-path "$BOOTSTRAP_DIR" \
#                             --identity-file "$IDENTITY_FILE" \
#                             --network "$NETWORK" \
#                             --ocaml-log-enabled "false" \
#                             --protocol-runner "./target/release/protocol-runner"
