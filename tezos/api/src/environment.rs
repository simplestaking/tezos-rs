// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::collections::HashMap;
use std::str::FromStr;

use enum_iterator::IntoEnumIterator;
use serde::{Deserialize, Serialize};

use lazy_static::lazy_static;

use crate::ffi::{GenesisChain, ProtocolOverrides};

lazy_static! {
    pub static ref TEZOS_ENV: HashMap<TezosEnvironment, TezosEnvironmentConfiguration> = init();
}

/// Enum representing different Tezos environment.
#[derive(Serialize, Deserialize, Copy, Clone, Debug, PartialEq, Eq, Hash, IntoEnumIterator)]
pub enum TezosEnvironment {
    Alphanet,
    Babylonnet,
    Mainnet,
    Zeronet,
}

#[derive(Debug, Clone)]
pub struct ParseTezosEnvironmentError(String);

impl FromStr for TezosEnvironment {
    type Err = ParseTezosEnvironmentError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "alphanet" => Ok(TezosEnvironment::Alphanet),
            "babylonnet" | "babylon" => Ok(TezosEnvironment::Babylonnet),
            "mainnet" => Ok(TezosEnvironment::Mainnet),
            "zeronet" => Ok(TezosEnvironment::Zeronet),
            _ => Err(ParseTezosEnvironmentError(format!("Invalid variant name: {}", s)))
        }
    }
}

/// Initializes hard-code configuration according to different Tezos git branches (genesis_chain.ml, node_config_file.ml)
fn init() -> HashMap<TezosEnvironment, TezosEnvironmentConfiguration> {
    let mut env: HashMap<TezosEnvironment, TezosEnvironmentConfiguration> = HashMap::new();

    env.insert(TezosEnvironment::Alphanet, TezosEnvironmentConfiguration {
        genesis: GenesisChain {
            time: "2018-11-30T15:30:56Z".to_string(),
            block: "BLockGenesisGenesisGenesisGenesisGenesisb83baZgbyZe".to_string(),
            protocol: "Ps6mwMrF2ER2s51cp9yYpjDcuzQjsc2yAz8bQsRgdaRxw4Fk95H".to_string(),
        },
        bootstrap_lookup_addresses: vec![
            "boot.tzalpha.net".to_string(),
            "bootalpha.tzbeta.net".to_string()
        ],
        version: "TEZOS_ALPHANET_2018-11-30T15:30:56Z".to_string(),
        protocol_overrides: ProtocolOverrides {
            forced_protocol_upgrades: vec![],
            voted_protocol_overrides: vec![],
        },
    });

    env.insert(TezosEnvironment::Babylonnet, TezosEnvironmentConfiguration {
        genesis: GenesisChain {
            time: "2019-09-27T07:43:32Z".to_string(),
            block: "BLockGenesisGenesisGenesisGenesisGenesisd1f7bcGMoXy".to_string(),
            protocol: "PtBMwNZT94N7gXKw4i273CKcSaBrrBnqnt3RATExNKr9KNX2USV".to_string(),
        },
        bootstrap_lookup_addresses: vec![
            "35.246.251.120".to_string(),
            "34.89.154.253".to_string(),
            "babylonnet.kaml.fr".to_string(),
            "tezaria.com".to_string()
        ],
        version: "TEZOS_ALPHANET_BABYLON_2019-09-27T07:43:32Z".to_string(),
        protocol_overrides: ProtocolOverrides {
            forced_protocol_upgrades: vec![],
            voted_protocol_overrides: vec![],
        },
    });

    env.insert(TezosEnvironment::Mainnet, TezosEnvironmentConfiguration {
        genesis: GenesisChain {
            time: "2018-06-30T16:07:32Z".to_string(),
            block: "BLockGenesisGenesisGenesisGenesisGenesisf79b5d1CoW2".to_string(),
            protocol: "Ps9mPmXaRzmzk35gbAYNCAw6UXdE2qoABTHbN2oEEc1qM7CwT9P".to_string(),
        },
        bootstrap_lookup_addresses: vec![
            "boot.tzbeta.net".to_string()
        ],
        version: "TEZOS_BETANET_2018-06-30T16:07:32Z".to_string(),
        protocol_overrides: ProtocolOverrides {
            forced_protocol_upgrades: vec![
                (28082 as i32, "PsYLVpVvgbLhAhoqAkMFUo6gudkJ9weNXhUYCiLDzcUpFpkk8Wt".to_string()),
                (204761 as i32, "PsddFKi32cMJ2qPjf43Qv5GDWLDPZb3T3bF6fLKiF5HtvHNU7aP".to_string())
            ],
            voted_protocol_overrides: vec![
                ("PsBABY5HQTSkA4297zNHfsZNKtxULfL18y95qb3m53QJiXGmrbU".to_string(), "PsBabyM1eUXZseaJdmXFApDSBqj8YBfwELoxZHHW77EMcAbbwAS".to_string())
            ],
        },
    });

    env.insert(TezosEnvironment::Zeronet, TezosEnvironmentConfiguration {
        genesis: GenesisChain {
            time: "2019-08-06T15:18:56Z".to_string(),
            block: "BLockGenesisGenesisGenesisGenesisGenesiscde8db4cX94".to_string(),
            protocol: "PtBMwNZT94N7gXKw4i273CKcSaBrrBnqnt3RATExNKr9KNX2USV".to_string(),
        },
        bootstrap_lookup_addresses: vec![
            "bootstrap.zeronet.fun".to_string(),
            "bootzero.tzbeta.net".to_string()
        ],
        version: "TEZOS_ZERONET_2019-08-06T15:18:56Z".to_string(),
        protocol_overrides: ProtocolOverrides {
            forced_protocol_upgrades: vec![],
            voted_protocol_overrides: vec![],
        },
    });

    env
}

/// Structure holding all environment specific crucial information
pub struct TezosEnvironmentConfiguration {
    pub genesis: GenesisChain,
    pub bootstrap_lookup_addresses: Vec<String>,
    pub version: String,
    pub protocol_overrides: ProtocolOverrides,
}