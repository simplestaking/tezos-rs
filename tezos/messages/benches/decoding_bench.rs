// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

#![allow(dead_code)]

use std::{fs::File, io::Read, path::PathBuf};

use anyhow::{Context, Error};
use criterion::{black_box, Criterion};
use tezos_messages::p2p::binary_message::BinaryRead;

pub fn read_data(file: &str) -> Result<Vec<u8>, Error> {
    let dir =
        std::env::var("CARGO_MANIFEST_DIR").context(format!("`CARGO_MANIFEST_DIR` is not set"))?;
    let path = PathBuf::from(dir).join("resources").join(file);
    let data = File::open(&path)
        .and_then(|mut file| {
            let mut data = Vec::new();
            file.read_to_end(&mut data)?;
            Ok(data)
        })
        .with_context(|| format!("Cannot read message from {}", path.to_string_lossy()))?;
    Ok(data)
}

pub fn read_data_unwrap(file: &str) -> Vec<u8> {
    read_data(file).unwrap_or_else(|e| panic!("Unexpected error: {}", e))
}

pub fn bench_decode<T: BinaryRead>(c: &mut Criterion, message: &str, data: &[u8]) {
    c.bench_function(&format!("{}-decode-nom", message), |b| {
        b.iter(|| {
            let _ = black_box(T::from_bytes(&data)).expect("Failed to decode with nom");
        });
    });
}
