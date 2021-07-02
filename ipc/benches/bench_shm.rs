// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT
#![feature(test)]

extern crate test;

use std::io;
use std::thread;
use std::time::Duration;
use test::Bencher;

use rand::Rng;
use serde::{Deserialize, Serialize};

use ipc::{temp_sock, IpcClient, IpcServer};

#[derive(Serialize, Deserialize, Clone)]
struct BenchData {
    block_hash: Vec<u8>,
    chain_hash: Vec<u8>,
}

impl BenchData {
    fn generate_small() -> Self {
        let mut rng = rand::thread_rng();
        Self {
            block_hash: (0..100).map(|_| rng.gen_range(0, 254)).collect(),
            chain_hash: (0..64).map(|_| rng.gen_range(0, 254)).collect(),
        }
    }

    fn generate_big() -> Self {
        let mut rng = rand::thread_rng();
        Self {
            block_hash: (0..2000).map(|_| rng.gen_range(0, 254)).collect(),
            chain_hash: (0..640).map(|_| rng.gen_range(0, 254)).collect(),
        }
    }

    fn generate_huge() -> Self {
        let mut rng = rand::thread_rng();
        Self {
            block_hash: (0..2000 * 33).map(|_| rng.gen_range(0, 254)).collect(),
            chain_hash: (0..2000 * 40).map(|_| rng.gen_range(0, 254)).collect(),
        }
    }
}

fn fork<F: FnOnce()>(child_func: F) -> libc::pid_t {
    unsafe {
        match libc::fork() {
            -1 => panic!("fork failed: {}", io::Error::last_os_error()),
            0 => {
                child_func();
                libc::exit(0);
            }
            pid => pid,
        }
    }
}

fn rand_file_name() -> String {
    let path = temp_sock();
    path.as_os_str().to_str().unwrap().to_string()
}

#[bench]
fn bench_ipmspc_shm_small_response(b: &mut Bencher) {
    use ipmpsc::*;
    let name_parent = rand_file_name();
    let name_child = rand_file_name();
    let buffer_parent = SharedRingBuffer::create(&name_parent, 32 * 1024 * 1024).unwrap();
    let buffer_child = SharedRingBuffer::create(&name_child, 32 * 1024 * 1024).unwrap();

    let child_pid = fork(|| {
        let rx = Receiver::new(buffer_child);
        let tx = Sender::new(SharedRingBuffer::open(&name_parent).unwrap());
        let bench_data = BenchData::generate_small();
        while rx.recv::<BenchData>().is_ok() {
            tx.send(&bench_data).unwrap();
        }
    });
    assert!(child_pid > 0);

    let rx = Receiver::new(buffer_parent);
    let tx = Sender::new(SharedRingBuffer::open(&name_child).unwrap());

    let bench_data = BenchData::generate_small();
    b.iter(|| {
        for _ in 0..100 {
            tx.send(&bench_data).unwrap();
            let _ = rx.recv::<BenchData>().unwrap();
        }
    });
}

#[bench]
fn bench_ipmspc_shm_big_response(b: &mut Bencher) {
    use ipmpsc::*;
    let name_parent = rand_file_name();
    let name_child = rand_file_name();
    let buffer_parent = SharedRingBuffer::create(&name_parent, 32 * 1024 * 1024).unwrap();
    let buffer_child = SharedRingBuffer::create(&name_child, 32 * 1024 * 1024).unwrap();

    let child_pid = fork(|| {
        let rx = Receiver::new(buffer_child);
        let tx = Sender::new(SharedRingBuffer::open(&name_parent).unwrap());
        let bench_data = BenchData::generate_big();
        while rx.recv::<BenchData>().is_ok() {
            tx.send(&bench_data).unwrap();
        }
    });
    assert!(child_pid > 0);

    let rx = Receiver::new(buffer_parent);
    let tx = Sender::new(SharedRingBuffer::open(&name_child).unwrap());

    let bench_data = BenchData::generate_small();
    b.iter(|| {
        for _ in 0..100 {
            tx.send(&bench_data).unwrap();
            let _ = rx.recv::<BenchData>().unwrap();
        }
    });
}

#[bench]
fn bench_ipmspc_shm_huge_response(b: &mut Bencher) {
    use ipmpsc::*;
    let name_parent = rand_file_name();
    let name_child = rand_file_name();
    let buffer_parent = SharedRingBuffer::create(&name_parent, 32 * 1024 * 1024).unwrap();
    let buffer_child = SharedRingBuffer::create(&name_child, 32 * 1024 * 1024).unwrap();

    let child_pid = fork(|| {
        let rx = Receiver::new(buffer_child);
        let tx = Sender::new(SharedRingBuffer::open(&name_parent).unwrap());
        let bench_data = BenchData::generate_huge();
        while rx.recv::<BenchData>().is_ok() {
            tx.send(&bench_data).unwrap();
        }
    });
    assert!(child_pid > 0);

    let rx = Receiver::new(buffer_parent);
    let tx = Sender::new(SharedRingBuffer::open(&name_child).unwrap());

    let bench_data = BenchData::generate_small();
    b.iter(|| {
        for _ in 0..100 {
            tx.send(&bench_data).unwrap();
            let _ = rx.recv::<BenchData>().unwrap();
        }
    });
}

#[bench]
fn bench_ipc_channel_small_response(b: &mut Bencher) {
    use ipc_channel::*;
    let (ctx, crx) = ipc::channel::<BenchData>().unwrap();
    let (ptx, prx) = ipc::channel::<BenchData>().unwrap();

    let child_pid = fork(|| {
        let bench_data = BenchData::generate_small();
        while crx.recv().is_ok() {
            ptx.send(bench_data.clone()).unwrap();
        }
    });
    assert!(child_pid > 0);

    let bench_data = BenchData::generate_small();
    b.iter(|| {
        for _ in 0..100 {
            ctx.send(bench_data.clone()).unwrap();
            let _ = prx.recv().unwrap();
        }
    });
}

#[bench]
fn bench_ipc_channel_big_response(b: &mut Bencher) {
    use ipc_channel::*;
    let (ctx, crx) = ipc::channel::<BenchData>().unwrap();
    let (ptx, prx) = ipc::channel::<BenchData>().unwrap();

    let child_pid = fork(|| {
        let bench_data = BenchData::generate_big();
        while crx.recv().is_ok() {
            ptx.send(bench_data.clone()).unwrap();
        }
    });
    assert!(child_pid > 0);

    let bench_data = BenchData::generate_small();
    b.iter(|| {
        for _ in 0..100 {
            ctx.send(bench_data.clone()).unwrap();
            let _ = prx.recv().unwrap();
        }
    });
}

#[bench]
fn bench_ipc_channel_huge_response(b: &mut Bencher) {
    use ipc_channel::*;
    let (ctx, crx) = ipc::channel::<BenchData>().unwrap();
    let (ptx, prx) = ipc::channel::<BenchData>().unwrap();

    let child_pid = fork(|| {
        let bench_data = BenchData::generate_huge();
        while crx.recv().is_ok() {
            ptx.send(bench_data.clone()).unwrap();
        }
    });
    assert!(child_pid > 0);

    let bench_data = BenchData::generate_small();
    b.iter(|| {
        for _ in 0..100 {
            ctx.send(bench_data.clone()).unwrap();
            let _ = prx.recv().unwrap();
        }
    });
}

#[bench]
fn bench_ipc_channel_small_response_bytes(b: &mut Bencher) {
    use ipc_channel::*;
    let (ctx, crx) = ipc::bytes_channel().unwrap();
    let (ptx, prx) = ipc::bytes_channel().unwrap();

    let child_pid = fork(|| {
        let bench_data = BenchData::generate_small();
        let mut bytes = Vec::new();
        bincode::serialize_into(&mut bytes, &bench_data).unwrap();
        while crx.recv().is_ok() {
            ptx.send(&bytes).unwrap();
        }
    });
    assert!(child_pid > 0);

    let bench_data = BenchData::generate_small();
    let mut bytes = Vec::new();
    bincode::serialize_into(&mut bytes, &bench_data).unwrap();
    b.iter(move || {
        for _ in 0..100 {
            ctx.send(&bytes).unwrap();
            let _ = prx.recv().unwrap();
        }
    });
}

#[bench]
fn bench_ipc_channel_big_response_bytes(b: &mut Bencher) {
    use ipc_channel::*;
    let (ctx, crx) = ipc::bytes_channel().unwrap();
    let (ptx, prx) = ipc::bytes_channel().unwrap();

    let child_pid = fork(|| {
        let bench_data = BenchData::generate_big();
        let mut bytes = Vec::new();
        bincode::serialize_into(&mut bytes, &bench_data).unwrap();
        while crx.recv().is_ok() {
            ptx.send(&bytes).unwrap();
        }
    });
    assert!(child_pid > 0);

    let bench_data = BenchData::generate_small();
    let mut bytes = Vec::new();
    bincode::serialize_into(&mut bytes, &bench_data).unwrap();
    b.iter(move || {
        for _ in 0..100 {
            ctx.send(&bytes).unwrap();
            let _ = prx.recv().unwrap();
        }
    });
}

#[bench]
fn bench_ipc_channel_huge_response_bytes(b: &mut Bencher) {
    use ipc_channel::*;
    let (ctx, crx) = ipc::bytes_channel().unwrap();
    let (ptx, prx) = ipc::bytes_channel().unwrap();

    let child_pid = fork(|| {
        let bench_data = BenchData::generate_huge();
        let mut bytes = Vec::new();
        bincode::serialize_into(&mut bytes, &bench_data).unwrap();
        while crx.recv().is_ok() {
            ptx.send(&bytes).unwrap();
        }
    });
    assert!(child_pid > 0);

    let bench_data = BenchData::generate_small();
    let mut bytes = Vec::new();
    bincode::serialize_into(&mut bytes, &bench_data).unwrap();
    b.iter(move || {
        for _ in 0..100 {
            ctx.send(&bytes).unwrap();
            let _ = prx.recv().unwrap();
        }
    });
}

#[bench]
fn bench_unix_socket_small_response(b: &mut Bencher) {
    let sock_path = temp_sock();

    let child_pid = fork(|| {
        // wait for parent to be ready
        let sock_path = sock_path.as_path();
        while !sock_path.exists() {
            thread::sleep(Duration::from_millis(20));
        }

        let client: IpcClient<BenchData, BenchData> = IpcClient::new(&sock_path);
        let (mut rx, mut tx) = client.connect().unwrap();
        let bench_data = BenchData::generate_small();
        while rx.receive().is_ok() {
            tx.send(&bench_data).unwrap();
        }
    });
    assert!(child_pid > 0);

    let mut server: IpcServer<BenchData, BenchData> = IpcServer::bind_path(&sock_path).unwrap();
    let (mut rx, mut tx) = server.try_accept(Duration::from_secs(3)).unwrap();

    let bench_data = BenchData::generate_small();
    b.iter(|| {
        for _ in 0..100 {
            tx.send(&bench_data).unwrap();
            let _ = rx.receive().unwrap();
        }
    });
}

#[bench]
fn bench_unix_socket_big_response(b: &mut Bencher) {
    let sock_path = temp_sock();

    let child_pid = fork(|| {
        // wait for parent to be ready
        let sock_path = sock_path.as_path();
        while !sock_path.exists() {
            thread::sleep(Duration::from_millis(20));
        }

        let client: IpcClient<BenchData, BenchData> = IpcClient::new(&sock_path);
        let (mut rx, mut tx) = client.connect().unwrap();
        let bench_data = BenchData::generate_big();
        while rx.receive().is_ok() {
            tx.send(&bench_data).unwrap();
        }
    });
    assert!(child_pid > 0);

    let mut server: IpcServer<BenchData, BenchData> = IpcServer::bind_path(&sock_path).unwrap();
    let (mut rx, mut tx) = server.try_accept(Duration::from_secs(3)).unwrap();

    let bench_data = BenchData::generate_small();
    b.iter(|| {
        for _ in 0..100 {
            tx.send(&bench_data).unwrap();
            let _ = rx.receive().unwrap();
        }
    });
}

#[bench]
fn bench_unix_socket_huge_response(b: &mut Bencher) {
    let sock_path = temp_sock();

    let child_pid = fork(|| {
        // wait for parent to be ready
        let sock_path = sock_path.as_path();
        while !sock_path.exists() {
            thread::sleep(Duration::from_millis(20));
        }

        let client: IpcClient<BenchData, BenchData> = IpcClient::new(&sock_path);
        let (mut rx, mut tx) = client.connect().unwrap();
        let bench_data = BenchData::generate_huge();
        while rx.receive().is_ok() {
            tx.send(&bench_data).unwrap();
        }
    });
    assert!(child_pid > 0);

    let mut server: IpcServer<BenchData, BenchData> = IpcServer::bind_path(&sock_path).unwrap();
    let (mut rx, mut tx) = server.try_accept(Duration::from_secs(3)).unwrap();

    let bench_data = BenchData::generate_small();
    b.iter(|| {
        for _ in 0..100 {
            tx.send(&bench_data).unwrap();
            let _ = rx.receive().unwrap();
        }
    });
}
