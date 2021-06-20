// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::thread;
use std::time::Duration;

use anyhow::format_err;
use serial_test::serial;

use ipc::*;

mod common;

#[test]
#[serial]
fn ipc_fork_and_client_exchange() -> Result<(), anyhow::Error> {
    let sock_path = temp_sock();
    assert!(!sock_path.exists());

    let child_pid = common::fork(|| {
        // wait for socket/bind to be ready
        let sock_path = sock_path.as_path();
        while !sock_path.exists() {
            thread::sleep(Duration::from_millis(20));
        }

        // try connect
        let client: IpcClient<String, String> = IpcClient::new(&sock_path);
        let (mut rx, mut tx) = client.connect().unwrap();

        // test send/receive
        tx.send(&String::from("hello")).unwrap();
        let recv = rx.receive().unwrap();
        assert_eq!(recv, "quick");

        tx.send(&String::from("this is")).unwrap();
        let recv = rx.receive().unwrap();
        assert_eq!(recv, "brown");

        tx.send(&String::from("dog")).unwrap();
        let recv = rx.receive().unwrap();
        assert_eq!(recv, "fox");
    });
    assert!(child_pid > 0);

    // bind and accept server
    let mut server: IpcServer<String, String> = IpcServer::bind_path(&sock_path)?;
    let (mut rx, mut tx) = server.try_accept(Duration::from_secs(10))?;

    // test send/receive
    tx.send(&String::from("quick"))?;
    let recv = rx.receive()?;
    assert_eq!(recv, "hello");

    tx.send(&String::from("brown"))?;
    let recv = rx.receive()?;
    assert_eq!(recv, "this is");

    tx.send(&String::from("fox"))?;
    let recv = rx.receive()?;
    assert_eq!(recv, "dog");

    assert!(common::wait(child_pid));

    Ok(())
}

#[test]
#[serial]
// TODO: TE-218, ignored in PR #521, re-enable once TE-218 has been solved
#[cfg_attr(target_os = "macos", ignore)]
fn ipc_fork_and_try_read_with_timeout() -> Result<(), anyhow::Error> {
    let sock_path = temp_sock();
    assert!(!sock_path.exists());

    let child_pid = common::fork(|| {
        // wait for socket/bind to be ready
        let sock_path = sock_path.as_path();
        while !sock_path.exists() {
            thread::sleep(Duration::from_millis(20));
        }

        // try connect
        let client: IpcClient<String, String> = IpcClient::new(&sock_path);
        let (mut rx, mut tx) = client.connect().unwrap();

        // test send/receive
        tx.send(&String::from("hello")).unwrap();
        let recv = rx.receive().unwrap();
        assert_eq!(recv, "quick");

        // sleep (5s) a little bit
        thread::sleep(Duration::from_secs(5));
        tx.send(&String::from("hello_after_2_seconds")).unwrap();
        tx.send(&String::from("hello_after_another_3_seconds"))
            .unwrap();

        // stop sending
        panic!("Oooops - I dont send anything more - dont be afraid, this is expected panic :)");
    });
    assert!(child_pid > 0);

    // bind and accept server
    let mut server: IpcServer<String, String> = IpcServer::bind_path(&sock_path)?;
    let (mut rx, mut tx) = server.try_accept(Duration::from_secs(10)).unwrap();

    // 1. test send/receive - direct without timeout
    tx.send(&String::from("quick")).unwrap();
    let recv = rx.receive().unwrap();
    assert_eq!(recv, "hello");

    // 1. try read something with timeout but nothing comes
    match rx.try_receive(Some(Duration::from_secs(1)), None) {
        Err(IpcError::ReceiveMessageTimeouted {}) => (/* ok */),
        Err(e) => return Err(format_err!("Unexpected result: {:?}", e)),
        Ok(_) => return Err(format_err!("Unexpected result")),
    }

    // 2. try read something with timeout
    match rx.try_receive(Some(Duration::from_secs(5)), None) {
        Ok(recv) => assert_eq!(recv, "hello_after_2_seconds"),
        Err(e) => return Err(format_err!("Unexpected result: {:?}", e)),
    }

    // 3. remove read timeout and read
    rx.set_read_timeout(None)?;
    match rx.receive() {
        Ok(recv) => assert_eq!(recv, "hello_after_another_3_seconds"),
        Err(e) => return Err(format_err!("Unexpected result: {:?}", e)),
    }

    // 4. try ready one more
    match rx.receive() {
        Err(IpcError::ReceiveMessageTimeouted { .. }) => {
            Err(format_err!("Unexpected IpcError::ReceiveMessageTimeouted"))
        }
        Err(_) => {
            assert!(common::wait(child_pid));
            Ok(())
        }
        Ok(_) => Err(format_err!("Unexpected result")),
    }
}

#[test]
#[serial]
fn ipc_accept_timeout() -> Result<(), anyhow::Error> {
    let mut server: IpcServer<String, String> = IpcServer::bind_path(&temp_sock())?;
    let result = server.try_accept(Duration::from_secs(2));

    match result {
        Err(IpcError::AcceptTimeout { .. }) => Ok(()),
        Err(e) => Err(format_err!("Unexpected result: {:?}", e)),
        Ok(_) => Err(format_err!("Unexpected result")),
    }
}
