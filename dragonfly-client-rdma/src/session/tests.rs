/*
 *     Copyright 2026 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use super::*;
use crate::session::listener::Listener;
use std::net::TcpListener;
use std::sync::mpsc;

fn options() -> SessionOptions {
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    SessionOptions {
        provider: "tcp".into(),
        timeout: Duration::from_secs(5),
        ..SessionOptions::new("127.0.0.1", port)
    }
}
fn connect(options: &SessionOptions, budget: ResourceBudget) -> Session {
    // Internal software provider uses the same native path; public APIs remain verbs-only.
    Session::connect_native(options, budget).unwrap()
}
fn region_bytes(region: RemoteRegion) -> Vec<u8> {
    [
        region.length.to_be_bytes(),
        region.address.to_be_bytes(),
        region.key.to_be_bytes(),
    ]
    .concat()
}
fn decode_region(bytes: &[u8]) -> RemoteRegion {
    assert_eq!(bytes.len(), 24);
    RemoteRegion {
        length: u64::from_be_bytes(bytes[..8].try_into().unwrap()),
        address: u64::from_be_bytes(bytes[8..16].try_into().unwrap()),
        key: u64::from_be_bytes(bytes[16..24].try_into().unwrap()),
    }
}
#[test]
fn repeated_reads_reuse_control_and_local_registrations() {
    let options = options();
    let server_options = options.clone();
    let (ready_tx, ready_rx) = mpsc::channel();
    let server = std::thread::spawn(move || {
        let budget = ResourceBudget::new(server_options.limits.clone()).unwrap();
        let mut listener = Listener::bind_native(&server_options, budget.clone()).unwrap();
        ready_tx.send(()).unwrap();
        let mut session = listener.accept().unwrap();
        for length in [4096, 4096, 1, 1024 * 1024 + 17, 4 * 1024 * 1024] {
            session
                .transaction(|session| {
                    assert_eq!(session.receive()?, b"read");
                    let data: Vec<u8> = (0..length).map(|i| (i % 251) as u8).collect();
                    let region = session.export(data)?;
                    session.send(&region_bytes(region))?;
                    assert_eq!(session.receive()?, b"done");
                    session.send(b"ok")
                })
                .unwrap();
        }
        // Both registrations and the allocation survive a failed deregistration.
        session.inner.as_mut().unwrap().exports[0].fail_close_for_test(true);
        assert!(session.try_close().is_err());
        assert!(budget.usage().registered_bytes > 0);
        session.inner.as_mut().unwrap().exports[0].fail_close_for_test(false);
        session.try_close().unwrap();
        session.try_close().unwrap();
        listener.try_close().unwrap();
        assert_eq!(budget.usage().registered_bytes, 0);
        assert_eq!(budget.usage().sessions, 0);
    });
    ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let budget = ResourceBudget::new(options.limits.clone()).unwrap();
    let mut session = connect(&options, budget.clone());
    let mut first_usage = None;
    for (index, length) in [4096, 4096, 1, 1024 * 1024 + 17, 4 * 1024 * 1024]
        .into_iter()
        .enumerate()
    {
        session
            .transaction(|session| {
                session.send(b"read")?;
                let region = decode_region(&session.receive()?);
                let data = session.read(region)?;
                assert_eq!(data.len(), length);
                assert!(data.iter().enumerate().all(|(i, b)| *b == (i % 251) as u8));
                session.send(b"done")?;
                assert_eq!(session.receive()?, b"ok");
                Ok(())
            })
            .unwrap();
        if index == 0 {
            first_usage = Some(budget.usage().registered_bytes);
        }
        if index == 1 || index == 2 {
            assert_eq!(Some(budget.usage().registered_bytes), first_usage);
        }
    }
    session.try_close().unwrap();
    assert_eq!(budget.usage().registered_bytes, 0);
    assert_eq!(budget.usage().sessions, 0);
    server.join().unwrap();
}
#[test]
fn timeout_retains_pending_memory_until_endpoint_closes() {
    let options = options();
    let server_options = options.clone();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (stop_tx, stop_rx) = mpsc::channel();
    let server = std::thread::spawn(move || {
        let budget = ResourceBudget::new(server_options.limits.clone()).unwrap();
        let mut listener = Listener::bind_native(&server_options, budget).unwrap();
        ready_tx.send(()).unwrap();
        let mut session = listener.accept().unwrap();
        stop_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        session.try_close().unwrap();
        listener.try_close().unwrap();
    });
    ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let budget = ResourceBudget::new(options.limits.clone()).unwrap();
    let mut session = connect(&options, budget.clone());
    session.timeout = Duration::from_millis(100);
    assert!(matches!(session.receive(), Err(SessionError::Timeout(_))));
    assert!(session.resources().unwrap().pending.is_some());
    assert!(matches!(
        session.send(b"retry"),
        Err(SessionError::InvalidState {
            state: "failed",
            ..
        })
    ));
    handle::fail_close_for_test(&mut session.inner.as_mut().unwrap().ep.handle, true);
    assert!(session.try_close().is_err());
    assert!(budget.usage().registered_bytes > 0);
    assert_eq!(budget.usage().sessions, 1);
    handle::fail_close_for_test(&mut session.inner.as_mut().unwrap().ep.handle, false);
    session.try_close().unwrap();
    assert_eq!(budget.usage().registered_bytes, 0);
    assert_eq!(budget.usage().sessions, 0);
    stop_tx.send(()).unwrap();
    server.join().unwrap();
}
#[test]
fn total_deadline_is_not_reset_by_each_message() {
    let options = options();
    let server_options = options.clone();
    let (ready_tx, ready_rx) = mpsc::channel();
    let server = std::thread::spawn(move || {
        let budget = ResourceBudget::new(server_options.limits.clone()).unwrap();
        let mut listener = Listener::bind_native(&server_options, budget).unwrap();
        ready_tx.send(()).unwrap();
        let mut session = listener.accept().unwrap();
        assert_eq!(session.receive().unwrap(), b"start");
        std::thread::sleep(Duration::from_millis(80));
        session.send(b"first").unwrap();
        std::thread::sleep(Duration::from_millis(200));
        session.try_close().unwrap();
        listener.try_close().unwrap();
    });
    ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let budget = ResourceBudget::new(options.limits.clone()).unwrap();
    let mut session = connect(&options, budget);
    session.timeout = Duration::from_millis(180);
    let result = session.transaction(|session| {
        session.send(b"start")?;
        assert_eq!(session.receive()?, b"first");
        session.receive()
    });
    assert!(matches!(result, Err(SessionError::Timeout(_))));
    session.try_close().unwrap();
    server.join().unwrap();
}
#[test]
fn listener_deadline_and_failed_connect_are_bounded() {
    let mut options = options();
    options.timeout = Duration::from_millis(100);
    let budget = ResourceBudget::new(options.limits.clone()).unwrap();
    assert!(Session::connect_native(&options, budget.clone()).is_err());
    assert_eq!(budget.usage().sessions, 0);
    let mut listener = Listener::bind_native(&options, budget).unwrap();
    assert!(matches!(
        listener.accept(),
        Err(SessionError::Timeout("accept"))
    ));
    for timeout in [Duration::ZERO, Duration::MAX] {
        assert!(matches!(
            listener.accept_with_timeout(timeout),
            Err(SessionError::InvalidOptions(_))
        ));
    }
    // A timed-out idle wait leaves the listener reusable.
    assert!(matches!(
        listener.accept_with_timeout(Duration::from_millis(10)),
        Err(SessionError::Timeout("accept"))
    ));
    listener.try_close().unwrap();
}
#[test]
fn admission_rejects_live_exports_without_freeing_their_memory() {
    let mut options = options();
    options.limits.max_registered_bytes = 8192;
    options.limits.max_message_size = 1024;
    let server_options = options.clone();
    let (ready_tx, ready_rx) = mpsc::channel();
    let server = std::thread::spawn(move || {
        let budget = ResourceBudget::new(server_options.limits.clone()).unwrap();
        let mut listener = Listener::bind_native(&server_options, budget.clone()).unwrap();
        ready_tx.send(()).unwrap();
        let mut session = listener.accept().unwrap();
        session.export(vec![1; 4096]).unwrap();
        assert!(matches!(
            session.export(vec![2; 4097]),
            Err(SessionError::ResourceLimit { .. })
        ));
        assert_eq!(budget.usage().registered_bytes, 4096);
        session.try_close().unwrap();
        listener.try_close().unwrap();
        assert_eq!(budget.usage().registered_bytes, 0);
    });
    ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let budget = ResourceBudget::new(options.limits.clone()).unwrap();
    let mut session = connect(&options, budget);
    let _ = session.receive();
    session.try_close().unwrap();
    server.join().unwrap();
}

#[test]
fn provider_read_errors_preserve_diagnostics_and_cleanup() {
    let options = options();
    let server_options = options.clone();
    let (ready_tx, ready_rx) = mpsc::channel();
    let server = std::thread::spawn(move || {
        let budget = ResourceBudget::new(server_options.limits.clone()).unwrap();
        let mut listener = Listener::bind_native(&server_options, budget).unwrap();
        ready_tx.send(()).unwrap();
        let mut session = listener.accept().unwrap();
        let region = session.export(vec![7; 4096]).unwrap();
        session.send(&region_bytes(region)).unwrap();
        // Drive the remote provider while it processes the invalid RMA request.
        let failure = session.receive().unwrap_err();
        if let SessionError::Native(error) = &failure {
            assert!(!error.message.is_empty());
        }
        session.try_close().unwrap();
        listener.try_close().unwrap();
        failure
    });
    ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let budget = ResourceBudget::new(options.limits.clone()).unwrap();
    let mut session = connect(&options, budget.clone());
    let mut region = decode_region(&session.receive().unwrap());
    region.key = region.key.wrapping_add(1);
    match session.read(region) {
        Err(SessionError::Native(error)) => assert!(!error.message.is_empty()),
        Err(SessionError::Disconnected) => {}
        other => panic!("expected provider failure or disconnect, got {other:?}"),
    }
    session.try_close().unwrap();
    assert_eq!(budget.usage().registered_bytes, 0);
    assert!(matches!(server.join().unwrap(), SessionError::Native(_)));
}
