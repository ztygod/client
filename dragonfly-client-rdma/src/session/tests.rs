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
        session
            .inner
            .as_mut()
            .unwrap()
            .exports
            .values_mut()
            .next()
            .unwrap()
            .fail_close_for_test(true);
        assert!(session.try_close().is_err());
        assert!(budget.usage().registered_bytes > 0);
        session
            .inner
            .as_mut()
            .unwrap()
            .exports
            .values_mut()
            .next()
            .unwrap()
            .fail_close_for_test(false);
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

fn exported_bytes(export: ExportedRegion) -> Vec<u8> {
    [
        export.id.session.to_be_bytes().as_slice(),
        export.id.generation.to_be_bytes().as_slice(),
        &region_bytes(export.region),
    ]
    .concat()
}
fn decode_export(bytes: &[u8]) -> ExportedRegion {
    assert_eq!(bytes.len(), 40);
    ExportedRegion {
        id: ExportId {
            session: u64::from_be_bytes(bytes[..8].try_into().unwrap()),
            generation: u64::from_be_bytes(bytes[8..16].try_into().unwrap()),
        },
        region: decode_region(&bytes[16..]),
    }
}
fn release_bytes(id: ExportId) -> Vec<u8> {
    [id.session.to_be_bytes(), id.generation.to_be_bytes()].concat()
}

#[test]
fn completed_exports_release_budget_and_reuse_only_deregistered_storage() {
    let mut options = options();
    options.limits.max_registered_bytes = 8192;
    options.limits.max_message_size = 1024;
    options.limits.max_registered_regions = 3;
    options.limits.max_cached_export_bytes = 4096;
    options.limits.max_cached_export_buffers = 1;
    let server_options = options.clone();
    let (ready_tx, ready_rx) = mpsc::channel();
    let server = std::thread::spawn(move || {
        let budget = ResourceBudget::new(server_options.limits.clone()).unwrap();
        let mut listener = Listener::bind_native(&server_options, budget.clone()).unwrap();
        ready_tx.send(()).unwrap();
        let mut session = listener.accept().unwrap();
        let mut previous = None;
        let mut allocation = None;
        for request in 0..128 {
            session
                .transaction(|session| {
                    assert_eq!(session.receive()?, b"read");
                    let mut data = session.reserve_export(4096)?;
                    if let Some(pointer) = allocation {
                        assert_eq!(data.as_slice().as_ptr(), pointer);
                    }
                    allocation = Some(data.as_slice().as_ptr());
                    data.as_mut_slice().fill(request as u8);
                    let export = session.export_buffer(data)?;
                    if let Some(old) = previous {
                        // SAFETY: this is a duplicate of an already completed release.
                        assert!(!unsafe { session.release_export(old)? });
                        assert_eq!(budget.usage().registered_bytes, 6144);
                    }
                    session.send(&exported_bytes(export))?;
                    assert_eq!(session.receive()?, release_bytes(export.id));
                    // SAFETY: our test peer waited for Read completion and will
                    // never post another access using this registration.
                    assert!(unsafe { session.release_export(export.id)? });
                    assert!(!unsafe { session.release_export(export.id)? });
                    assert_eq!(budget.usage().registered_bytes, 2048);
                    assert_eq!(budget.usage().registered_regions, 2);
                    assert_eq!(budget.usage().cached_export_bytes, 4096);
                    previous = Some(export.id);
                    session.send(b"released")
                })
                .unwrap();
        }
        let reserved = session.reserve_export(4096).unwrap();
        assert_eq!(budget.usage().registered_bytes, 6144);
        drop(reserved);
        assert_eq!(budget.usage().registered_bytes, 2048);
        session.try_close().unwrap();
        listener.try_close().unwrap();
        assert_eq!(budget.usage().registered_bytes, 0);
        budget.clear_export_cache();
        assert_eq!(budget.usage().cached_export_bytes, 0);
    });
    ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let budget = ResourceBudget::new(options.limits.clone()).unwrap();
    let mut session = connect(&options, budget.clone());
    for request in 0..128 {
        session
            .transaction(|session| {
                session.send(b"read")?;
                let export = decode_export(&session.receive()?);
                assert_eq!(session.read(export.region)?, vec![request as u8; 4096]);
                session.send(&release_bytes(export.id))?;
                assert_eq!(session.receive()?, b"released");
                Ok(())
            })
            .unwrap();
    }
    session.try_close().unwrap();
    assert_eq!(budget.usage().registered_bytes, 0);
    server.join().unwrap();
}

#[test]
fn release_failure_retains_registration_until_retry_and_rejects_foreign_ids() {
    let options = options();
    let server_options = options.clone();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (stop_tx, stop_rx) = mpsc::channel();
    let server = std::thread::spawn(move || {
        let budget = ResourceBudget::new(server_options.limits.clone()).unwrap();
        let mut listener = Listener::bind_native(&server_options, budget.clone()).unwrap();
        ready_tx.send(()).unwrap();
        let mut session = listener.accept().unwrap();
        let export = session.export_tracked(vec![7; 4096]).unwrap();
        let foreign = ExportId {
            session: export.id.session + 1,
            ..export.id
        };
        let future = ExportId {
            generation: export.id.generation + 1,
            ..export.id
        };
        // SAFETY: none of these exports' metadata was published to any peer.
        assert!(unsafe { session.release_export(foreign) }.is_err());
        assert!(unsafe { session.release_export(future) }.is_err());
        assert!(session.state == State::Connected);
        session
            .resources_mut()
            .unwrap()
            .exports
            .get_mut(export.id)
            .unwrap()
            .unwrap()
            .fail_close_for_test(true);
        assert!(unsafe { session.release_export(export.id) }.is_err());
        assert!(session.state == State::Failed);
        assert_eq!(budget.usage().registered_bytes, 4096);
        assert_eq!(budget.usage().registered_regions, 1);
        assert_eq!(budget.usage().cached_export_bytes, 0);
        let buffer = session
            .resources_mut()
            .unwrap()
            .exports
            .get_mut(export.id)
            .unwrap()
            .unwrap();
        assert_eq!(buffer.data().unwrap(), &[7; 4096]);
        buffer.fail_close_for_test(false);
        assert!(unsafe { session.release_export(export.id) }.unwrap());
        assert_eq!(budget.usage().registered_bytes, 0);
        assert_eq!(budget.usage().cached_export_bytes, 4096);
        session.try_close().unwrap();
        listener.try_close().unwrap();
        stop_tx.send(()).unwrap();
    });
    ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let mut session = connect(
        &options,
        ResourceBudget::new(options.limits.clone()).unwrap(),
    );
    stop_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    session.try_close().unwrap();
    server.join().unwrap();
}

#[test]
fn admission_rotates_before_byte_or_region_exhaustion_without_poisoning() {
    for region_bound in [false, true] {
        let mut options = options();
        options.limits.max_message_size = 1024;
        options.limits.max_registered_bytes = if region_bound { 64 * 1024 } else { 10240 };
        options.limits.max_registered_regions = if region_bound { 4 } else { 16 };
        let server_options = options.clone();
        let (ready_tx, ready_rx) = mpsc::channel();
        let server = std::thread::spawn(move || {
            let budget = ResourceBudget::new(server_options.limits.clone()).unwrap();
            let mut listener = Listener::bind_native(&server_options, budget.clone()).unwrap();
            ready_tx.send(()).unwrap();
            for connection in 0..3 {
                let mut session = listener.accept().unwrap();
                for request in 0..2 {
                    assert_eq!(session.receive().unwrap(), b"read");
                    let mut data = session.reserve_export(4096).unwrap();
                    data.as_mut_slice().fill((connection * 2 + request) as u8);
                    let export = session.export_buffer(data).unwrap();
                    session.send(&exported_bytes(export)).unwrap();
                    assert_eq!(session.receive().unwrap(), b"done");
                    session.send(b"ok").unwrap();
                }
                if connection < 2 {
                    assert_eq!(session.receive().unwrap(), b"read");
                    let before = budget.usage();
                    assert!(matches!(
                        session.reserve_export(4096),
                        Err(SessionError::ResourceLimit { .. })
                    ));
                    assert_eq!(before.registered_bytes, budget.usage().registered_bytes);
                    assert_eq!(before.registered_regions, budget.usage().registered_regions);
                    assert!(session.state == State::Connected);
                    session.send(b"rotate").unwrap();
                    assert_eq!(session.receive().unwrap(), b"rotate ack");
                }
                session.try_close().unwrap();
                assert_eq!(budget.usage().registered_bytes, 0);
                assert_eq!(budget.usage().registered_regions, 0);
                assert_eq!(budget.usage().sessions, 0);
            }
            listener.try_close().unwrap();
        });
        ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let budget = ResourceBudget::new(options.limits.clone()).unwrap();
        let mut session = connect(&options, budget.clone());
        let mut completed = 0;
        while completed < 6 {
            session.send(b"read").unwrap();
            let response = session.receive().unwrap();
            if response == b"rotate" {
                session.send(b"rotate ack").unwrap();
                session.try_close().unwrap();
                session = connect(&options, budget.clone());
                continue;
            }
            let export = decode_export(&response);
            assert_eq!(
                session.read(export.region).unwrap(),
                vec![completed as u8; 4096]
            );
            session.send(b"done").unwrap();
            assert_eq!(session.receive().unwrap(), b"ok");
            completed += 1;
        }
        session.try_close().unwrap();
        server.join().unwrap();
    }
}

#[test]
fn missing_release_and_failed_endpoint_close_keep_exports_live() {
    let options = options();
    let server_options = options.clone();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (stop_tx, stop_rx) = mpsc::channel();
    let server = std::thread::spawn(move || {
        let budget = ResourceBudget::new(server_options.limits.clone()).unwrap();
        let mut listener = Listener::bind_native(&server_options, budget.clone()).unwrap();
        ready_tx.send(()).unwrap();
        let mut session = listener.accept().unwrap();
        let export = session.export_tracked(vec![7; 4096]).unwrap();
        session.send(&exported_bytes(export)).unwrap();
        session.timeout = Duration::from_millis(100);
        assert!(matches!(session.receive(), Err(SessionError::Timeout(_))));
        handle::fail_close_for_test(&mut session.inner.as_mut().unwrap().ep.handle, true);
        assert!(session.try_close().is_err());
        assert!(budget.usage().registered_bytes >= 4096);
        assert_eq!(budget.usage().cached_export_bytes, 0);
        assert!(session
            .resources_mut()
            .unwrap()
            .exports
            .get_mut(export.id)
            .unwrap()
            .is_some());
        handle::fail_close_for_test(&mut session.inner.as_mut().unwrap().ep.handle, false);
        session.try_close().unwrap();
        assert_eq!(budget.usage().registered_bytes, 0);
        listener.try_close().unwrap();
        stop_tx.send(()).unwrap();
    });
    ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let mut session = connect(
        &options,
        ResourceBudget::new(options.limits.clone()).unwrap(),
    );
    let _export = decode_export(&session.receive().unwrap());
    stop_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    session.try_close().unwrap();
    server.join().unwrap();
}

#[test]
fn zero_copy_reads_reuse_storage_and_leases_survive_native_close() {
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
        assert_eq!(session.receive().unwrap(), b"done");
        session.send(b"ok").unwrap();
        session.try_close().unwrap();
        listener.try_close().unwrap();
    });
    ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let budget = ResourceBudget::new(options.limits.clone()).unwrap();
    let mut session = connect(&options, budget.clone());
    let region = decode_region(&session.receive().unwrap());
    let first_pointer = session
        .read_with(region, |data| {
            assert_eq!(data, &[7; 4096]);
            Ok(data.as_ptr() as usize)
        })
        .unwrap();
    for _ in 0..8 {
        assert_eq!(
            session
                .read_with(region, |data| Ok(data.as_ptr() as usize))
                .unwrap(),
            first_pointer
        );
    }
    let lease = session.read_lease(region).unwrap();
    assert_eq!(lease.as_slice().as_ptr() as usize, first_pointer);
    let cloned = lease.clone();
    let second = session.read_lease(region).unwrap();
    assert_ne!(lease.as_slice().as_ptr(), second.as_slice().as_ptr());
    assert_eq!(lease.as_slice(), &[7; 4096]);
    drop(second);
    session.send(b"done").unwrap();
    assert_eq!(session.receive().unwrap(), b"ok");
    session.try_close().unwrap();
    // The completed lease owns bytes/admission only, not a native Domain/MR.
    assert_eq!(budget.usage().sessions, 0);
    assert_eq!(budget.usage().registered_bytes, 4096);
    drop(lease);
    std::thread::spawn(move || assert_eq!(cloned.as_slice(), &[7; 4096]))
        .join()
        .unwrap();
    assert_eq!(budget.usage().registered_bytes, 0);
    server.join().unwrap();
}

#[test]
fn windowed_reads_preserve_input_identity_and_bound_registered_working_set() {
    use crate::ReadOptions;
    let mut options = options();
    options.limits.max_read_buffers = 4;
    options.limits.max_read_buffer_bytes = 32768;
    let server_options = options.clone();
    let (ready_tx, ready_rx) = mpsc::channel();
    let server = std::thread::spawn(move || {
        let budget = ResourceBudget::new(server_options.limits.clone()).unwrap();
        let mut listener = Listener::bind_native(&server_options, budget).unwrap();
        ready_tx.send(()).unwrap();
        let mut session = listener.accept().unwrap();
        let region = session
            .export((0..65536).map(|i| (i % 251) as u8).collect())
            .unwrap();
        session.send(&region_bytes(region)).unwrap();
        assert_eq!(session.receive().unwrap(), b"done");
        session.send(b"ok").unwrap();
        session.try_close().unwrap();
        listener.try_close().unwrap();
    });
    ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let budget = ResourceBudget::new(options.limits.clone()).unwrap();
    let mut session = connect(&options, budget.clone());
    let region = decode_region(&session.receive().unwrap());
    let regions: Vec<_> = (0..128)
        .map(|index| RemoteRegion {
            address: region.address + (index * 128) as u64,
            length: [1, 2048, 4096, 8192][index % 4],
            key: region.key,
        })
        .collect();
    for _ in 0..4 {
        let mut seen = vec![false; regions.len()];
        session
            .read_many_with(
                &regions,
                ReadOptions {
                    max_in_flight: 4,
                    max_in_flight_bytes: 16384,
                },
                |index, data| {
                    assert!(!seen[index]);
                    seen[index] = true;
                    assert_eq!(data.len(), regions[index].length as usize);
                    assert!(data
                        .iter()
                        .enumerate()
                        .all(|(offset, byte)| *byte == ((index * 128 + offset) % 251) as u8));
                    Ok(())
                },
            )
            .unwrap();
        assert!(seen.into_iter().all(|seen| seen));
        assert!(session.resources().unwrap().read_pool.len() <= 4);
        assert!(session.resources().unwrap().read_pool_bytes <= 32768);
        assert!(session.resources().unwrap().reads.is_none());
    }
    session.send(b"done").unwrap();
    assert_eq!(session.receive().unwrap(), b"ok");
    session.try_close().unwrap();
    assert_eq!(budget.usage().registered_bytes, 0);
    server.join().unwrap();
}

#[test]
fn failed_batch_retains_every_unconfirmed_operation_until_endpoint_close() {
    use crate::ReadOptions;
    let options = options();
    let server_options = options.clone();
    let (ready_tx, ready_rx) = mpsc::channel();
    let server = std::thread::spawn(move || {
        let budget = ResourceBudget::new(server_options.limits.clone()).unwrap();
        let mut listener = Listener::bind_native(&server_options, budget).unwrap();
        ready_tx.send(()).unwrap();
        let mut session = listener.accept().unwrap();
        let region = session.export(vec![3; 4096]).unwrap();
        session.send(&region_bytes(region)).unwrap();
        assert!(session.receive().is_err());
        session.try_close().unwrap();
        listener.try_close().unwrap();
    });
    ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let budget = ResourceBudget::new(options.limits.clone()).unwrap();
    let mut session = connect(&options, budget.clone());
    let region = decode_region(&session.receive().unwrap());
    let invalid = RemoteRegion {
        key: region.key.wrapping_add(1),
        ..region
    };
    let failure = session.read_many_with(
        &[region, invalid, region],
        ReadOptions::default(),
        |_, _| Ok(()),
    );
    assert!(matches!(
        failure,
        Err(SessionError::Native(_)) | Err(SessionError::Disconnected)
    ));
    assert!(!session
        .resources()
        .unwrap()
        .reads
        .as_ref()
        .unwrap()
        .is_empty());
    let retained = budget.usage().registered_bytes;
    handle::fail_close_for_test(&mut session.inner.as_mut().unwrap().ep.handle, true);
    assert!(session.try_close().is_err());
    assert_eq!(budget.usage().registered_bytes, retained);
    handle::fail_close_for_test(&mut session.inner.as_mut().unwrap().ep.handle, false);
    session.try_close().unwrap();
    assert_eq!(budget.usage().registered_bytes, 0);
    server.join().unwrap();
}

#[test]
fn callback_cancellation_drains_all_submitted_reads_and_keeps_session_reusable() {
    use crate::ReadOptions;
    let options = options();
    let server_options = options.clone();
    let (ready_tx, ready_rx) = mpsc::channel();
    let server = std::thread::spawn(move || {
        let budget = ResourceBudget::new(server_options.limits.clone()).unwrap();
        let mut listener = Listener::bind_native(&server_options, budget).unwrap();
        ready_tx.send(()).unwrap();
        let mut session = listener.accept().unwrap();
        let region = session.export(vec![5; 4096]).unwrap();
        session.send(&region_bytes(region)).unwrap();
        assert_eq!(session.receive().unwrap(), b"after cancel");
        session.send(b"ok").unwrap();
        session.try_close().unwrap();
        listener.try_close().unwrap();
    });
    ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let budget = ResourceBudget::new(options.limits.clone()).unwrap();
    let mut session = connect(&options, budget.clone());
    let region = decode_region(&session.receive().unwrap());
    let mut callbacks = 0;
    let result = session.read_many_with(&[region; 32], ReadOptions::default(), |_, data| {
        assert_eq!(data.as_slice(), &[5; 4096]);
        callbacks += 1;
        Err(SessionError::Cancelled)
    });
    assert!(matches!(result, Err(SessionError::Cancelled)));
    assert_eq!(callbacks, 1);
    assert!(session.state == State::Connected);
    assert!(session.resources().unwrap().reads.is_none());
    session.send(b"after cancel").unwrap();
    assert_eq!(session.receive().unwrap(), b"ok");
    session.try_close().unwrap();
    assert_eq!(budget.usage().registered_bytes, 0);
    server.join().unwrap();
}

#[test]
fn consumer_held_pool_capacity_observes_deadline_and_preserves_completed_bytes() {
    use crate::ReadOptions;
    let mut options = options();
    options.limits.max_read_buffers = 1;
    let server_options = options.clone();
    let (ready_tx, ready_rx) = mpsc::channel();
    let server = std::thread::spawn(move || {
        let budget = ResourceBudget::new(server_options.limits.clone()).unwrap();
        let mut listener = Listener::bind_native(&server_options, budget).unwrap();
        ready_tx.send(()).unwrap();
        let mut session = listener.accept().unwrap();
        let region = session.export(vec![6; 4096]).unwrap();
        session.send(&region_bytes(region)).unwrap();
        assert!(session.receive().is_err());
        session.try_close().unwrap();
        listener.try_close().unwrap();
    });
    ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let budget = ResourceBudget::new(options.limits.clone()).unwrap();
    let mut session = connect(&options, budget.clone());
    let region = decode_region(&session.receive().unwrap());
    session.timeout = Duration::from_millis(80);
    let mut held = None;
    let result = session.read_many_with(
        &[region; 2],
        ReadOptions {
            max_in_flight: 1,
            max_in_flight_bytes: 4096,
        },
        |_, data| {
            held = Some(data);
            Ok(())
        },
    );
    assert!(matches!(result, Err(SessionError::Timeout(_))));
    assert!(session.state == State::Failed);
    let held = held.unwrap();
    assert_eq!(held.as_slice(), &[6; 4096]);
    session.try_close().unwrap();
    assert_eq!(budget.usage().registered_bytes, 4096);
    assert_eq!(held.as_slice(), &[6; 4096]);
    drop(held);
    assert_eq!(budget.usage().registered_bytes, 0);
    server.join().unwrap();
}
