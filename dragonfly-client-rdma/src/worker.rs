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

//! Async command boundary for a thread-affine native Session.

use crate::{ResourceBudget, Session, SessionError, SessionOptions};
use tokio::sync::{mpsc, oneshot};

// Closures cross the thread boundary; the session and native handles never do.
type Job = Box<dyn FnOnce(&mut Session) + Send>;
enum Command {
    Run(Job),
    Close(oneshot::Sender<Result<(), SessionError>>),
}

/// Bounded async handle to a connection owned by one dedicated OS thread.
///
/// Submit an entire request in execute() to share its deadline and prevent
/// interleaving. Dropping an awaiting future never frees in-flight native memory:
/// the worker finishes or times out the request and then owns cleanup.
/// Drop disconnects the queue without blocking the async runtime. Use close()
/// when cleanup errors must be observed and retried.
pub struct SessionWorker {
    sender: mpsc::Sender<Command>,
    budget: ResourceBudget,
    timeout: std::time::Duration,
}
impl SessionWorker {
    pub async fn connect(
        options: SessionOptions,
        queue_capacity: usize,
    ) -> Result<Self, SessionError> {
        let budget = ResourceBudget::new(options.limits.clone())?;
        Self::connect_with_budget(options, queue_capacity, budget).await
    }
    pub async fn connect_with_budget(
        options: SessionOptions,
        queue_capacity: usize,
        budget: ResourceBudget,
    ) -> Result<Self, SessionError> {
        options.validate()?;
        Self::connect_using(
            options,
            queue_capacity,
            budget,
            Session::connect_with_budget,
        )
        .await
    }
    async fn connect_using(
        options: SessionOptions,
        queue_capacity: usize,
        budget: ResourceBudget,
        connect: fn(&SessionOptions, ResourceBudget) -> Result<Session, SessionError>,
    ) -> Result<Self, SessionError> {
        if queue_capacity == 0 || queue_capacity > 1024 {
            return Err(SessionError::InvalidOptions(
                "worker queue capacity must be in 1..=1024",
            ));
        }
        let timeout = options.timeout;
        let (sender, mut receiver) = mpsc::channel(queue_capacity);
        let (ready_tx, ready_rx) = oneshot::channel();
        let thread_budget = budget.clone();
        std::thread::Builder::new()
            .name("dragonfly-rdma".into())
            .spawn(move || {
                let mut session = match connect(&options, thread_budget) {
                    Ok(session) => session,
                    Err(error) => {
                        let _ = ready_tx.send(Err(error));
                        return;
                    }
                };
                // A cancelled connect must close the connection on its owning thread.
                if ready_tx.send(Ok(())).is_err() {
                    return;
                }
                while let Some(command) = receiver.blocking_recv() {
                    match command {
                        Command::Run(job) => job(&mut session),
                        Command::Close(reply) => {
                            let result = session.try_close();
                            let closed = result.is_ok();
                            let _ = reply.send(result);
                            if closed {
                                return;
                            }
                        }
                    }
                }
                // Session::Drop preserves the resource bundle if provider close fails.
            })
            .map_err(|_| SessionError::WorkerStopped)?;
        ready_rx.await.map_err(|_| SessionError::WorkerStopped)??;
        Ok(Self {
            sender,
            budget,
            timeout,
        })
    }
    /// Run a complete request on the native owner, including queue wait in its deadline.
    pub async fn execute<T: Send + 'static>(
        &mut self,
        operation: impl FnOnce(&mut Session) -> Result<T, SessionError> + Send + 'static,
    ) -> Result<T, SessionError> {
        let deadline = std::time::Instant::now() + self.timeout;
        let (reply, result) = oneshot::channel();
        let job = Box::new(move |session: &mut Session| {
            if reply.is_closed() {
                return;
            }
            let value = if std::time::Instant::now() >= deadline {
                Err(SessionError::Timeout("worker queue"))
            } else {
                session.transaction_until(deadline, operation)
            };
            let _ = reply.send(value);
        });
        // Bound queue admission without detaching the native request on timeout.
        tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            self.sender.send(Command::Run(job)),
        )
        .await
        .map_err(|_| SessionError::Timeout("worker queue"))?
        .map_err(|_| SessionError::WorkerStopped)?;
        result.await.map_err(|_| SessionError::WorkerStopped)?
    }
    pub fn resource_usage(&self) -> crate::ResourceUsage {
        self.budget.usage()
    }
    /// Retryable explicit cleanup; a failed close leaves the worker owning resources.
    pub async fn close(&mut self) -> Result<(), SessionError> {
        let (reply, result) = oneshot::channel();
        self.sender
            .send(Command::Close(reply))
            .await
            .map_err(|_| SessionError::WorkerStopped)?;
        result.await.map_err(|_| SessionError::WorkerStopped)?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn invalid_worker_options_are_rejected_before_starting_a_thread() {
        let options = SessionOptions::new("127.0.0.1", 1);
        assert!(matches!(
            SessionWorker::connect(options, 0).await,
            Err(SessionError::InvalidOptions(_))
        ));
    }
    #[cfg(all(target_os = "linux", feature = "rdma"))]
    #[tokio::test]
    async fn cancelled_waiter_does_not_free_in_flight_resources_or_interleave_requests() {
        use crate::session::{connection::Session, listener::Listener};
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let options = SessionOptions {
            provider: "tcp".into(),
            ..SessionOptions::new("127.0.0.1", port)
        };
        let server_options = options.clone();
        let (ready_tx, ready_rx) = oneshot::channel();
        let (started_tx, started_rx) = oneshot::channel();
        let server = std::thread::spawn(move || {
            let budget = ResourceBudget::new(server_options.limits.clone()).unwrap();
            let mut listener = Listener::bind_native(&server_options, budget).unwrap();
            ready_tx.send(()).unwrap();
            let mut session = listener.accept().unwrap();
            assert_eq!(session.receive().unwrap(), b"first");
            started_tx.send(()).unwrap();
            std::thread::sleep(std::time::Duration::from_millis(100));
            session.send(b"first done").unwrap();
            assert_eq!(session.receive().unwrap(), b"second");
            session.send(b"second done").unwrap();
            session.try_close().unwrap();
            listener.try_close().unwrap();
        });
        ready_rx.await.unwrap();
        let budget = ResourceBudget::new(options.limits.clone()).unwrap();
        let mut worker =
            SessionWorker::connect_using(options, 1, budget.clone(), Session::connect_native)
                .await
                .unwrap();
        let mut request = Box::pin(worker.execute(|session| {
            session.send(b"first")?;
            session.receive()
        }));
        tokio::select! {
            result = &mut request => panic!("request finished before cancellation: {result:?}"),
            _ = started_rx => {}
        }
        drop(request);
        assert!(budget.usage().registered_bytes > 0);
        let second = worker
            .execute(|session| {
                session.send(b"second")?;
                session.receive()
            })
            .await
            .unwrap();
        assert_eq!(second, b"second done");
        worker.close().await.unwrap();
        assert_eq!(budget.usage().registered_bytes, 0);
        assert_eq!(budget.usage().sessions, 0);
        server.join().unwrap();
    }
}
