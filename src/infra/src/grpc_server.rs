// Copyright 2026 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

//! Connection liveness and shutdown settings shared by the node and router gRPC servers.

use std::{future::Future, time::Duration};

use tokio::sync::oneshot;
use tonic::transport::Server;

/// Enables HTTP/2 keepalive PINGs on every server connection, idle ones included.
///
/// A client that vanishes without closing its TCP connection (spot reclaim, frozen process)
/// sends no FIN or RST, so without PINGs the server keeps the connection ESTABLISHED forever.
/// Graceful shutdown then waits on it: h2 sends GOAWAY plus a PING and waits for the PING ACK
/// with no timeout. With keepalive the connection is closed once a PING is not acknowledged
/// within `timeout_secs`. `interval_secs == 0` leaves keepalive disabled.
pub fn with_keepalive(builder: Server, interval_secs: u64, timeout_secs: u64) -> Server {
    if interval_secs == 0 {
        return builder;
    }
    builder
        .http2_keepalive_interval(Some(Duration::from_secs(interval_secs)))
        .http2_keepalive_timeout(Some(Duration::from_secs(timeout_secs.max(1))))
}

/// Drives `serve` (a tonic `serve_with_shutdown` future) and bounds its graceful drain.
///
/// `shutdown_started` must fire when the shutdown signal passed to `serve` resolves. From then
/// on the server gets at most `drain_timeout` to finish its connections. When the bound is hit
/// this logs a warning and returns `Ok(())`; dropping `serve` stops waiting for connections
/// whose peers never answer, and their tasks end when the gRPC runtime shuts down.
pub async fn serve_with_drain_timeout<F, E>(
    serve: F,
    shutdown_started: oneshot::Receiver<()>,
    drain_timeout: Duration,
) -> Result<(), E>
where
    F: Future<Output = Result<(), E>>,
{
    tokio::pin!(serve);
    tokio::select! {
        biased;
        ret = &mut serve => return ret,
        // A RecvError means the signal future was dropped without firing. tonic drops it only
        // when it stops serving, so the bounded wait below covers that case as well.
        _ = shutdown_started => {}
    }
    match tokio::time::timeout(drain_timeout, serve).await {
        Ok(ret) => ret,
        Err(_) => {
            log::warn!(
                "gRPC server graceful shutdown did not finish within {}s, dropping the remaining connections",
                drain_timeout.as_secs()
            );
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        task::JoinHandle,
        time::Instant,
    };
    use tonic::{service::Routes, transport::server::TcpIncoming};

    use super::*;

    const H2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
    // Frame header: length 0, type SETTINGS (0x4), no flags, stream 0.
    const H2_EMPTY_SETTINGS: [u8; 9] = [0, 0, 0, 0x4, 0, 0, 0, 0, 0];

    struct TestServer {
        addr: SocketAddr,
        shutdown_tx: oneshot::Sender<()>,
        handle: JoinHandle<Result<(), tonic::transport::Error>>,
    }

    /// Wires the server the same way as `init_common_grpc_server`: keepalive on the builder,
    /// a shutdown signal that also reports when it fired, and an optional drain bound.
    async fn spawn_server(
        keepalive_interval_secs: u64,
        keepalive_timeout_secs: u64,
        drain_timeout: Option<Duration>,
    ) -> TestServer {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let (drain_tx, drain_rx) = oneshot::channel::<()>();
        let mut builder = with_keepalive(
            Server::builder(),
            keepalive_interval_secs,
            keepalive_timeout_secs,
        );
        let serve = builder
            .add_routes(Routes::default())
            .serve_with_incoming_shutdown(TcpIncoming::from(listener), async move {
                shutdown_rx.await.ok();
                drain_tx.send(()).ok();
            });
        let handle = tokio::spawn(async move {
            match drain_timeout {
                Some(timeout) => serve_with_drain_timeout(serve, drain_rx, timeout).await,
                None => serve.await,
            }
        });
        TestServer {
            addr,
            shutdown_tx,
            handle,
        }
    }

    /// Opens an HTTP/2 connection and then never writes again, so it never acknowledges a
    /// PING: from the server's point of view this is a peer that vanished. Returns after the
    /// server's first frame (its SETTINGS) arrived, so the connection is being served.
    async fn connect_frozen_h2_client(addr: SocketAddr) -> TcpStream {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(H2_PREFACE).await.unwrap();
        stream.write_all(&H2_EMPTY_SETTINGS).await.unwrap();
        let mut header = [0u8; 9];
        stream.read_exact(&mut header).await.unwrap();
        assert_eq!(header[3], 0x4, "first server frame must be SETTINGS");
        stream
    }

    /// Reads (without ever writing) until the server closes the connection.
    async fn read_until_closed(stream: &mut TcpStream) {
        let mut buf = [0u8; 4096];
        loop {
            match stream.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(_) => {}
            }
        }
    }

    #[tokio::test]
    async fn drain_timeout_bounds_shutdown_when_a_client_never_answers() {
        // Control: an unbounded graceful shutdown waits on the frozen client indefinitely.
        let mut unbounded = spawn_server(0, 0, None).await;
        let _frozen = connect_frozen_h2_client(unbounded.addr).await;
        unbounded.shutdown_tx.send(()).unwrap();
        let waited = tokio::time::timeout(Duration::from_secs(3), &mut unbounded.handle).await;
        assert!(
            waited.is_err(),
            "unbounded graceful shutdown finished although a client never answered"
        );
        unbounded.handle.abort();

        let bounded = spawn_server(0, 0, Some(Duration::from_secs(1))).await;
        let _frozen = connect_frozen_h2_client(bounded.addr).await;
        let started = Instant::now();
        bounded.shutdown_tx.send(()).unwrap();
        let ret = tokio::time::timeout(Duration::from_secs(5), bounded.handle)
            .await
            .expect("bounded graceful shutdown did not return within 5s")
            .expect("server task panicked");
        ret.expect("serve returned an error");
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_millis(900) && elapsed < Duration::from_secs(3),
            "bounded shutdown took {elapsed:?}, expected about 1s"
        );
    }

    #[tokio::test]
    async fn keepalive_closes_idle_connection_of_a_client_that_never_answers() {
        // The drain bound is far away, so only keepalive can make shutdown fast here.
        let server = spawn_server(1, 1, Some(Duration::from_secs(30))).await;
        let mut frozen = connect_frozen_h2_client(server.addr).await;
        let connected = Instant::now();
        tokio::time::timeout(Duration::from_secs(10), read_until_closed(&mut frozen))
            .await
            .expect("server kept the connection of a client that never answered PINGs");
        let closed_after = connected.elapsed();
        assert!(
            closed_after >= Duration::from_millis(900),
            "connection closed after {closed_after:?}, before a keepalive round trip"
        );

        let started = Instant::now();
        server.shutdown_tx.send(()).unwrap();
        let ret = tokio::time::timeout(Duration::from_secs(5), server.handle)
            .await
            .expect("graceful shutdown did not return within 5s")
            .expect("server task panicked");
        ret.expect("serve returned an error");
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_secs(2),
            "graceful shutdown took {elapsed:?} although no connection was left"
        );
    }
}
