//! Operational HTTP server on port 7084.
//!
//! Serves operational endpoints directly (not through proto transcoding):
//! - GET /health — liveness check (process alive)
//! - GET /ready — readiness check (storage open, can serve)
//! - GET /metrics — Prometheus OpenMetrics
//!
//! # Cluster-ready notes
//! - Each CE node (3-node HA) has its own :7084.
//! - K8s probes: liveness → /health, readiness → /ready.
//! - Metrics are per-node (Prometheus scrapes each node independently).

use std::net::SocketAddr;

use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tracing::{error, info};

/// Start the operational HTTP server.
///
/// Handles /health, /ready, /metrics on the given address.
/// Runs until the process exits.
pub async fn start_ops_server(
    addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Install Prometheus metrics recorder
    let prometheus_handle = PrometheusBuilder::new()
        .install_recorder()
        .map_err(|e| format!("failed to install Prometheus recorder: {e}"))?;

    // Register all CE metric families
    crate::metrics_catalog::register_all_metrics();

    info!(port = addr.port(), "operational HTTP server listening");

    let listener = TcpListener::bind(addr).await?;

    loop {
        let (mut stream, _peer) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                error!("accept error: {e}");
                continue;
            }
        };

        let handle = prometheus_handle.clone();

        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            let n = match tokio::io::AsyncReadExt::read(&mut stream, &mut buf).await {
                Ok(n) => n,
                Err(_) => return,
            };

            let request = String::from_utf8_lossy(&buf[..n]);

            // Parse the HTTP request line
            let path = request
                .lines()
                .next()
                .and_then(|line| line.split_whitespace().nth(1))
                .unwrap_or("/");

            let (status, content_type, body) = match path {
                "/health" => (
                    "200 OK",
                    "application/json",
                    r#"{"status":"ok"}"#.to_string(),
                ),
                "/ready" => (
                    "200 OK",
                    "application/json",
                    r#"{"ready":true}"#.to_string(),
                ),
                "/metrics" => {
                    let metrics_output = handle.render();
                    ("200 OK", "text/plain; charset=utf-8", metrics_output)
                }
                _ => (
                    "404 Not Found",
                    "application/json",
                    r#"{"error":"not found"}"#.to_string(),
                ),
            };

            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );

            let _ = stream.write_all(response.as_bytes()).await;
        });
    }
}
