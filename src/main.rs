mod config;
mod grpc;
mod http;
mod sfu;

use std::io;
use std::sync::Arc;
use std::time::Duration;

use actix_web::{App, HttpServer};
use saasy_proto_rust::sfu::SfuServiceServer;
use tokio::signal::ctrl_c;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tonic::transport::Server;
use tracing::{error, info};

use crate::config::ServerConfig;
use crate::grpc::SfuHandler;
use crate::sfu::{RouterManager, SfuCore, WorkerManager};

/// Entry point for the Saasy SFU server
///
/// Initializes configuration, sets up logging,
/// initializes Mediasoup workers for real-time media processing,
/// spawns the SFU server as a background Tokio task,
/// and starts the gRPC server for signaling service communication
#[tokio::main]
async fn main() -> io::Result<()> {
    // Load server configuration from .env, default.toml, and environment
    let config = ServerConfig::from_env()
        .map_err(|e| io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("Failed to load config: {e}")
        ))?;

    // Initialize structured logging with optional RUST_LOG override
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let listen_ip_addr = config.parsed_listen_ip_addr()
        .map_err(|e| io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("Invalid listen ip address: {e}")
        ))?;

    // Create shared WorkerManager for handling Mediasoup workers
    let mediasoup_workers = config.mediasoup_workers;
    info!("Configured to use {mediasoup_workers} mediasoup workers");
    let worker_manager = Arc::new(
        WorkerManager::new(
            mediasoup_workers,
            &config.mediasoup_log,
            config.announced_ip_addr.clone(),
            listen_ip_addr,
        )
        .await
        .map_err(|e| io::Error::new(
            io::ErrorKind::Other,
            format!("Failed to initialize Mediasoup workers: {e}")
        ))?
    );

    // Create shared RouterManager using available workers
    let router_manager = Arc::new(
        RouterManager::new(worker_manager.workers(), worker_manager.webrtc_servers())
            .await
            .map_err(|e| io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to initialize RouterManager: {e}")
            ))?
        );
    
    // Spawn health check HTTP server
    let http_host = config.http_host.clone();
    let http_port = config.http_port;
    info!("Starting health server on {}:{}", http_host, http_port);

    let health_server = HttpServer::new(|| {
        App::new()
            .service(http::health::liveness)
            .service(http::health::readiness)
    })
    .bind((http_host, http_port))
    .map_err(|e| io::Error::new(
        io::ErrorKind::AddrInUse,
        format!("Failed to bind health server: {e}")
    ))?;

    tokio::spawn(health_server.run());

    // Create SFU server
    let sfu_core = Arc::new(tokio::sync::Mutex::new(SfuCore::new(
        router_manager,
        config.max_sessions_per_router,
    )));

    // Spawn a shutdown signal handler
    let shutdown_handler = Arc::clone(&sfu_core);
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        if let Err(e) = ctrl_c().await {
            error!("Failed to listen for shutdown signal: {e}");
            return;
        }

        info!("Starting SFU shutdown...");

        let shutdown_future = async {
            let mut guard = shutdown_handler.lock().await;
            guard.shutdown();
        };
        
        if timeout(Duration::from_secs(5), shutdown_future).await.is_ok() {
            info!("SFU shutdown completed successfully");
        } else {
            error!("SFU shutdown timed out after 5 seconds");
        }

        let _ = shutdown_tx.send(());
    });

    // Create gRPC service implementation
    let sfu_handler = SfuHandler::new(sfu_core, config.subscribe_channel_capacity);

    // Start gRPC server
    let grpc_bind_address = config.grpc_socket_addr()
        .map_err(|e| io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("Invalid gRPC bind address: {e}")
        ))?;
    info!("Starting gRPC server on {}", grpc_bind_address);
    let grpc_concurrency_limit = config.grpc_concurrency_limit.unwrap_or(128);
    Server::builder()
        .concurrency_limit_per_connection(grpc_concurrency_limit)
        .tcp_nodelay(true)
        .add_service(SfuServiceServer::new(sfu_handler))
        .serve_with_shutdown(grpc_bind_address, async {
            let _ = shutdown_rx.await;
        })
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("gRPC server error: {e}")))?;

    info!("SFU server shutdown complete");

    Ok(())
}
