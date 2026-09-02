use std::net::{AddrParseError, IpAddr, SocketAddr};

use config::{Config, ConfigError, File, Environment};
use serde::Deserialize;

/// Represents the server configuration for the SFU service
#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    pub http_host: String,

    pub http_port: u16,

    /// The host address to bind the gRPC server to
    pub grpc_host: String,

    /// The port number for the gRPC server
    pub grpc_port: u16,

    /// Maximum number of concurrent in-flight gRPC requests per connection
    pub grpc_concurrency_limit: Option<usize>,

    /// Number of worker threads to spawn for Mediasoup
    pub mediasoup_workers: usize,

    /// Log level for mediasoup workers ("debug", "info", "warn", "error")
    pub mediasoup_log: String,

    /// IP address that WebRTC transports should listen on
    pub listen_ip_addr: String,

    /// Publicly announced IP for ICE candidates (optional)
    pub announced_ip_addr: Option<String>,

    /// Maximum number of sessions allowed per router
    pub max_sessions_per_router: usize,

    /// The capacity of the mpsc channel used for streaming server-sent events
    pub subscribe_channel_capacity: usize,
}

impl ServerConfig {
    /// Initializes the config from `.env` and environment variables
    pub fn from_env() -> Result<Self, ConfigError> {
        dotenvy::dotenv().ok();

        Config::builder()
            .add_source(File::with_name("config/default").required(false))
            .add_source(Environment::default())
            .build()?
            .try_deserialize()
    }

    /// Returns the gRPC socket address string to bind to
    pub fn grpc_socket_addr(&self) -> Result<SocketAddr, AddrParseError> {
        format!("{}:{}", self.grpc_host, self.grpc_port).parse()
    }

    /// Parses the SFU listen IP or returns an address parsing error
    pub fn parsed_listen_ip_addr(&self) -> Result<IpAddr, AddrParseError> {
        self.listen_ip_addr.parse()
    }
}
