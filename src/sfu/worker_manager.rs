use std::net::IpAddr;
use std::sync::Arc;

use mediasoup::prelude:: {
    ListenInfo,
    Protocol,
    WebRtcServer,
    WebRtcServerListenInfos,
    WebRtcServerOptions,
    Worker,
    WorkerManager as MediasoupWorkerManager,
    WorkerSettings,
};
use mediasoup::worker::{WorkerLogLevel, WorkerLogTag};

use super::error::WorkerManagerError;

/// Manages a pool of Mediasoup Workers for the SFU
pub struct WorkerManager {
    /// Pool of Mediasoup `Worker` instances shared across the SFU
    workers: Vec<Arc<Worker>>,
    webrtc_servers: Vec<Arc<WebRtcServer>>,
}

impl WorkerManager {
    /// Create a new `WorkerManager` with `worker_count` Mediasoup Workers
    pub async fn new(
        worker_count: usize,
        log_level: &str,
        announced_ip: Option<String>,
        listen_ip_addr: IpAddr,
    ) -> Result<Self, WorkerManagerError> {
        let mut workers = Vec::with_capacity(worker_count);
        let mediasoup_manager = MediasoupWorkerManager::new();
        let level = parse_log_level(log_level);

        for _i in 0..worker_count {
            let mut settings = WorkerSettings::default();
            settings.log_level = level;

            if matches!(level, WorkerLogLevel::Debug) {
                settings.log_tags = default_debug_log_tags();
            }

            let worker = mediasoup_manager.create_worker(settings).await?;
            workers.push(Arc::new(worker));
        }

        let mut webrtc_servers = Vec::with_capacity(worker_count);
        for (i, worker) in workers.iter().enumerate() {
            let port_offset = u16::try_from(i)
                .map_err(|_| WorkerManagerError::TooManyWorkers(
                    format!("Worker index {i} exceeds u16 range")
                ))?;

            let listen_info = ListenInfo {
                protocol: Protocol::Udp,
                ip: listen_ip_addr,
                port: Some(10000 + port_offset), // TODO: make starting port an env var?
                announced_address: announced_ip.clone(),
                expose_internal_ip: false,  
                port_range: None,
                flags: None,
                send_buffer_size: None,
                recv_buffer_size: None,
            };
            
            let webrtc_server = worker
                .create_webrtc_server(
                    WebRtcServerOptions::new(
                        WebRtcServerListenInfos::new(listen_info)
                    )
                )
                .await?;
            webrtc_servers.push(Arc::new(webrtc_server));
        }

        Ok(Self {
            workers,
            webrtc_servers,
        })
    }

    pub fn webrtc_servers(&self) -> &[Arc<WebRtcServer>] {
        &self.webrtc_servers
    }

    /// Returns a reference to the internal list of workers
    pub fn workers(&self) -> &[Arc<Worker>] {
        &self.workers
    }
}

/// Parses a string log level into a `WorkerLogLevel`
fn parse_log_level(value: &str) -> WorkerLogLevel {
    match value.to_lowercase().as_str() {
        "debug" => WorkerLogLevel::Debug,
        "error" => WorkerLogLevel::Error,
        _ => WorkerLogLevel::Warn, // fallback
    }
}

/// Returns a predefined set of Mediasoup `WorkerLogTag`s for debugging
fn default_debug_log_tags() -> Vec<WorkerLogTag> {
    vec![
        WorkerLogTag::Info,
        WorkerLogTag::Ice,
        WorkerLogTag::Dtls,
        WorkerLogTag::Rtp,
        WorkerLogTag::Srtp,
        WorkerLogTag::Rtcp,
        WorkerLogTag::Rtx,
        WorkerLogTag::Bwe,
        WorkerLogTag::Score,
        WorkerLogTag::Sctp,
        WorkerLogTag::Message,
    ]
}
