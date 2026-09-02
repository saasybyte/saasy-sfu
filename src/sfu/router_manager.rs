use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use mediasoup::prelude::{Router, RouterOptions, WebRtcServer, Worker};
use mediasoup::router::RouterId;
use tokio::sync::Mutex;

use super::codecs::get_codecs;
use super::error::RouterManagerError;

/// Manages a pool of Mediasoup Routers, one for each worker
/// 
/// We might want to expand to multiple routers per worker later on
pub struct RouterManager {
    /// Pool of Mediasoup `Router` instances shared across the SFU
    routers: Vec<Arc<Router>>,

    router_to_webrtc_server: HashMap<RouterId, Arc<WebRtcServer>>,

    /// Index for round-robin selection of the next router
    next_index: Mutex<usize>,
}

impl RouterManager {
    /// Creates a new `RouterManager` with one `Router` per provided `Worker`
    pub async fn new(
        workers: &[Arc<Worker>],
        webrtc_servers: &[Arc<WebRtcServer>]
    ) -> Result<Self, RouterManagerError> {
        let mut routers = Vec::with_capacity(workers.len());
        let mut router_to_webrtc_server = HashMap::new();

        for (worker, webrtc_server) in workers.iter().zip(webrtc_servers.iter()) {
            let codecs = get_codecs()?;
            let router = Arc::new(worker.create_router(RouterOptions::new(codecs)).await?);
            router_to_webrtc_server.insert(router.id(), webrtc_server.clone());
            routers.push(router);
        }

        Ok(Self {
            routers,
            router_to_webrtc_server,
            next_index: Mutex::new(0),
        })
    }

    pub fn routers(&self) -> &[Arc<Router>] {
        &self.routers
    }

    pub fn get_webrtc_server(&self, router_id: &RouterId) -> Option<Arc<WebRtcServer>> {
        self.router_to_webrtc_server.get(router_id).cloned()
    }

    /// Returns the next available Mediasoup `Router` in round-robin fashion
    pub async fn get_next_router(&self) -> Result<Arc<Router>, RouterManagerError> {
        if self.routers.is_empty() {
            return Err(RouterManagerError::EmptyPool);
        }

        let index = {
            let mut guard = self.next_index.lock().await;
            let idx = *guard;
            *guard = (idx + 1) % self.routers.len(); // update before releasing lock
            idx
        };

        let router = self.routers[index].clone();
        Ok(router)
    }

    pub async fn get_available_router(
        &self,
        router_session_counts: &HashMap<RouterId, Arc<AtomicUsize>>,
        max_sessions_per_router: usize,
    ) -> Option<Arc<Router>> {
        let total = self.routers.len();
        if total == 0 {
            return None;
        }

        for _ in 0..total {
            let router = self.get_next_router().await.ok()?;
            let router_id = router.id();

            if let Some(counter) = router_session_counts.get(&router_id) {
                let count = counter.load(Ordering::SeqCst);
                if count < max_sessions_per_router {
                    return Some(router);
                }
            }
        }

        None
    }
}
