use std::sync::Arc;

use mediasoup::prelude::WebRtcTransport;

/// WebRTC transports associated with a single participant
pub struct Transports {
    /// Transport for sending media from client to SFU (carries producers)
    pub send: Option<Arc<WebRtcTransport>>,

    /// Transport for receiving media from SFU to client (carries consumers)
    pub recv: Option<Arc<WebRtcTransport>>,
}

impl Transports {
    pub fn new() -> Self {
        Self {
            send: None,
            recv: None,
        }
    }
}
