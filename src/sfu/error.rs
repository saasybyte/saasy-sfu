use std::io;

use mediasoup::worker::{CreateRouterError, CreateWebRtcServerError, RequestError};

#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error("Router error: {0}")]
    RouterManager(#[from] RouterManagerError),

    #[error("Missing Webrtc server")]
    MissingWebRtcServer,

    #[error("Transport error: {0}")]
    Transport(#[from] TransportError),

    #[error("Session not found: {0}")]
    SessionNotFound(String),

    #[error("Failed to create transport: {0}")]
    TransportCreationFailed(String),

    #[error("Failed to connect transport: {0}")]
    TransportConnectFailed(String),

    #[error("Invalid transport ID: {0}")]
    InvalidTransportId(String),

    #[error("Invalid transport direction: {0}")]
    InvalidTransportDirection(String),

    #[error("Invalid transport: {0}")]
    InvalidTransport(String),

    #[error("Failed to create producer: {0}")]
    ProducerCreationFailed(String),

    #[error("RTP capabilities mismatch: {0}")]
    RtpCapabilitiesMismatch(String),

    #[error("Failed to create consumer: {0}")]
    ConsumerCreationFailed(String),

    #[error("Consumer not found: {0}")]
    ConsumerNotFound(String),

    #[error("Failed to resume consumer: {0}")]
    ConsumerResumeFailed(String),

    #[error("Event channel is closed or receiver dropped")]
    EventChannelClosed,

    #[error("No counter found for router")]
    MissingRouterCounter,

    #[error("All routers are at max session capacity")]
    AllRoutersFull,

    #[error("Session is full: {0}")]
    SessionFull(String),
    
    #[error("Participant not found: {0}")]
    ParticipantNotFound(String),
    
    #[error("Participant already exists: {0}")]
    ParticipantAlreadyExists(String),
}

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("Mediasoup transport creaton error: {0}")]
    Request(#[from] RequestError),
}

#[derive(Debug, thiserror::Error)]
pub enum WorkerManagerError {
    #[error("Mediasoup worker creation error: {0}")]
    Io(#[from] io::Error),

    #[error("Too many workers: {0}")]
    TooManyWorkers(String),

    #[error("WebRTC server creation error: {0}")]
    WebRtcServerCreation(#[from] CreateWebRtcServerError),
}

#[derive(Debug, thiserror::Error)]
pub enum RouterManagerError {
    #[error("Router pool is empty")]
    EmptyPool,

    #[error("Codec init error: {0}")]
    Codec(#[from] CodecError),

    #[error("Mediasoup router creation error: {0}")]
    Mediasoup(#[from] CreateRouterError),
}

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("Invalid clock rate")]
    InvalidClockRate,

    #[error("Invalid channel count")]
    InvalidChannels,
}
