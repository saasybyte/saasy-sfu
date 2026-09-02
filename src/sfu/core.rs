use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use mediasoup::prelude::{
    Consumer,
    ConsumerId,
    ConsumerOptions,
    DtlsParameters,
    IceCandidate,
    IceParameters,
    MediaKind,
    Producer,
    ProducerId,
    ProducerOptions,
    RtpCapabilities,
    RtpCapabilitiesFinalized,
    RtpParameters,
    Transport,
    TransportId,
    WebRtcTransportOptions,
    WebRtcTransportRemoteParameters,
};
use mediasoup::router::{Router, RouterId};
use saasy_proto_rust::sfu::{
    sfu_event,
    SfuEvent,
    SubscriptionConfirmedEvent,
};
use saasy_proto_rust::shared::ParticipantType;
use tokio::sync::mpsc;
use tonic::Status;
use tracing::{error, info};
use uuid::Uuid;

use super::error::SessionError;
use super::event_handler::{EventHandler, PendingEventSetup};
use super::router_manager::RouterManager;
use super::transport::Transports;

/// Type alias for event channel senders
type EventSender = mpsc::Sender<Result<SfuEvent, Status>>;

/// Represents the state of a single participant in a session
#[allow(dead_code)]
struct ParticipantState {
    /// Type of participant (User or LLM)
    participant_type: ParticipantType,
    
    /// Client's RTP capabilities
    rtp_capabilities: Option<RtpCapabilities>,
    
    /// WebRTC transports (producer + consumer)
    transports: Transports,
    
    /// Media producers created by this participant
    producers: HashMap<ProducerId, Arc<Producer>>,
    
    /// Active media consumers for this participant
    consumers: HashMap<ConsumerId, Arc<Consumer>>,
}

/// Represents the state of an active SFU session
struct SessionState {    
    /// The Mediasoup Router used for this session
    router: Arc<Router>,
    
    /// All participants in this session (by `participant_id`)
    participants: HashMap<String, ParticipantState>,
    
    /// Maximum number of participants allowed (always 2 for 1-on-1)
    max_participants: usize,
}

/// The SFU server that manages all active signaling sessions
pub struct SfuCore {
    /// Router manager for Mediasoup
    router_manager: Arc<RouterManager>,
    
    /// Active signaling sessions by session ID
    sessions: HashMap<Uuid, SessionState>,

    /// Maps a router ID to an atomic session counter
    active_sessions_per_router: HashMap<RouterId, Arc<AtomicUsize>>,

    /// Controls how many sessions a router can serve
    max_sessions_per_router: usize,

    /// Stores event channel senders for each participant in each session
    event_senders: HashMap<Uuid, HashMap<String, EventSender>>,
}

#[allow(dead_code)]
pub struct RegisterSessionData {
    pub session_id: Uuid,
    pub participant_id: String,
}

pub struct CreateTransportData {
    pub transport_id: TransportId,
    pub ice_candidates: Vec<IceCandidate>,
    pub dtls_parameters: DtlsParameters,
    pub ice_parameters: IceParameters,
}

impl SfuCore{
    pub fn new(router_manager: Arc<RouterManager>, max_sessions_per_router: usize) -> Self {
        let mut active_sessions_per_router = HashMap::new();
        for router in router_manager.routers() {
            active_sessions_per_router.insert(router.id(), Arc::new(AtomicUsize::new(0)));
        }

        Self {
            router_manager,
            sessions: HashMap::new(),
            active_sessions_per_router,
            max_sessions_per_router,
            event_senders: HashMap::new(),
        }
    }

    pub async fn register_session(
        &mut self,
        participant_id: String,
        participant_type: ParticipantType,
    ) -> Result<RegisterSessionData, SessionError> {
        let session_id = Uuid::new_v4();
        info!("New session: {session_id} for participant: {participant_id}");
    
        let router = self.router_manager
            .get_available_router(&self.active_sessions_per_router, self.max_sessions_per_router)
            .await
            .ok_or(SessionError::AllRoutersFull)?;

        let register_session_data = RegisterSessionData {
            session_id,
            participant_id: participant_id.clone(),
        };

        {
            let counter = self.active_sessions_per_router
                .get(&router.id())
                .ok_or(SessionError::MissingRouterCounter)?;
            counter.fetch_add(1, Ordering::SeqCst);
            
            let mut participants = HashMap::new();
            participants.insert(participant_id, ParticipantState {
                participant_type,
                rtp_capabilities: None,
                transports: Transports::new(),
                producers: HashMap::new(),
                consumers: HashMap::new(),
            });

            self.sessions.insert(
                session_id,
                SessionState {
                    router,
                    participants,
                    max_participants: 2,
                },
            );
        }

        Ok(register_session_data)
    }

    pub fn join_session(
        &mut self,
        session_id: Uuid,
        participant_id: String,
        participant_type: ParticipantType,
    ) -> Result<(), SessionError> {
        info!("Participant {participant_id} joining session: {session_id}");
        
        let session_state = self.sessions
            .get_mut(&session_id)
            .ok_or(SessionError::SessionNotFound(session_id.to_string()))?;

        if session_state.participants.len() >= session_state.max_participants {
            return Err(SessionError::SessionFull(session_id.to_string()));
        }

        if session_state.participants.contains_key(&participant_id) {
            return Err(SessionError::ParticipantAlreadyExists(participant_id));
        }

        session_state.participants.insert(participant_id, ParticipantState {
            participant_type,
            rtp_capabilities: None,
            transports: Transports::new(),
            producers: HashMap::new(),
            consumers: HashMap::new(),
        });
        
        Ok(())
    }

    pub fn get_router_rtp_capabilities(
        &self,
        session_id: Uuid,
    ) -> Result<RtpCapabilitiesFinalized, SessionError> {
        let session_state = self.sessions
            .get(&session_id)
            .ok_or(SessionError::SessionNotFound(session_id.to_string()))?;
    
        Ok(session_state.router.rtp_capabilities().clone())
    }

    pub fn set_rtp_capabilities(
        &mut self,
        session_id: Uuid,
        participant_id: &str,
        rtp_capabilities: RtpCapabilities,
    ) -> Result<(), SessionError> {
        let session_state = self.sessions
            .get_mut(&session_id)
            .ok_or(SessionError::SessionNotFound(session_id.to_string()))?;
        
        let participant = session_state.participants
            .get_mut(participant_id)
            .ok_or(SessionError::ParticipantNotFound(participant_id.to_string()))?;
        
        participant.rtp_capabilities = Some(rtp_capabilities);
        Ok(())
    }

    pub async fn create_transport(
        &mut self,
        session_id: Uuid,
        participant_id: &str,
        direction: &str,
    ) -> Result<(CreateTransportData, Option<PendingEventSetup>), SessionError> {
        let session_state = self.sessions
            .get_mut(&session_id)
            .ok_or(SessionError::SessionNotFound(session_id.to_string()))?;

        let participant = session_state.participants
            .get_mut(participant_id)
            .ok_or(SessionError::ParticipantNotFound(participant_id.to_string()))?;

        let webrtc_server = self.router_manager
            .get_webrtc_server(&session_state.router.id())
            .ok_or(SessionError::MissingWebRtcServer)?;

        let transport_options = WebRtcTransportOptions::new_with_server(
            webrtc_server.as_ref().clone()
        );

        let transport = Arc::new(
            session_state.router
                .create_webrtc_transport(transport_options)
                .await
                .map_err(|e| SessionError::TransportCreationFailed(e.to_string()))?
        );

        match direction {
            "send" => participant.transports.send = Some(transport.clone()),
            "recv" => participant.transports.recv = Some(transport.clone()),
            _ => return Err(SessionError::InvalidTransportDirection(direction.to_string())),
        };

        let create_transport_data = CreateTransportData {
            transport_id: transport.id(),
            ice_candidates: transport.ice_candidates().clone(),
            dtls_parameters: transport.dtls_parameters(),
            ice_parameters: transport.ice_parameters().clone(),
        };

        let pending_setup = PendingEventSetup::Transport {
            transport,
            session_id,
            participant_id: participant_id.to_string(),
        };

        Ok((create_transport_data, Some(pending_setup)))
    }

    pub async fn connect_transport(
        &mut self,
        session_id: Uuid,
        participant_id: &str,
        transport_id: TransportId,
        dtls_parameters: DtlsParameters,
    ) -> Result<(), SessionError> {
        let session_state = self.sessions
            .get_mut(&session_id)
            .ok_or(SessionError::SessionNotFound(session_id.to_string()))?;
        
        let participant = session_state.participants
            .get_mut(participant_id)
            .ok_or(SessionError::ParticipantNotFound(participant_id.to_string()))?;

        let transport = match (&participant.transports.send, &participant.transports.recv) {
            (Some(send), _) if send.id() == transport_id => send,
            (_, Some(recv)) if recv.id() == transport_id => recv,
            _ => return Err(SessionError::InvalidTransportId(transport_id.to_string())),
        };

        transport
            .connect(WebRtcTransportRemoteParameters { dtls_parameters })
            .await
            .map_err(|e| SessionError::TransportConnectFailed(e.to_string()))
    }

    pub async fn create_producer(
        &mut self,
        session_id: Uuid,
        participant_id: &str,
        transport_id: TransportId,
        kind: MediaKind,
        rtp_parameters: RtpParameters,
    ) -> Result<(ProducerId, Option<PendingEventSetup>), SessionError> {
        let session_state = self.sessions
            .get_mut(&session_id)
            .ok_or(SessionError::SessionNotFound(session_id.to_string()))?;
        
        let participant = session_state.participants
            .get_mut(participant_id)
            .ok_or(SessionError::ParticipantNotFound(participant_id.to_string()))?;

        let send_transport = participant.transports.send.as_ref()
            .ok_or_else(|| SessionError::InvalidTransport("Send transport not created".to_string()))?;

        if transport_id != send_transport.id() {
            let msg = format!("Cannot create producer on non-producer transport id: {transport_id}");
            error!("{msg}");
            return Err(SessionError::InvalidTransport(msg));
        }

        let producer = Arc::new(send_transport
            .produce(ProducerOptions::new(kind, rtp_parameters))
            .await
            .map_err(|e| SessionError::ProducerCreationFailed(e.to_string()))?);

        let producer_id = producer.id();

        let pending_setup = PendingEventSetup::Producer {
            producer: producer.clone(),
            session_id,
            participant_id: participant_id.to_string(),
            kind,
        };

        participant.producers.insert(producer_id, producer);

        Ok((producer_id, Some(pending_setup)))
    }

    pub async fn create_consumer(
        &mut self,
        session_id: Uuid,
        participant_id: &str,
        transport_id: TransportId,
        producer_id: ProducerId,
        rtp_capabilities: RtpCapabilities,
    ) -> Result<(Arc<Consumer>, Option<PendingEventSetup>), SessionError> {
        let session_state = self.sessions
            .get_mut(&session_id)
            .ok_or(SessionError::SessionNotFound(session_id.to_string()))?;
        
        let participant = session_state.participants
            .get_mut(participant_id)
            .ok_or(SessionError::ParticipantNotFound(participant_id.to_string()))?;

        let recv_transport = participant.transports.recv.as_ref()
            .ok_or_else(|| SessionError::InvalidTransport("Recv transport not created".to_string()))?;

        if transport_id != recv_transport.id() {
            let msg = format!("Cannot create consumer on non-consumer transport: {transport_id}");
            error!("{msg}");
            return Err(SessionError::InvalidTransport(msg));
        }

        if !session_state.router.can_consume(&producer_id, &rtp_capabilities) {
            let msg = "Router cannot consume this producer with provided RTP capabilities".to_string();
            error!(msg);
            return Err(SessionError::RtpCapabilitiesMismatch(msg));
        }

        let mut consumer_options = ConsumerOptions::new(producer_id, rtp_capabilities);
        consumer_options.paused = true;

        let consumer = Arc::new(recv_transport
            .consume(consumer_options)
            .await
            .map_err(|e| SessionError::ConsumerCreationFailed(e.to_string()))?);

        let pending_setup = PendingEventSetup::Consumer {
            consumer: consumer.clone(),
            session_id,
            participant_id: participant_id.to_string(),
        };

        participant.consumers.insert(consumer.id(), Arc::clone(&consumer));

        Ok((consumer, Some(pending_setup)))
    }

    pub async fn resume_consumer(
        &mut self,
        session_id: Uuid,
        participant_id: &str,
        consumer_id: ConsumerId,
    ) -> Result<(), SessionError> {
        let session_state = self.sessions
            .get_mut(&session_id)
            .ok_or(SessionError::SessionNotFound(session_id.to_string()))?;
        
        let participant = session_state.participants
            .get_mut(participant_id)
            .ok_or(SessionError::ParticipantNotFound(participant_id.to_string()))?;

        let consumer = participant.consumers
            .get(&consumer_id)
            .ok_or_else(|| SessionError::ConsumerNotFound(consumer_id.to_string()))?;

        consumer
            .resume()
            .await
            .map_err(|e| SessionError::ConsumerResumeFailed(e.to_string()))?;

        Ok(())
    }

    pub fn close_session(&mut self, session_id: Uuid) {
        info!("Session closed: {session_id}");

        if let Some(session_state) = self.sessions.remove(&session_id) {
            let router_id = session_state.router.id();
            if let Some(counter) = self.active_sessions_per_router.get(&router_id) {
                counter.fetch_sub(1, Ordering::SeqCst);
            }

            self.event_senders.remove(&session_id);
        }
    }

    pub async fn subscribe_to_events(
        &mut self,
        session_id: Uuid,
        participant_id: String,
        sender: EventSender,
    ) -> Result<(), SessionError> {
        if !self.sessions.contains_key(&session_id) {
            return Err(SessionError::SessionNotFound(session_id.to_string()));
        }

        self.event_senders
            .entry(session_id)
            .or_default()
            .insert(participant_id.clone(), sender.clone());

        let confirmed_event = SfuEvent {
            event: Some(sfu_event::Event::SubscriptionConfirmed(
                SubscriptionConfirmedEvent {
                    session_id: session_id.to_string(),
                }
            )),
        };

        if let Err(e) = sender.send(Ok(confirmed_event)).await {
            error!("Failed to send subscription confirmed event: {e}");
            return Err(SessionError::EventChannelClosed);
        }

        // Send existing producers to the subscribing participant
        if let Ok(existing_producers) = self.get_existing_producers_for_session(session_id) {
            for (producer_id, kind, producer_participant_id) in existing_producers {
                // Don't send the participant their own producers
                if producer_participant_id != participant_id {
                    let event = EventHandler::create_new_producer_event(producer_id, kind);
                    if let Err(e) = sender.send(Ok(event)).await {
                        error!("Failed to send existing producer event: {e}");
                    }
                }
            }
        }

        Ok(())
    }

    pub fn broadcast_event(&self, session_id: Uuid, event: &SfuEvent, exclude_participant_id: Option<&str>) {
        if let Some(participant_senders) = self.event_senders.get(&session_id) {
            for (participant_id, sender) in participant_senders {
                // Skip if this is the participant to exclude
                if let Some(exclude_id) = exclude_participant_id {
                    if participant_id == exclude_id {
                        continue;
                    }
                }

                if let Err(e) = sender.try_send(Ok(event.clone())) {
                    match e {
                        mpsc::error::TrySendError::Full(_) => {
                            error!("Event channel full for participant {participant_id} in session {session_id}");
                        }
                        mpsc::error::TrySendError::Closed(_) => {
                            error!("Event channel closed for participant {participant_id} in session {session_id}");
                        }
                    }
                }
            }
        }
    }

    pub fn get_existing_producers_for_session(
        &self,
        session_id: Uuid,
    ) -> Result<Vec<(ProducerId, MediaKind, String)>, SessionError> {
        let session_state = self.sessions
            .get(&session_id)
            .ok_or(SessionError::SessionNotFound(session_id.to_string()))?;
        
        let mut producers = Vec::new();
        
        // Collect all producers from all participants in the session
        for (participant_id, participant_state) in &session_state.participants {
            for (producer_id, producer) in &participant_state.producers {
                producers.push((*producer_id, producer.kind(), participant_id.clone()));
            }
        }

        Ok(producers)
    }

    pub fn shutdown(&mut self) {
        let session_ids: Vec<Uuid> = self.sessions.keys().copied().collect();
        for session_id in session_ids {
            self.close_session(session_id);
        }
        
        // Add any additional cleanup here if needed
    }
}
