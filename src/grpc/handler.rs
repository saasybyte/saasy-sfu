use std::convert::Into;
use std::sync::Arc;

use saasy_proto_rust::sfu::{
    sfu_request_envelope,
    sfu_response_envelope,
    SfuEvent,
    SfuRequestEnvelope,
    SfuResponseEnvelope,
    SfuService,
};
use saasy_proto_rust::shared::{
    CloseSessionResponse,
    ConnectTransportResponse,
    ConsumerInfo,
    CreateConsumerResponse,
    CreateProducerResponse,
    CreateTransportResponse,
    GetRouterRtpCapabilitiesResponse,
    IceCandidate,
    JoinSessionResponse,
    MediaKind,
    ParticipantType,
    ProducerId,
    RegisterSessionResponse,
    ResumeConsumerResponse,
    SessionId,
    SetRtpCapabilitiesResponse,
    TransportDirection,
};
use tonic::{Request, Response, Status};
use tokio_stream::wrappers::ReceiverStream;
use tokio::sync::{mpsc, Mutex};
use uuid::Uuid;

use crate::sfu::{EventHandler, PendingEventSetup, SfuCore};

pub struct SfuHandler {
    sfu_core: Arc<Mutex<SfuCore>>,
    subscribe_channel_capacity: usize,
}

impl SfuHandler {
    pub fn new(sfu_core: Arc<Mutex<SfuCore>>, subscribe_channel_capacity: usize) -> Self {
        Self {
            sfu_core,
            subscribe_channel_capacity
        }
    }
}

impl SfuHandler {
    fn setup_event_handlers(&self, pending_setups: Vec<PendingEventSetup>) {
        let sfu_core = Arc::clone(&self.sfu_core);
        
        for setup in pending_setups {
            match setup {
                PendingEventSetup::Transport { transport, session_id, participant_id: _ } => {
                    let sfu_core = Arc::clone(&sfu_core);
                    EventHandler::setup_transport_events(
                        &transport,
                        session_id,
                        move |session_id, event| {
                            let sfu_core = Arc::clone(&sfu_core);
                            tokio::spawn(async move {
                                // Don't exclude anyone for transport events
                                sfu_core.lock().await.broadcast_event(session_id, &event, None);
                            });
                        },
                    );
                }
                PendingEventSetup::Producer { producer, session_id, participant_id, kind } => {
                    let sfu_core = Arc::clone(&sfu_core);
                    let producer_participant_id = participant_id.clone();
                    EventHandler::setup_producer_events(
                        &producer,
                        session_id,
                        move |session_id, event| {
                            let sfu_core = Arc::clone(&sfu_core);
                            tokio::spawn(async move {
                                // Don't exclude anyone for close events
                                sfu_core.lock().await.broadcast_event(session_id, &event, None);
                            });
                        },
                    );

                    // Broadcast new producer to OTHER participants (exclude the producer)
                    let sfu_core = Arc::clone(&self.sfu_core);
                    tokio::spawn(async move {
                        let event = EventHandler::create_new_producer_event(producer.id(), kind);
                        sfu_core.lock().await.broadcast_event(session_id, &event, Some(&producer_participant_id));
                    });
                }
                PendingEventSetup::Consumer { consumer, session_id, participant_id: _ } => {
                    let sfu_core = Arc::clone(&sfu_core);
                    EventHandler::setup_consumer_events(
                        &consumer,
                        session_id,
                        move |session_id, event| {
                            let sfu_core = Arc::clone(&sfu_core);
                            tokio::spawn(async move {
                                // Don't exclude anyone for consumer events
                                sfu_core.lock().await.broadcast_event(session_id, &event, None);
                            });
                        },
                    );
                }
            }
        }
    }
}

#[tonic::async_trait]
impl SfuService for SfuHandler {
    type SubscribeToEventsStream = ReceiverStream<Result<SfuEvent, Status>>;

    async fn register_session(
        &self,
        request: Request<SfuRequestEnvelope>
    ) -> Result<Response<SfuResponseEnvelope>, Status> {
        let request_envelope = request.into_inner();

        let participant_id = request_envelope.participant_id.clone();
        if participant_id.is_empty() {
            return Err(Status::invalid_argument("Participant ID is required"));
        }

        let participant_type = ParticipantType::User;
        
        let data = self.sfu_core
            .lock()
            .await
            .register_session(participant_id.clone(), participant_type)
            .await
            .map_err(|e| Status::internal(format!("Failed to register session: {e}")))?;

        let response_data = RegisterSessionResponse {
            session_id: Some(SessionId { id: data.session_id.to_string() }),
            ice_servers: vec![],
        };

        let response_envelope = SfuResponseEnvelope {
            r#type: "register_session".to_string(),
            session_id: data.session_id.to_string(),
            participant_id: request_envelope.participant_id,
            data: Some(sfu_response_envelope::Data::RegisterSessionResponse(response_data)),
        };

        Ok(Response::new(response_envelope))
    }

    async fn join_session(
        &self,
        request: Request<SfuRequestEnvelope>
    ) -> Result<Response<SfuResponseEnvelope>, Status> {
        let request_envelope = request.into_inner();

        let Some(sfu_request_envelope::Data::JoinSessionRequest(request_data)) = request_envelope.data else {
            return Err(Status::invalid_argument("Invalid request type"));
        };

        let session_id = request_data.session_id
            .ok_or_else(|| Status::invalid_argument("Session id is required"))?
            .id
            .parse::<Uuid>()
            .map_err(|e| Status::invalid_argument(format!("Invalid session id format: {e}")))?;

        let participant_id = request_data.participant_id
            .ok_or_else(|| Status::invalid_argument("Participant id is required"))?
            .id;

        let participant_type = ParticipantType::try_from(request_data.participant_type)
            .map_err(|_| Status::invalid_argument("Invalid participant type"))?;

        self.sfu_core
            .lock()
            .await
            .join_session(session_id, participant_id, participant_type)
            .map_err(|e| Status::internal(format!("Failed to join session: {e}")))?;

        let response_data = JoinSessionResponse {};

        let response_envelope = SfuResponseEnvelope {
            r#type: "join_session".to_string(),
            session_id: request_envelope.session_id,
            participant_id: request_envelope.participant_id,
            data: Some(sfu_response_envelope::Data::JoinSessionResponse(response_data)),
        };

        Ok(Response::new(response_envelope))
    }
    
    async fn get_router_rtp_capabilities(
        &self,
        request: Request<SfuRequestEnvelope>
    ) -> Result<Response<SfuResponseEnvelope>, Status> {
        let request_envelope = request.into_inner();

        let Some(sfu_request_envelope::Data::GetRouterRtpCapabilitiesRequest(request_data)) = request_envelope.data else {
            return Err(Status::invalid_argument("Invalid request type"));
        };

        let session_id = request_data.session_id
            .ok_or_else(|| Status::invalid_argument("Session id is required"))?
            .id
            .parse::<Uuid>()
            .map_err(|e| Status::invalid_argument(format!("Invalid session id format: {e}")))?;

        let data = self.sfu_core
            .lock()
            .await
            .get_router_rtp_capabilities(session_id)
            .map_err(|e| Status::internal(format!("Failed to get RTP capabilities: {e}")))?;

        let response_data = GetRouterRtpCapabilitiesResponse {
            rtp_capabilities: Some(data.try_into()
                .map_err(|e| Status::internal(format!("Failed to convert RTP capabilities: {e}")))?),
        };

        let response_envelope = SfuResponseEnvelope {
            r#type: "get_router_rtp_capabilities".to_string(),
            session_id: request_envelope.session_id,
            participant_id: request_envelope.participant_id,
            data: Some(sfu_response_envelope::Data::GetRouterRtpCapabilitiesResponse(response_data)),
        };

        Ok(Response::new(response_envelope))
    }

    async fn set_rtp_capabilities(
        &self,
        request: Request<SfuRequestEnvelope>
    ) -> Result<Response<SfuResponseEnvelope>, Status> {
        let request_envelope = request.into_inner();

        let Some(sfu_request_envelope::Data::SetRtpCapabilitiesRequest(request_data)) = request_envelope.data else {
            return Err(Status::invalid_argument("Invalid request type"));
        };

        let session_id = request_data.session_id
            .ok_or_else(|| Status::invalid_argument("Session id is required"))?
            .id
            .parse::<Uuid>()
            .map_err(|e| Status::invalid_argument(format!("Invalid session id format: {e}")))?;

        let rtp_capabilities = request_data.rtp_capabilities
            .ok_or_else(|| Status::invalid_argument("RTP capabilities are required"))?
            .try_into()
            .map_err(|e| Status::internal(format!("Failed to convert RTP capabilities: {e}")))?;
        
        self.sfu_core
            .lock()
            .await
            .set_rtp_capabilities(session_id, &request_envelope.participant_id, rtp_capabilities)
            .map_err(|e| Status::internal(format!("Failed to set RTP capabilities: {e}")))?;

        let response_data = SetRtpCapabilitiesResponse {};

        let response_envelope = SfuResponseEnvelope {
            r#type: "set_rtp_capabilities".to_string(),
            session_id: request_envelope.session_id,
            participant_id: request_envelope.participant_id,
            data: Some(sfu_response_envelope::Data::SetRtpCapabilitiesResponse(response_data)),
        };
        
        Ok(Response::new(response_envelope))
    }

    async fn create_transport(
        &self,
        request: Request<SfuRequestEnvelope>
    ) -> Result<Response<SfuResponseEnvelope>, Status> {
        let request_envelope = request.into_inner();

        let Some(sfu_request_envelope::Data::CreateTransportRequest(request_data)) = request_envelope.data else {
            return Err(Status::invalid_argument("Invalid request type"));
        };
        
        let session_id = request_data.session_id
            .ok_or_else(|| Status::invalid_argument("Session id is required"))?
            .id
            .parse::<Uuid>()
            .map_err(|e| Status::invalid_argument(format!("Invalid session id format: {e}")))?;
        
        let proto_direction = TransportDirection::try_from(request_data.direction)
            .map_err(|_| Status::invalid_argument("Invalid transport direction"))?;
            
        let direction = match proto_direction {
            TransportDirection::Send => "send",
            TransportDirection::Recv => "recv",
            TransportDirection::Unspecified => {
                return Err(Status::invalid_argument("Transport direction cannot be unspecified"));
            }
        };
        
        let (data, pending_setup) = self.sfu_core
            .lock()
            .await
            .create_transport(session_id, &request_envelope.participant_id, direction)
            .await
            .map_err(|e| Status::internal(format!("Failed to create transport: {e}")))?;

        if let Some(setup) = pending_setup {
            self.setup_event_handlers(vec![setup]);
        }

        let ice_candidates = data.ice_candidates
            .into_iter()
            .map(IceCandidate::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| Status::internal("Failed to convert ICE candidates"))?;
        
        let response_data = CreateTransportResponse {
            ice_candidates,
            transport_id: Some(data.transport_id.into()),
            dtls_parameters: Some(data.dtls_parameters.try_into()
                .map_err(|_| Status::internal("Failed to convert DTLS parameters"))?),
            ice_parameters: Some(data.ice_parameters.into()),
        };

        let response_envelope = SfuResponseEnvelope {
            r#type: "create_transport".to_string(),
            session_id: request_envelope.session_id,
            participant_id: request_envelope.participant_id,
            data: Some(sfu_response_envelope::Data::CreateTransportResponse(response_data)),
        };
        
        Ok(Response::new(response_envelope))
    }

    async fn connect_transport(
        &self,
        request: Request<SfuRequestEnvelope>
    ) -> Result<Response<SfuResponseEnvelope>, Status> {
        let request_envelope = request.into_inner();

        let Some(sfu_request_envelope::Data::ConnectTransportRequest(request_data)) = request_envelope.data else {
            return Err(Status::invalid_argument("Invalid request type"));
        };

        let session_id = request_data.session_id
            .ok_or_else(|| Status::invalid_argument("Session id is required"))?
            .id
            .parse::<Uuid>()
            .map_err(|e| Status::invalid_argument(format!("Invalid session id format: {e}")))?;

        let transport_id = request_data.transport_id
            .ok_or_else(|| Status::invalid_argument("Transport id is required"))?
            .id
            .parse()
            .map_err(|e| Status::invalid_argument(format!("Invalid transport id format: {e}")))?;

        let dtls_parameters = request_data.dtls_parameters
            .ok_or_else(|| Status::invalid_argument("DTLS parameters are required"))?
            .try_into()
            .map_err(|e| Status::internal(format!("Failed to convert DTLS parameters: {e}")))?;
        
        self.sfu_core
            .lock()
            .await
            .connect_transport(session_id, &request_envelope.participant_id, transport_id, dtls_parameters)
            .await
            .map_err(|e| Status::internal(format!("Failed to connect transport: {e}")))?;        

        let response_data = ConnectTransportResponse {};

        let response_envelope = SfuResponseEnvelope {
            r#type: "connect_transport".to_string(),
            session_id: request_envelope.session_id,
            participant_id: request_envelope.participant_id,
            data: Some(sfu_response_envelope::Data::ConnectTransportResponse(response_data)),
        };

        Ok(Response::new(response_envelope))
    }
    
    async fn create_producer(
        &self,
        request: Request<SfuRequestEnvelope>
    ) -> Result<Response<SfuResponseEnvelope>, Status> {
        let request_envelope = request.into_inner();

        let Some(sfu_request_envelope::Data::CreateProducerRequest(request_data)) = request_envelope.data else {
            return Err(Status::invalid_argument("Invalid request type"));
        };

        let session_id = request_data.session_id
            .ok_or_else(|| Status::invalid_argument("Session id is required"))?
            .id
            .parse::<Uuid>()
            .map_err(|e| Status::invalid_argument(format!("Invalid session id format: {e}")))?;

        let transport_id = request_data.transport_id
            .ok_or_else(|| Status::invalid_argument("Transport id is required"))?
            .id
            .parse()
            .map_err(|e| Status::invalid_argument(format!("Invalid transport id format: {e}")))?;

        let proto_kind = MediaKind::try_from(request_data.kind)
            .map_err(|_| Status::invalid_argument("Invalid media kind"))?;

        let kind = proto_kind.try_into()
            .map_err(|_| Status::invalid_argument("Failed to convert media kind"))?;

        let rtp_parameters = request_data.rtp_parameters
            .ok_or_else(|| Status::invalid_argument("RTP parameters are required"))?
            .try_into()
            .map_err(|e| Status::internal(format!("Failed to convert RTP parameters: {e}")))?;
        
        let (producer_id, pending_setup) = self.sfu_core
            .lock()
            .await
            .create_producer(session_id, &request_envelope.participant_id, transport_id, kind, rtp_parameters)
            .await
            .map_err(|e| Status::internal(format!("Failed to create producer: {e}")))?;

        if let Some(setup) = pending_setup {
            self.setup_event_handlers(vec![setup]);
        }

        let response_data = CreateProducerResponse {
            producer_id: Some(ProducerId { id: producer_id.to_string() }),
        };

        let response_envelope = SfuResponseEnvelope {
            r#type: "create_producer".to_string(),
            session_id: request_envelope.session_id,
            participant_id: request_envelope.participant_id,
            data: Some(sfu_response_envelope::Data::CreateProducerResponse(response_data)),
        };

        Ok(Response::new(response_envelope))
    }
    
    async fn create_consumer(
        &self,
        request: Request<SfuRequestEnvelope>
    ) -> Result<Response<SfuResponseEnvelope>, Status> {
        let request_envelope = request.into_inner();

        let Some(sfu_request_envelope::Data::CreateConsumerRequest(request_data)) = request_envelope.data else {
            return Err(Status::invalid_argument("Invalid request type"));
        };

        let session_id = request_data.session_id
            .ok_or_else(|| Status::invalid_argument("Session id is required"))?
            .id
            .parse::<Uuid>()
            .map_err(|e| Status::invalid_argument(format!("Invalid session id format: {e}")))?;
        
        let transport_id = request_data.transport_id
            .ok_or_else(|| Status::invalid_argument("Transport id is required"))?
            .id
            .parse()
            .map_err(|e| Status::invalid_argument(format!("Invalid transport id format: {e}")))?;
        
        let producer_id = request_data.producer_id
            .ok_or_else(|| Status::invalid_argument("Producer id is required"))?
            .id
            .parse()
            .map_err(|e| Status::invalid_argument(format!("Invalid producer id format: {e}")))?;
        
        let rtp_capabilities = request_data.rtp_capabilities
            .ok_or_else(|| Status::invalid_argument("RTP capabilities are required"))?
            .try_into()
            .map_err(|e| Status::internal(format!("Failed to convert RTP capabilities: {e}")))?;
        
        let (data, pending_setup) = self.sfu_core
            .lock()
            .await
            .create_consumer(session_id, &request_envelope.participant_id, transport_id, producer_id, rtp_capabilities)
            .await
            .map_err(|e| Status::internal(format!("Failed to create consumer: {e}")))?;

        if let Some(setup) = pending_setup {
            self.setup_event_handlers(vec![setup]);
        }

        let consumer_info = ConsumerInfo {
            id: data.id().to_string(),
            producer_id: data.producer_id().to_string(),
            kind: MediaKind::from(data.kind()) as i32,
            rtp_parameters: Some(data.rtp_parameters().clone().try_into()
                .map_err(|e| Status::internal(format!("Failed to convert RTP parameters: {e}")))?),
        };

        let response_data = CreateConsumerResponse {
            consumer_info: Some(consumer_info),
        };

        let response_envelope = SfuResponseEnvelope {
            r#type: "create_consumer".to_string(),
            session_id: request_envelope.session_id,
            participant_id: request_envelope.participant_id,
            data: Some(sfu_response_envelope::Data::CreateConsumerResponse(response_data)),
        };
        
        Ok(Response::new(response_envelope))
    }

    async fn resume_consumer(
        &self,
        request: Request<SfuRequestEnvelope>
    ) -> Result<Response<SfuResponseEnvelope>, Status> {
        let request_envelope = request.into_inner();

        let Some(sfu_request_envelope::Data::ResumeConsumerRequest(request_data)) = request_envelope.data else {
            return Err(Status::invalid_argument("Invalid request type"));
        };

        let session_id = request_data.session_id
            .ok_or_else(|| Status::invalid_argument("Session id is required"))?
            .id
            .parse::<Uuid>()
            .map_err(|e| Status::invalid_argument(format!("Invalid session id format: {e}")))?;

        let consumer_id = request_data.consumer_id
            .ok_or_else(|| Status::invalid_argument("Consumer id is required"))?
            .id
            .parse()
            .map_err(|e| Status::invalid_argument(format!("Invalid consumer id format: {e}")))?;

        self.sfu_core
            .lock()
            .await
            .resume_consumer(session_id, &request_envelope.participant_id, consumer_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to resume consumer: {e}")))?;

        let response_data = ResumeConsumerResponse {};

        let response_envelope = SfuResponseEnvelope {
            r#type: "resume_consumer".to_string(),
            session_id: request_envelope.session_id,
            participant_id: request_envelope.participant_id,
            data: Some(sfu_response_envelope::Data::ResumeConsumerResponse(response_data)),
        };

        Ok(Response::new(response_envelope))
    }

    async fn close_session(
        &self,
        request: Request<SfuRequestEnvelope>
    ) -> Result<Response<SfuResponseEnvelope>, Status> {
        let request_envelope = request.into_inner();

        let Some(sfu_request_envelope::Data::CloseSessionRequest(request_data)) = request_envelope.data else {
            return Err(Status::invalid_argument("Invalid request type"));
        };

        let session_id = request_data.session_id
            .ok_or_else(|| Status::invalid_argument("Session id is required"))?
            .id
            .parse::<Uuid>()
            .map_err(|e| Status::invalid_argument(format!("Invalid session id format: {e}")))?;

        self.sfu_core.lock().await.close_session(session_id);

        let response_data = CloseSessionResponse {};

        let response_envelope = SfuResponseEnvelope {
            r#type: "close_session".to_string(),
            session_id: request_envelope.session_id,
            participant_id: request_envelope.participant_id,
            data: Some(sfu_response_envelope::Data::CloseSessionResponse(response_data)),
        };
        
        Ok(Response::new(response_envelope))
    }

    async fn subscribe_to_events(
        &self,
        request: Request<SfuRequestEnvelope>
    ) -> Result<Response<Self::SubscribeToEventsStream>, Status> {
        let request_envelope = request.into_inner();

        let Some(sfu_request_envelope::Data::SubscribeToEventsRequest(request_data)) = request_envelope.data else {
            return Err(Status::invalid_argument("Invalid request type"));
        };

        let session_id = request_data.session_id
            .ok_or_else(|| Status::invalid_argument("Session id is required"))?
            .id
            .parse::<Uuid>()
            .map_err(|e| Status::invalid_argument(format!("Invalid session id format: {e}")))?;

        let participant_id = request_envelope.participant_id.clone();

        let (sender, receiver) = mpsc::channel(self.subscribe_channel_capacity);

        self.sfu_core
            .lock()
            .await
            .subscribe_to_events(session_id, participant_id, sender)
            .await
            .map_err(|e| Status::internal(format!("Failed to subscribe to events: {e}")))?;

        let stream = ReceiverStream::new(receiver);
        
        Ok(Response::new(stream))
    }
}
