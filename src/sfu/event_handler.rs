use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use mediasoup::prelude::{
    Consumer as MediasoupConsumer,
    ConsumerId as MediasoupConsumerId,
    MediaKind as MediasoupMediaKind,
    Producer as MediasoupProducer,
    ProducerId as MediasoupProducerId,
    Transport as MediasoupTransport,
    TransportId as MediasoupTransportId,
};
use saasy_proto_rust::sfu::{
    sfu_event,
    ConsumerClosedEvent,
    NewProducerEvent,
    ProducerClosedEvent,
    SfuEvent,
    TransportClosedEvent,
};
use saasy_proto_rust::shared::{
    ConsumerId,
    MediaKind,
    ProducerId,
    TransportId,
};
use tracing::info;
use uuid::Uuid;

pub struct EventHandler;

pub enum PendingEventSetup {
    Transport {
        transport: Arc<dyn MediasoupTransport>,
        session_id: Uuid,
        participant_id: String,
    },
    Producer {
        producer: Arc<MediasoupProducer>,
        session_id: Uuid,
        participant_id: String,
        kind: MediasoupMediaKind,
    },
    Consumer {
        consumer: Arc<MediasoupConsumer>,
        session_id: Uuid,
        participant_id: String,
    },
}

impl EventHandler {
    pub fn create_transport_closed_event(transport_id: MediasoupTransportId) -> SfuEvent {
        let proto_id: TransportId = transport_id.into();
        SfuEvent {
            event: Some(sfu_event::Event::TransportClosed(TransportClosedEvent {
                transport_id: proto_id.id,
            })),
        }
    }

    pub fn create_new_producer_event(producer_id: MediasoupProducerId, kind: MediasoupMediaKind) -> SfuEvent {
        let proto_id: ProducerId = producer_id.into();
        let proto_kind: MediaKind = kind.into();
        SfuEvent {
            event: Some(sfu_event::Event::NewProducer(NewProducerEvent {
                producer_id: proto_id.id,
                kind: proto_kind as i32,
            })),
        }
    }

    pub fn create_producer_closed_event(producer_id: MediasoupProducerId) -> SfuEvent {
        let proto_id: ProducerId = producer_id.into();
        SfuEvent {
            event: Some(sfu_event::Event::ProducerClosed(ProducerClosedEvent {
                producer_id: proto_id.id,
            })),
        }
    }

    pub fn create_consumer_closed_event(consumer_id: MediasoupConsumerId) -> SfuEvent {
        let proto_id: ConsumerId = consumer_id.into();
        SfuEvent {
            event: Some(sfu_event::Event::ConsumerClosed(ConsumerClosedEvent {
                consumer_id: proto_id.id,
            })),
        }
    }

    pub fn setup_transport_events<F>(
        transport: &Arc<dyn MediasoupTransport>,
        session_id: Uuid,
        event_callback: F,
    ) where
        F: FnMut(Uuid, SfuEvent) + Send + 'static + Clone,
    {
        let transport_id = transport.id();
        let is_closed = Arc::new(AtomicBool::new(false));

        // Handle transport close event
        {
            let is_closed = Arc::clone(&is_closed);
            let mut callback = event_callback.clone();
            let _ = transport.on_close(Box::new(move || {
                if is_closed.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
                    info!("Transport {transport_id} closed");
                    let event = Self::create_transport_closed_event(transport_id);
                    callback(session_id, event);
                }
            }));
        }
        
        // Handle router close event
        {
            let is_closed = Arc::clone(&is_closed);
            let mut callback = event_callback;
            let _ = transport.on_router_close(Box::new(move || {
                if is_closed.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
                    info!("Transport {transport_id} closed due to router close");
                    let event = Self::create_transport_closed_event(transport_id);
                    callback(session_id, event);
                }
            }));
        }
    }

    pub fn setup_producer_events<F>(
        producer: &Arc<MediasoupProducer>,
        session_id: Uuid,
        event_callback: F,
    ) where
        F: FnMut(Uuid, SfuEvent) + Send + 'static + Clone,
    {
        let producer_id = producer.id();
        let is_closed = Arc::new(AtomicBool::new(false));

        // Handle producer close event
        {
            let is_closed = Arc::clone(&is_closed);
            let mut callback = event_callback.clone();
            let _ = producer.on_close(Box::new(move || {
                if is_closed.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
                    info!("Producer {producer_id} closed");
                    let event = Self::create_producer_closed_event(producer_id);
                    callback(session_id, event);
                }
            }));
        }
        
        // Handle transport close event
        {
            let is_closed = Arc::clone(&is_closed);
            let mut callback = event_callback;
            let _ = producer.on_transport_close(Box::new(move || {
                if is_closed.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
                    info!("Producer {producer_id} closed due to transport close");
                    let event = Self::create_producer_closed_event(producer_id);
                    callback(session_id, event);
                }
            }));
        }
    }

    pub fn setup_consumer_events<F>(
        consumer: &Arc<MediasoupConsumer>,
        session_id: Uuid,
        event_callback: F,
    ) where
        F: FnMut(Uuid, SfuEvent) + Send + 'static + Clone,
    {
        let consumer_id = consumer.id();
        let is_closed = Arc::new(AtomicBool::new(false));

        // Handle consumer close event
        {
            let is_closed = Arc::clone(&is_closed);
            let mut callback = event_callback.clone();
            let _ = consumer.on_close(Box::new(move || {
                if is_closed.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
                    info!("Consumer {consumer_id} closed");
                    let event = Self::create_consumer_closed_event(consumer_id);
                    callback(session_id, event);
                }
            }));
        }
        
        // Handle transport close event
        {
            let is_closed = Arc::clone(&is_closed);
            let mut callback = event_callback.clone();
            let _ = consumer.on_transport_close(Box::new(move || {
                if is_closed.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
                    info!("Consumer {consumer_id} closed due to transport close");
                    let event = Self::create_consumer_closed_event(consumer_id);
                    callback(session_id, event);
                }
            }));
        }
        
        // Handle producer close event
        {
            let is_closed = Arc::clone(&is_closed);
            let mut callback = event_callback;
            let _ = consumer.on_producer_close(Box::new(move || {
                if is_closed.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
                    info!("Consumer {consumer_id} closed due to producer close");
                    let event = Self::create_consumer_closed_event(consumer_id);
                    callback(session_id, event);
                }
            }));
        }
    }
}
