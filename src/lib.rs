//! A small, broker-less, **staged** message bus.
//!
//! Work is decomposed into stages ([`Bus::channel`]) connected by bounded
//! queues. One shared thread pool drains every stage; each stage has its own
//! concurrency limit so none can monopolise the pool.
//!
//! ```
//! use seda_bus::{make_envelope, envelope_payload, Bus, ChannelConfig, Delivery, Envelope};
//! use ra_common::serde_json::Value;
//! use std::sync::mpsc::channel;
//! use std::time::Duration;
//!
//! let bus = Bus::new(4);
//! bus.channel("upper", ChannelConfig::default().capacity(64));
//! let (tx, rx) = channel();
//! bus.subscribe("upper", move |e: &mut Envelope| {
//!     let s = envelope_payload(e).and_then(Value::as_str).unwrap_or("").to_uppercase();
//!     tx.send(s).is_ok()
//! });
//!
//! bus.publish(make_envelope("upper", Some(Value::String("hello".into())), []), Some(Duration::from_secs(1)));
//! assert_eq!(rx.recv_timeout(Duration::from_secs(2)).unwrap(), "HELLO");
//! bus.shutdown(Duration::from_secs(2));
//! ```
//!
//! What this is *not*: SEDA's original design also included a controller that
//! watched per-stage latency and queue depth at runtime and re-tuned thread
//! allocation and shed load automatically. That adaptive controller is future
//! work. This is the static-configuration core it builds on.

mod bus;
mod envelope;
mod pool;

pub use bus::{Backpressure, Bus, ChannelConfig, Consumer, Delivery, Stats};
pub use envelope::{envelope_payload, make_envelope, set_payload, target_service, Envelope};

pub use ra_common;
