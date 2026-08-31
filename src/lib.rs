//! A small, broker-less, **staged** message bus.
//!
//! Work is decomposed into stages ([`Bus::channel`]) connected by bounded
//! queues. One shared thread pool drains every stage; each stage has its own
//! concurrency limit so none can monopolise the pool.
//!
//! ```
//! use seda_bus::{Bus, ChannelConfig, Delivery, Envelope};
//! use std::sync::mpsc::channel;
//! use std::time::Duration;
//!
//! let bus = Bus::new(4);
//! bus.channel("upper", ChannelConfig::default().capacity(64));
//! let (tx, rx) = channel();
//! bus.subscribe("upper", move |e: &mut Envelope| {
//!     e.payload.make_ascii_uppercase();
//!     tx.send(e.payload.clone()).is_ok()
//! });
//!
//! bus.publish(Envelope::new("upper", b"hello".to_vec()), Some(Duration::from_secs(1)));
//! assert_eq!(rx.recv_timeout(Duration::from_secs(2)).unwrap(), b"HELLO");
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
pub use envelope::Envelope;
