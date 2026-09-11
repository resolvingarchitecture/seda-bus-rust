//! A three-stage pipeline: ingest -> transform -> sink, via a routing slip.
//!
//! ```sh
//! cargo run --example pipeline
//! ```

use std::sync::mpsc::channel;
use std::time::Duration;

use ra_common::serde_json::Value;
use seda_bus::{envelope_payload, make_envelope, set_payload, Bus, ChannelConfig, Envelope};

fn main() {
    let bus = Bus::new(4);

    bus.channel("ingest", ChannelConfig::default().capacity(100));
    bus.channel(
        "transform",
        ChannelConfig::default().capacity(100).concurrency(2),
    );
    bus.channel("sink", ChannelConfig::default().capacity(100));

    bus.subscribe("ingest", |e: &mut Envelope| {
        e.set_header("seen_by", Value::String("ingest".into()));
        true
    });
    bus.subscribe("transform", |e: &mut Envelope| {
        let s = envelope_payload(e).and_then(Value::as_str).unwrap_or("").to_uppercase();
        set_payload(e, Value::String(s));
        true
    });

    let (tx, rx) = channel();
    bus.subscribe("sink", move |e: &mut Envelope| {
        let s = envelope_payload(e).and_then(Value::as_str).unwrap_or("").to_string();
        tx.send(s).is_ok()
    });

    for word in ["alpha", "bravo", "charlie", "delta", "echo"] {
        bus.publish(
            make_envelope(
                "ingest",
                Some(Value::String(word.to_string())),
                ["transform".to_string(), "sink".to_string()],
            ),
            Some(Duration::from_secs(1)),
        );
    }

    let mut out: Vec<String> = (0..5)
        .filter_map(|_| rx.recv_timeout(Duration::from_secs(5)).ok())
        .collect();
    out.sort();
    println!("sink saw: {out:?}");

    bus.shutdown(Duration::from_secs(5));
    for (name, s) in bus.get_stats() {
        println!("  {name:<10} {s:?}");
    }
}
