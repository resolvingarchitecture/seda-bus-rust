//! A three-stage pipeline: ingest -> transform -> sink, via a routing slip.
//!
//! ```sh
//! cargo run --example pipeline
//! ```

use std::sync::mpsc::channel;
use std::time::Duration;

use seda_bus::{Bus, ChannelConfig, Envelope};

fn main() {
    let bus = Bus::new(4);

    bus.channel("ingest", ChannelConfig::default().capacity(100));
    bus.channel(
        "transform",
        ChannelConfig::default().capacity(100).concurrency(2),
    );
    bus.channel("sink", ChannelConfig::default().capacity(100));

    bus.subscribe("ingest", |e: &mut Envelope| {
        e.headers.insert("seen_by".into(), "ingest".into());
        true
    });
    bus.subscribe("transform", |e: &mut Envelope| {
        e.payload.make_ascii_uppercase();
        true
    });

    let (tx, rx) = channel();
    bus.subscribe("sink", move |e: &mut Envelope| {
        tx.send(String::from_utf8_lossy(&e.payload).into_owned())
            .is_ok()
    });

    for word in ["alpha", "bravo", "charlie", "delta", "echo"] {
        bus.publish(
            Envelope::new("ingest", word.as_bytes().to_vec()).with_slip(["transform", "sink"]),
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
