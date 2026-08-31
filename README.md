<div align="center">
  <h1>seda-bus</h1>
  <p><strong>Resolving Architecture &mdash; Clarity in Design</strong></p>
  <p>A small, broker-less, <strong>staged</strong> message bus for Rust.</p>
</div>

Work is decomposed into stages (`Channel`s) connected by bounded queues. One
shared thread pool drains every stage; each stage has its own concurrency limit
so none can monopolise the pool. There is no broker and no external
dependency beyond `log`.

```rust
use seda_bus::{Bus, ChannelConfig, Delivery, Envelope};
use std::sync::mpsc::channel;
use std::time::Duration;

let bus = Bus::new(4); // 4 shared worker threads

bus.channel("ingest",    ChannelConfig::default().capacity(1000));
bus.channel("transform", ChannelConfig::default().capacity(1000).concurrency(4));
bus.channel("sink",      ChannelConfig::default().capacity(1000));

bus.subscribe("ingest",    |_e: &mut Envelope| true);
bus.subscribe("transform", |e: &mut Envelope| { e.payload.make_ascii_uppercase(); true });

let (tx, rx) = channel();
bus.subscribe("sink", move |e: &mut Envelope| tx.send(e.payload.clone()).is_ok());

bus.publish(
    Envelope::new("ingest", b"hello".to_vec()).with_slip(["transform", "sink"]),
    Some(Duration::from_secs(1)),
);
assert_eq!(rx.recv_timeout(Duration::from_secs(2)).unwrap(), b"HELLO");
bus.shutdown(Duration::from_secs(5));
```

## Features

| | |
|---|---|
| **Bounded stages** | each channel has a capacity &mdash; admission control |
| **Back-pressure policy** | `Block` / `Reject` / `DropNewest` / `DropOldest` per stage |
| **Per-stage concurrency** | how many envelopes a stage may process at once |
| **Delivery** | `PointToPoint` (round-robin) or `PubSub` (fan-out) |
| **Routing slips** | an envelope carries an itinerary of stages to visit |
| **Retry + dead-letter** | nacked envelopes retry up to `max_attempts`, then route to a DLQ |
| **Metrics** | per-stage enqueued / delivered / nacked / dropped / dead-lettered / depth |
| **Graceful shutdown** | stop accepting, drain within a timeout, then join the pool |

## What this is not

SEDA's original design also included a **controller** that watched per-stage
latency and queue depth at runtime and re-tuned thread allocation and shed load
automatically. That adaptive controller is not implemented here &mdash; every
setting is static configuration. It is the interesting next step (`2.0`), and
the reason the model is worth revisiting. Matt Welsh (SEDA's author) later
argued the strict per-stage-queue-and-pool split was usually a mistake; this
bus already uses one shared pool, in line with that retrospective.

## Companion implementations

Same design, other languages:

* [seda-bus-java](https://github.com/resolvingarchitecture/seda-bus-java) &mdash; the original, with optional guaranteed-delivery persistence
* [seda-bus-python](https://github.com/resolvingarchitecture/seda-bus-python) &mdash; built to exercise free-threaded (PEP 703) CPython

## Development

```sh
cargo test
cargo run --example pipeline
```

## Status

`0.3.0` &mdash; working core, tested. Not stable until `1.0`.

## Reference

Welsh, Culler, Brewer. *SEDA: An Architecture for Well-Conditioned, Scalable
Internet Services.* SOSP 2001.
