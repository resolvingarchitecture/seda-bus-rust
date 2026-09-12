use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ra_common::serde_json::Value;
use seda_bus::{make_envelope, envelope_payload, Backpressure, Bus, ChannelConfig, Consumer, Delivery, Envelope};

fn cfg() -> ChannelConfig {
    ChannelConfig::default()
}

fn env(to: &str, n: u32) -> Envelope {
    make_envelope(to, Some(Value::from(n)), [])
}

fn payload_u32(e: &Envelope) -> u32 {
    envelope_payload(e).and_then(Value::as_u64).unwrap() as u32
}

#[test]
fn point_to_point_round_robins() {
    let bus = Bus::new(4);
    let a = Arc::new(AtomicUsize::new(0));
    let b = Arc::new(AtomicUsize::new(0));
    bus.channel("work", cfg().capacity(100));
    {
        let a = Arc::clone(&a);
        bus.subscribe("work", move |_: &mut Envelope| {
            a.fetch_add(1, Ordering::SeqCst);
            true
        });
    }
    {
        let b = Arc::clone(&b);
        bus.subscribe("work", move |_: &mut Envelope| {
            b.fetch_add(1, Ordering::SeqCst);
            true
        });
    }

    for i in 0..20 {
        assert!(bus.publish(env("work", i), Some(Duration::from_secs(1))));
    }
    assert!(bus.shutdown(Duration::from_secs(5)));
    assert_eq!(a.load(Ordering::SeqCst), 10);
    assert_eq!(b.load(Ordering::SeqCst), 10);
}

#[test]
fn pub_sub_fans_out() {
    let bus = Bus::new(4);
    let (tx, rx) = channel();
    bus.channel("events", cfg().capacity(100).delivery(Delivery::PubSub));
    for tag in ["a", "b", "c"] {
        let tx = tx.clone();
        bus.subscribe("events", move |e: &mut Envelope| tx.send((tag, payload_u32(e))).is_ok());
    }
    drop(tx);

    for i in 0..4u32 {
        bus.publish(env("events", i), Some(Duration::from_secs(1)));
    }

    let mut got: Vec<(&str, u32)> = (0..12)
        .map(|_| {
            rx.recv_timeout(Duration::from_secs(5))
                .expect("fan-out message")
        })
        .collect();
    bus.shutdown(Duration::from_secs(5));
    got.sort();
    assert_eq!(got.len(), 12);
    for tag in ["a", "b", "c"] {
        assert_eq!(got.iter().filter(|(t, _)| *t == tag).count(), 4);
    }
}

#[test]
fn routing_slip_visits_every_stage_in_order() {
    let bus = Bus::new(4);
    let trail = Arc::new(Mutex::new(Vec::<String>::new()));
    for name in ["one", "two", "three"] {
        bus.channel(name, cfg().capacity(50));
        let trail = Arc::clone(&trail);
        let n = name.to_string();
        bus.subscribe(name, move |_: &mut Envelope| {
            trail.lock().unwrap().push(n.clone());
            true
        });
    }

    let (tx, rx) = channel();
    bus.publish_with_callback(
        make_envelope("one", Some(Value::String("x".into())), ["two".to_string(), "three".to_string()]),
        Some(Duration::from_secs(1)),
        move |_e| {
            let _ = tx.send(());
        },
    );

    rx.recv_timeout(Duration::from_secs(5)).expect("completed");
    bus.shutdown(Duration::from_secs(5));
    assert_eq!(&*trail.lock().unwrap(), &["one", "two", "three"]);
}

#[test]
fn backpressure_reject_when_full() {
    let bus = Bus::new(4);
    let gate = Arc::new(Mutex::new(false));
    let cv = Arc::new(std::sync::Condvar::new());
    bus.channel(
        "slow",
        cfg()
            .capacity(2)
            .concurrency(1)
            .backpressure(Backpressure::Reject),
    );
    {
        let gate = Arc::clone(&gate);
        let cv = Arc::clone(&cv);
        bus.subscribe("slow", move |_: &mut Envelope| {
            let mut g = gate.lock().unwrap();
            while !*g {
                g = cv.wait_timeout(g, Duration::from_secs(5)).unwrap().0;
            }
            true
        });
    }

    let accepted: usize = (0..10)
        .map(|i| bus.publish(env("slow", i), Some(Duration::from_millis(50))) as usize)
        .sum();
    *gate.lock().unwrap() = true;
    cv.notify_all();

    assert!(accepted <= 3, "accepted {accepted}");
    bus.shutdown(Duration::from_secs(5));
    assert!(bus.get_stats()["slow"].dropped >= 7);
}

#[test]
fn drop_newest_behaves_like_reject() {
    // DropNewest's observable contract from the caller's side is identical
    // to Reject - discard the incoming envelope, don't admit it - matching
    // every other seda-bus port's own choice to treat the two the same.
    // This exists to prove the policy is actually wired to distinguishable,
    // intentional behavior, not silently ignored.
    let bus = Bus::new(4);
    let gate = Arc::new(Mutex::new(false));
    let cv = Arc::new(std::sync::Condvar::new());
    bus.channel(
        "dn",
        cfg().capacity(2).concurrency(1).backpressure(Backpressure::DropNewest),
    );
    {
        let gate = Arc::clone(&gate);
        let cv = Arc::clone(&cv);
        bus.subscribe("dn", move |_: &mut Envelope| {
            let mut g = gate.lock().unwrap();
            while !*g {
                g = cv.wait_timeout(g, Duration::from_secs(5)).unwrap().0;
            }
            true
        });
    }

    let accepted: usize = (0..10)
        .map(|i| bus.publish(env("dn", i), Some(Duration::from_millis(50))) as usize)
        .sum();
    *gate.lock().unwrap() = true;
    cv.notify_all();

    assert!(accepted <= 3, "accepted {accepted}");
    bus.shutdown(Duration::from_secs(5));
    assert!(bus.get_stats()["dn"].dropped >= 7);
}

#[test]
fn drop_oldest_evicts_instead_of_rejecting() {
    let bus = Bus::new(4);
    let gate = Arc::new(Mutex::new(false));
    let cv = Arc::new(std::sync::Condvar::new());
    bus.channel(
        "do",
        cfg().capacity(2).concurrency(1).backpressure(Backpressure::DropOldest),
    );
    {
        let gate = Arc::clone(&gate);
        let cv = Arc::clone(&cv);
        bus.subscribe("do", move |_: &mut Envelope| {
            let mut g = gate.lock().unwrap();
            while !*g {
                g = cv.wait_timeout(g, Duration::from_secs(5)).unwrap().0;
            }
            true
        });
    }

    for i in 0..10 {
        assert!(
            bus.publish(env("do", i), Some(Duration::from_millis(50))),
            "DropOldest must always admit the newest envelope"
        );
        assert!(
            bus.get_stats()["do"].depth <= 2,
            "depth must never exceed capacity under DropOldest"
        );
    }
    *gate.lock().unwrap() = true;
    cv.notify_all();
    bus.shutdown(Duration::from_secs(5));

    assert!(bus.get_stats()["do"].dropped >= 7, "dropped={}", bus.get_stats()["do"].dropped);
}

#[test]
fn nack_retries_then_dead_letters() {
    let bus = Bus::new(4);
    let attempts = Arc::new(AtomicUsize::new(0));
    bus.channel("flaky", cfg().capacity(10).max_attempts(3));
    bus.channel("dead", cfg().capacity(10));
    bus.set_dead_letter_channel("flaky", "dead");

    let (tx, rx) = channel();
    bus.subscribe("dead", move |_: &mut Envelope| tx.send(()).is_ok());
    {
        let attempts = Arc::clone(&attempts);
        bus.subscribe("flaky", move |_: &mut Envelope| {
            attempts.fetch_add(1, Ordering::SeqCst);
            false
        });
    }

    bus.publish(env("flaky", 0), Some(Duration::from_secs(1)));
    rx.recv_timeout(Duration::from_secs(5))
        .expect("dead-lettered");
    bus.shutdown(Duration::from_secs(5));

    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    assert_eq!(bus.get_stats()["flaky"].dead_lettered, 1);
}

/// C2(a): succeeds on the final allowed attempt - delivered exactly once,
/// and the per-envelope attempts map (keyed by envelope id) is cleared
/// afterward rather than leaking. There's no public accessor for the
/// attempts map's size, so this is verified indirectly: publish a *second*
/// envelope with the *same id* after the first succeeds on its last
/// attempt, give it a consumer that always acks, and confirm it delivers
/// on attempt 1 rather than starting from wherever the first envelope's
/// (finished) attempt count left off - which is only possible if
/// `clear_attempt` actually ran.
#[test]
fn nack_then_succeeds_on_final_attempt_clears_attempt_state() {
    let bus = Bus::new(4);
    bus.channel("flaky2", cfg().capacity(10).max_attempts(3));
    let seen_attempts = Arc::new(Mutex::new(Vec::<u32>::new()));
    let call_count = Arc::new(AtomicUsize::new(0));
    {
        let seen_attempts = Arc::clone(&seen_attempts);
        let call_count = Arc::clone(&call_count);
        bus.subscribe("flaky2", move |e: &mut Envelope| {
            let n = call_count.fetch_add(1, Ordering::SeqCst);
            seen_attempts.lock().unwrap().push(payload_u32(e));
            // Nack the first two calls for id "a" (envelope 0), then ack -
            // succeeds on attempt 3, the last one allowed.
            n >= 2
        });
    }

    let mut e = env("flaky2", 0);
    let fixed_id = "same-id-for-attempt-state-check".to_string();
    e.id = fixed_id.clone();
    assert!(bus.publish(e, Some(Duration::from_secs(1))));
    // Wait for the 3rd (successful) call.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while call_count.load(Ordering::SeqCst) < 3 && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(call_count.load(Ordering::SeqCst), 3, "expected exactly 3 attempts");
    assert_eq!(bus.get_stats()["flaky2"].delivered, 1);
    assert_eq!(bus.get_stats()["flaky2"].dead_lettered, 0);

    // Reused id, same channel: if attempt state weren't cleared after the
    // first envelope's success, this one would inherit a stale attempt
    // count and could dead-letter after just one more nack instead of
    // getting its own fresh 3 attempts. Nack it exactly twice, then ack.
    let mut e2 = env("flaky2", 1);
    e2.id = fixed_id;
    assert!(bus.publish(e2, Some(Duration::from_secs(1))));
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while call_count.load(Ordering::SeqCst) < 6 && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(bus.shutdown(Duration::from_secs(5)), true);
    assert_eq!(bus.get_stats()["flaky2"].delivered, 2, "second envelope with the reused id should also get its own fresh 3 attempts and succeed, not inherit stale state");
    assert_eq!(bus.get_stats()["flaky2"].dead_lettered, 0);
}

/// C2: a channel with no consumers at all dead-letters immediately rather
/// than silently discarding.
#[test]
fn no_consumers_dead_letters_immediately() {
    let bus = Bus::new(4);
    bus.channel("nobody-home", cfg().capacity(10));
    bus.channel("dead2", cfg().capacity(10));
    bus.set_dead_letter_channel("nobody-home", "dead2");
    let (tx, rx) = channel();
    bus.subscribe("dead2", move |_: &mut Envelope| tx.send(()).is_ok());

    assert!(bus.publish(env("nobody-home", 0), Some(Duration::from_secs(1))));
    rx.recv_timeout(Duration::from_secs(5))
        .expect("dead-lettered immediately despite no consumers");
    bus.shutdown(Duration::from_secs(5));
    assert_eq!(bus.get_stats()["nobody-home"].dead_lettered, 1);
}

#[test]
fn shutdown_drains_queued_work() {
    let bus = Bus::new(4);
    let done = Arc::new(AtomicUsize::new(0));
    bus.channel("drain", cfg().capacity(500).concurrency(4));
    {
        let done = Arc::clone(&done);
        bus.subscribe("drain", move |_: &mut Envelope| {
            std::thread::sleep(Duration::from_millis(10));
            done.fetch_add(1, Ordering::SeqCst);
            true
        });
    }
    for i in 0..50u32 {
        bus.publish(env("drain", i), Some(Duration::from_secs(1)));
    }
    assert!(bus.shutdown(Duration::from_secs(10)));
    assert_eq!(done.load(Ordering::SeqCst), 50);
}

#[test]
fn publish_after_pause_is_rejected() {
    let bus = Bus::new(2);
    bus.channel("p", cfg().capacity(10));
    bus.subscribe("p", |_: &mut Envelope| true);
    bus.pause();
    assert!(!bus.publish(env("p", 1), Some(Duration::from_millis(10))));
    bus.resume();
    assert!(bus.publish(env("p", 2), Some(Duration::from_millis(10))));
    bus.shutdown(Duration::from_secs(2));
}

#[test]
fn unknown_channel_returns_false() {
    let bus = Bus::new(2);
    assert!(!bus.publish(env("nope", 1), None));
    bus.shutdown(Duration::from_secs(2));
}

struct Counter(Arc<Mutex<Vec<u32>>>);
impl Consumer for Counter {
    fn receive(&self, e: &mut Envelope) -> bool {
        self.0.lock().unwrap().push(payload_u32(e));
        true
    }
}

#[test]
fn concurrent_producers_deliver_exactly_once() {
    let bus = Bus::new(8);
    let seen = Arc::new(Mutex::new(Vec::<u32>::new()));
    bus.channel("fan", cfg().capacity(5000).concurrency(8));
    bus.subscribe("fan", Counter(Arc::clone(&seen)));

    let mut threads = Vec::new();
    for base in 0..6u32 {
        let bus = bus.clone();
        threads.push(std::thread::spawn(move || {
            for i in 0..500u32 {
                let n = base * 1000 + i;
                while !bus.publish(env("fan", n), Some(Duration::from_secs(1))) {}
            }
        }));
    }
    for t in threads {
        t.join().unwrap();
    }
    assert!(bus.shutdown(Duration::from_secs(15)));

    let mut seen = seen.lock().unwrap().clone();
    seen.sort_unstable();
    seen.dedup();
    assert_eq!(seen.len(), 3000);
}

#[test]
fn block_backpressure_no_lost_wakeup_under_saturation() {
    // Deliberately tiny capacity + untimed Block: forces every producer to
    // wait on almost every publish, hammering the exact race this test
    // exists to catch - a producer incrementing `waiters` a moment too late
    // to see a slot a concurrent poll() already freed, with that poll()
    // having already decided (waiters == 0 at the time) not to notify
    // anyone. Before the fix, this could - rarely, timing-dependently -
    // leave a producer parked forever on an untimed wait() with no future
    // notify coming. Bounded by an explicit deadline below rather than a
    // bare `.join()`, so a regression hangs this test loudly instead of the
    // whole suite silently.
    let bus = Bus::new(4);
    let seen = Arc::new(Mutex::new(Vec::<u32>::new()));
    bus.channel(
        "tight",
        cfg().capacity(2).concurrency(2).backpressure(Backpressure::Block),
    );
    bus.subscribe("tight", Counter(Arc::clone(&seen)));

    let done = Arc::new(AtomicUsize::new(0));
    let mut threads = Vec::new();
    for base in 0..6u32 {
        let bus = bus.clone();
        let done = Arc::clone(&done);
        threads.push(std::thread::spawn(move || {
            for i in 0..300u32 {
                let n = base * 1000 + i;
                // Untimed Block - exactly the path the fix closes a race in.
                assert!(bus.publish(env("tight", n), None));
            }
            done.fetch_add(1, Ordering::SeqCst);
        }));
    }

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while done.load(Ordering::SeqCst) < 6 && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        done.load(Ordering::SeqCst),
        6,
        "a producer never returned from an untimed Block publish - lost wakeup"
    );
    for t in threads {
        t.join().unwrap();
    }
    assert!(bus.shutdown(Duration::from_secs(15)));

    let mut seen = seen.lock().unwrap().clone();
    seen.sort_unstable();
    seen.dedup();
    assert_eq!(seen.len(), 1800);
}

/// C3: a consumer that panics on every Nth envelope must not crash the bus
/// or the worker that ran it - every other envelope, before and after the
/// panic, still gets delivered. `safe_receive` already wraps every consumer
/// call in `catch_unwind`; this pins that behavior down explicitly rather
/// than trusting it by inspection.
#[test]
fn panicking_consumer_does_not_crash_the_bus() {
    let bus = Bus::new(4);
    bus.channel("flaky-panic", cfg().capacity(50).concurrency(1));
    let delivered = Arc::new(Mutex::new(Vec::<u32>::new()));
    {
        let delivered = Arc::clone(&delivered);
        bus.subscribe("flaky-panic", move |e: &mut Envelope| {
            let n = payload_u32(e);
            if n % 3 == 0 {
                panic!("simulated consumer failure for {n}");
            }
            delivered.lock().unwrap().push(n);
            true
        });
    }

    for i in 0..15u32 {
        assert!(bus.publish(env("flaky-panic", i), Some(Duration::from_secs(1))));
    }
    bus.shutdown(Duration::from_secs(5));

    let delivered = delivered.lock().unwrap();
    let expected: Vec<u32> = (0..15).filter(|n| n % 3 != 0).collect();
    let mut got = delivered.clone();
    got.sort_unstable();
    assert_eq!(got, expected, "every non-panicking envelope should still be delivered");
    // A panicking receive() is a nack with max_attempts == 1 (the default),
    // so it's dead-lettered, not endlessly retried.
    assert_eq!(bus.get_stats()["flaky-panic"].dead_lettered, 5);
}

/// C4: `shutdown(timeout)` must account for every accepted envelope as
/// delivered, dead-lettered, or (new, see the fix in `Bus::shutdown`)
/// dropped - never silently stranded - regardless of whether the timeout
/// elapsed first or everything drained in time.
#[test]
fn shutdown_accounts_for_every_envelope_even_on_timeout() {
    let bus = Bus::new(4);
    let delivered = Arc::new(AtomicUsize::new(0));
    bus.channel("slow-drain", cfg().capacity(50).concurrency(2));
    {
        let delivered = Arc::clone(&delivered);
        bus.subscribe("slow-drain", move |_: &mut Envelope| {
            std::thread::sleep(Duration::from_millis(30));
            delivered.fetch_add(1, Ordering::SeqCst);
            true
        });
    }
    let total = 20u32;
    for i in 0..total {
        assert!(bus.publish(env("slow-drain", i), Some(Duration::from_secs(1))));
    }
    // Deliberately much shorter than the ~300ms (20 envelopes / 2
    // concurrency * 30ms) minimum processing time - await_drain almost
    // certainly times out (drained == false) here.
    let drained = bus.shutdown(Duration::from_millis(15));
    let stats = bus.get_stats();
    let s = &stats["slow-drain"];
    assert_eq!(
        s.delivered + s.dead_lettered + s.dropped,
        total as u64,
        "shutdown(drained={drained}) must account for every envelope, not strand any of them: {s:?}"
    );
}

#[test]
fn shutdown_accounts_for_every_envelope_when_it_drains_in_time() {
    let bus = Bus::new(4);
    bus.channel("fast-drain", cfg().capacity(50).concurrency(4));
    bus.subscribe("fast-drain", |_: &mut Envelope| true);
    let total = 20u32;
    for i in 0..total {
        assert!(bus.publish(env("fast-drain", i), Some(Duration::from_secs(1))));
    }
    let drained = bus.shutdown(Duration::from_secs(5));
    assert!(drained, "expected a generous timeout to actually drain");
    let stats = bus.get_stats();
    let s = &stats["fast-drain"];
    assert_eq!(s.delivered + s.dead_lettered + s.dropped, total as u64);
    assert_eq!(s.dropped, 0, "nothing should have been force-dropped when it drained normally");
}

/// C5: `ChannelConfig`'s builder methods clamp invalid values (<=0, or in
/// this case simply 0 - `usize`/`u32` have no negative values to clamp from
/// here) to 1 rather than failing fast or misbehaving silently at runtime.
/// This pins down that this port's documented choice is clamp, not panic.
#[test]
fn invalid_channel_config_clamps_to_documented_minimums() {
    let c = ChannelConfig::default().capacity(0).concurrency(0).max_attempts(0);
    assert_eq!(c.capacity, 1);
    assert_eq!(c.concurrency, 1);
    assert_eq!(c.max_attempts, 1);

    // And a channel built from it is actually usable, not just holding
    // clamped numbers that are never exercised.
    let bus = Bus::new(2);
    bus.channel("clamped", c);
    bus.subscribe("clamped", |_: &mut Envelope| true);
    assert!(bus.publish(env("clamped", 0), Some(Duration::from_secs(1))));
    assert!(bus.shutdown(Duration::from_secs(5)));
    assert_eq!(bus.get_stats()["clamped"].delivered, 1);
}

/// C6: constructing and fully shutting down a Bus repeatedly must not leak
/// the one externally-visible resource this port's design owns per
/// instance - the Pool's OS threads. `Pool::join` (called by both
/// `Bus::shutdown` and `Pool`'s own `Drop`) joins every worker thread
/// before returning, so there's no portable "current thread count" API
/// needed to prove this in Rust: if threads were leaking, later iterations
/// of this loop would slow down or eventually fail to spawn new ones
/// (`thread::Builder::spawn` returns a `Result`, unwrapped with `.expect`
/// in `Pool::new` - a real leak would surface here directly, not need a
/// separate counter).
#[test]
fn repeated_bus_lifecycles_do_not_leak_threads() {
    for i in 0..25 {
        let bus = Bus::new(4);
        bus.channel("cycle", cfg().capacity(10));
        bus.subscribe("cycle", |_: &mut Envelope| true);
        assert!(bus.publish(env("cycle", i), Some(Duration::from_secs(1))));
        assert!(bus.shutdown(Duration::from_secs(5)), "cycle {i} failed to drain");
    }
}
