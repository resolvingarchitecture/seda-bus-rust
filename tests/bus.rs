use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use seda_bus::{Backpressure, Bus, ChannelConfig, Consumer, Delivery, Envelope};

fn cfg() -> ChannelConfig {
    ChannelConfig::default()
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
        assert!(bus.publish(Envelope::new("work", vec![i]), Some(Duration::from_secs(1))));
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
        bus.subscribe("events", move |e: &mut Envelope| {
            tx.send((tag, e.payload[0])).is_ok()
        });
    }
    drop(tx);

    for i in 0..4u8 {
        bus.publish(
            Envelope::new("events", vec![i]),
            Some(Duration::from_secs(1)),
        );
    }

    let mut got: Vec<(&str, u8)> = (0..12)
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
        Envelope::new("one", b"x".to_vec()).with_slip(["two", "three"]),
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
        .map(|i| {
            bus.publish(
                Envelope::new("slow", vec![i]),
                Some(Duration::from_millis(50)),
            ) as usize
        })
        .sum();
    *gate.lock().unwrap() = true;
    cv.notify_all();

    assert!(accepted <= 3, "accepted {accepted}");
    bus.shutdown(Duration::from_secs(5));
    assert!(bus.get_stats()["slow"].dropped >= 7);
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

    bus.publish(
        Envelope::new("flaky", b"boom".to_vec()),
        Some(Duration::from_secs(1)),
    );
    rx.recv_timeout(Duration::from_secs(5))
        .expect("dead-lettered");
    bus.shutdown(Duration::from_secs(5));

    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    assert_eq!(bus.get_stats()["flaky"].dead_lettered, 1);
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
    for i in 0..50u8 {
        bus.publish(
            Envelope::new("drain", vec![i]),
            Some(Duration::from_secs(1)),
        );
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
    assert!(!bus.publish(Envelope::new("p", vec![1]), Some(Duration::from_millis(10))));
    bus.resume();
    assert!(bus.publish(Envelope::new("p", vec![2]), Some(Duration::from_millis(10))));
    bus.shutdown(Duration::from_secs(2));
}

#[test]
fn unknown_channel_returns_false() {
    let bus = Bus::new(2);
    assert!(!bus.publish(Envelope::new("nope", vec![1]), None));
    bus.shutdown(Duration::from_secs(2));
}

struct Counter(Arc<Mutex<Vec<u32>>>);
impl Consumer for Counter {
    fn receive(&self, env: &mut Envelope) -> bool {
        let n = u32::from_le_bytes(env.payload[..4].try_into().unwrap());
        self.0.lock().unwrap().push(n);
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
                while !bus.publish(
                    Envelope::new("fan", n.to_le_bytes().to_vec()),
                    Some(Duration::from_secs(1)),
                ) {}
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
