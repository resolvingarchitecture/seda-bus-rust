//! The bus: a registry of stages drained by one shared worker pool.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::{Duration, Instant};

use log::warn;

use crate::envelope::Envelope;
use crate::pool::Pool;

/// Envelopes a single drain task handles before releasing its permit and
/// rescheduling. Amortises scheduling cost without starving other stages.
const BATCH: usize = 16;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Delivery {
    /// One consumer handles each envelope (round-robin across consumers).
    PointToPoint,
    /// Every consumer handles every envelope.
    PubSub,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Backpressure {
    /// Producer blocks (up to the publish timeout) until there is room.
    Block,
    /// `publish` returns `false` immediately when the stage queue is full.
    Reject,
    /// Silently discard the envelope being offered.
    DropNewest,
    /// Evict the oldest queued envelope to make room.
    DropOldest,
}

/// A consumer handles envelopes for a stage. Return `true` to ack, `false` to
/// nack (the envelope is retried up to the stage's `max_attempts`, then
/// dead-lettered).
pub trait Consumer: Send + Sync {
    fn receive(&self, envelope: &mut Envelope) -> bool;
}

impl<F> Consumer for F
where
    F: Fn(&mut Envelope) -> bool + Send + Sync,
{
    fn receive(&self, envelope: &mut Envelope) -> bool {
        self(envelope)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ChannelConfig {
    pub capacity: usize,
    pub concurrency: usize,
    pub delivery: Delivery,
    pub backpressure: Backpressure,
    pub max_attempts: u32,
}

impl Default for ChannelConfig {
    fn default() -> Self {
        ChannelConfig {
            capacity: 1024,
            concurrency: 1,
            delivery: Delivery::PointToPoint,
            backpressure: Backpressure::Block,
            max_attempts: 1,
        }
    }
}

impl ChannelConfig {
    pub fn capacity(mut self, n: usize) -> Self {
        self.capacity = n.max(1);
        self
    }
    pub fn concurrency(mut self, n: usize) -> Self {
        self.concurrency = n.max(1);
        self
    }
    pub fn delivery(mut self, d: Delivery) -> Self {
        self.delivery = d;
        self
    }
    pub fn backpressure(mut self, b: Backpressure) -> Self {
        self.backpressure = b;
        self
    }
    pub fn max_attempts(mut self, n: u32) -> Self {
        self.max_attempts = n.max(1);
        self
    }
}

#[derive(Debug, Default, Clone)]
pub struct Stats {
    pub depth: usize,
    pub enqueued: u64,
    pub delivered: u64,
    pub nacked: u64,
    pub dropped: u64,
    pub dead_lettered: u64,
}

struct Channel {
    name: String,
    cfg: ChannelConfig,
    queue: Mutex<VecDeque<Envelope>>,
    not_full: Condvar,
    consumers: RwLock<Vec<Arc<dyn Consumer>>>,
    rr: AtomicUsize,
    permits: AtomicUsize,
    enqueued: AtomicU64,
    delivered: AtomicU64,
    nacked: AtomicU64,
    dropped: AtomicU64,
    dead_lettered: AtomicU64,
}

impl Channel {
    fn new(name: String, cfg: ChannelConfig) -> Channel {
        Channel {
            name,
            cfg,
            queue: Mutex::new(VecDeque::new()),
            not_full: Condvar::new(),
            consumers: RwLock::new(Vec::new()),
            rr: AtomicUsize::new(0),
            permits: AtomicUsize::new(cfg.concurrency),
            enqueued: AtomicU64::new(0),
            delivered: AtomicU64::new(0),
            nacked: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            dead_lettered: AtomicU64::new(0),
        }
    }

    fn depth(&self) -> usize {
        self.queue.lock().unwrap().len()
    }

    fn offer(&self, env: Envelope, timeout: Option<Duration>) -> bool {
        let deadline = timeout.map(|d| Instant::now() + d);
        let mut q = self.queue.lock().unwrap();
        while q.len() >= self.cfg.capacity {
            match self.cfg.backpressure {
                Backpressure::Reject | Backpressure::DropNewest => {
                    self.dropped.fetch_add(1, Ordering::Relaxed);
                    return false;
                }
                Backpressure::DropOldest => {
                    q.pop_front();
                    self.dropped.fetch_add(1, Ordering::Relaxed);
                    break;
                }
                Backpressure::Block => match deadline {
                    None => q = self.not_full.wait(q).unwrap(),
                    Some(dl) => {
                        let now = Instant::now();
                        if now >= dl {
                            self.dropped.fetch_add(1, Ordering::Relaxed);
                            return false;
                        }
                        let (g, res) = self.not_full.wait_timeout(q, dl - now).unwrap();
                        q = g;
                        if res.timed_out() && q.len() >= self.cfg.capacity {
                            self.dropped.fetch_add(1, Ordering::Relaxed);
                            return false;
                        }
                    }
                },
            }
        }
        q.push_back(env);
        self.enqueued.fetch_add(1, Ordering::Relaxed);
        true
    }

    fn poll(&self) -> Option<Envelope> {
        let mut q = self.queue.lock().unwrap();
        let env = q.pop_front();
        if env.is_some() {
            self.not_full.notify_one();
        }
        env
    }

    fn requeue(&self, env: Envelope) {
        self.queue.lock().unwrap().push_front(env);
    }

    fn try_acquire(&self) -> bool {
        let mut cur = self.permits.load(Ordering::Acquire);
        loop {
            if cur == 0 {
                return false;
            }
            match self.permits.compare_exchange_weak(
                cur,
                cur - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => cur = actual,
            }
        }
    }

    fn release(&self) {
        self.permits.fetch_add(1, Ordering::AcqRel);
    }

    fn stats(&self) -> Stats {
        Stats {
            depth: self.depth(),
            enqueued: self.enqueued.load(Ordering::Relaxed),
            delivered: self.delivered.load(Ordering::Relaxed),
            nacked: self.nacked.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            dead_lettered: self.dead_lettered.load(Ordering::Relaxed),
        }
    }
}

type CompleteCb = Box<dyn FnOnce(&Envelope) + Send>;

struct Inner {
    channels: RwLock<HashMap<String, Arc<Channel>>>,
    dlq: RwLock<HashMap<String, String>>,
    callbacks: Mutex<HashMap<String, CompleteCb>>,
    pool: Pool,
    running: AtomicBool,
    accepting: AtomicBool,
}

/// A staged, broker-less message bus. Cheap to clone (it is an `Arc` inside).
#[derive(Clone)]
pub struct Bus(Arc<Inner>);

impl Bus {
    /// Create and start a bus with `workers` shared threads (defaults to the
    /// number of available cores when `0`).
    pub fn new(workers: usize) -> Bus {
        let workers = if workers == 0 {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
        } else {
            workers
        };
        Bus(Arc::new(Inner {
            channels: RwLock::new(HashMap::new()),
            dlq: RwLock::new(HashMap::new()),
            callbacks: Mutex::new(HashMap::new()),
            pool: Pool::new(workers),
            running: AtomicBool::new(true),
            accepting: AtomicBool::new(true),
        }))
    }

    pub fn workers(&self) -> usize {
        self.0.pool.size()
    }

    /// Register a stage. Re-registering a name is a no-op.
    pub fn channel(&self, name: impl Into<String>, cfg: ChannelConfig) -> &Self {
        let name = name.into();
        let mut chans = self.0.channels.write().unwrap();
        chans
            .entry(name.clone())
            .or_insert_with(|| Arc::new(Channel::new(name, cfg)));
        self
    }

    /// Attach a consumer to a stage. Creates the stage with defaults if needed.
    pub fn subscribe<C: Consumer + 'static>(
        &self,
        channel: impl Into<String>,
        consumer: C,
    ) -> &Self {
        let name = channel.into();
        let ch = self.get_or_create(&name, ChannelConfig::default());
        ch.consumers.write().unwrap().push(Arc::new(consumer));
        self
    }

    /// Route dead letters from `source` to the channel named `dlq`.
    pub fn set_dead_letter_channel(
        &self,
        source: impl Into<String>,
        dlq: impl Into<String>,
    ) -> &Self {
        let dlq = dlq.into();
        self.get_or_create(
            &dlq,
            ChannelConfig::default()
                .capacity(4096)
                .backpressure(Backpressure::DropOldest),
        );
        self.0.dlq.write().unwrap().insert(source.into(), dlq);
        self
    }

    fn get_or_create(&self, name: &str, cfg: ChannelConfig) -> Arc<Channel> {
        if let Some(ch) = self.0.channels.read().unwrap().get(name) {
            return Arc::clone(ch);
        }
        let mut chans = self.0.channels.write().unwrap();
        Arc::clone(
            chans
                .entry(name.to_string())
                .or_insert_with(|| Arc::new(Channel::new(name.to_string(), cfg))),
        )
    }

    pub fn get_stats(&self) -> HashMap<String, Stats> {
        self.0
            .channels
            .read()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.stats()))
            .collect()
    }

    // -- publishing ------------------------------------------------------

    /// Publish an envelope to the channel named by `env.to`.
    pub fn publish(&self, env: Envelope, timeout: Option<Duration>) -> bool {
        if !self.0.running.load(Ordering::Acquire) || !self.0.accepting.load(Ordering::Acquire) {
            return false;
        }
        let ch = match self.0.channels.read().unwrap().get(&env.to) {
            Some(c) => Arc::clone(c),
            None => {
                warn!("no channel {:?}; dropping envelope {}", env.to, env.id);
                return false;
            }
        };
        if !ch.offer(env, timeout) {
            return false;
        }
        self.schedule(&ch);
        true
    }

    /// Publish and invoke `on_complete` once the envelope finishes its
    /// itinerary (all routing-slip hops acked).
    pub fn publish_with_callback<F>(
        &self,
        env: Envelope,
        timeout: Option<Duration>,
        on_complete: F,
    ) -> bool
    where
        F: FnOnce(&Envelope) + Send + 'static,
    {
        let id = env.id.clone();
        self.0
            .callbacks
            .lock()
            .unwrap()
            .insert(id.clone(), Box::new(on_complete));
        let ok = self.publish(env, timeout);
        if !ok {
            self.0.callbacks.lock().unwrap().remove(&id);
        }
        ok
    }

    // -- scheduling / draining ----------------------------------------

    fn schedule(&self, ch: &Arc<Channel>) {
        while ch.depth() > 0 && ch.try_acquire() {
            let bus = self.clone();
            let chan = Arc::clone(ch);
            if !self.0.pool.execute(move || bus.drain(chan)) {
                ch.release();
                return;
            }
        }
    }

    fn drain(&self, ch: Arc<Channel>) {
        for _ in 0..BATCH {
            if !self.0.running.load(Ordering::Acquire) {
                break;
            }
            match ch.poll() {
                Some(env) => self.process(&ch, env),
                None => break,
            }
        }
        ch.release();
        if self.0.running.load(Ordering::Acquire) {
            self.schedule(&ch);
        }
    }

    fn process(&self, ch: &Arc<Channel>, mut env: Envelope) {
        let consumers = ch.consumers.read().unwrap().clone();
        if consumers.is_empty() {
            warn!(
                "channel {:?} has no consumers; dead-lettering {}",
                ch.name, env.id
            );
            self.dead_letter(ch, env);
            return;
        }

        env.attempts += 1;
        let ok = match ch.cfg.delivery {
            Delivery::PubSub => {
                let mut all = true;
                for c in &consumers {
                    all &= safe_receive(c, &mut env);
                }
                all
            }
            Delivery::PointToPoint => {
                let n = consumers.len();
                let idx = ch.rr.fetch_add(1, Ordering::Relaxed) % n;
                safe_receive(&consumers[idx], &mut env)
            }
        };

        if ok {
            ch.delivered.fetch_add(1, Ordering::Relaxed);
            self.complete_hop(env);
        } else if env.attempts < ch.cfg.max_attempts {
            ch.nacked.fetch_add(1, Ordering::Relaxed);
            ch.requeue(env);
        } else {
            ch.nacked.fetch_add(1, Ordering::Relaxed);
            self.dead_letter(ch, env);
        }
    }

    fn complete_hop(&self, mut env: Envelope) {
        if env.advance() {
            self.publish(env, Some(Duration::from_secs(5)));
            return;
        }
        let cb = self.0.callbacks.lock().unwrap().remove(&env.id);
        if let Some(cb) = cb {
            cb(&env);
        }
    }

    fn dead_letter(&self, ch: &Arc<Channel>, env: Envelope) {
        ch.dead_lettered.fetch_add(1, Ordering::Relaxed);
        let dlq_name = self.0.dlq.read().unwrap().get(&ch.name).cloned();
        let id = env.id.clone();
        if let Some(dlq_name) = dlq_name {
            let dlq = self.0.channels.read().unwrap().get(&dlq_name).cloned();
            if let Some(dlq) = dlq {
                dlq.offer(env, Some(Duration::from_millis(0)));
                self.schedule(&dlq);
            }
        }
        self.0.callbacks.lock().unwrap().remove(&id);
    }

    // -- lifecycle ------------------------------------------------------

    pub fn pause(&self) {
        self.0.accepting.store(false, Ordering::Release);
    }

    pub fn resume(&self) {
        if self.0.running.load(Ordering::Acquire) {
            self.0.accepting.store(true, Ordering::Release);
        }
    }

    /// Stop accepting, drain queued work (up to `timeout`), then stop the pool.
    /// Returns `true` if everything drained.
    pub fn shutdown(&self, timeout: Duration) -> bool {
        self.0.accepting.store(false, Ordering::Release);
        let drained = self.await_drain(timeout);
        self.0.running.store(false, Ordering::Release);
        self.0.pool.join();
        drained
    }

    /// Stop accepting and stop the pool without waiting for the queues to drain.
    pub fn shutdown_now(&self) {
        self.0.accepting.store(false, Ordering::Release);
        self.0.running.store(false, Ordering::Release);
        self.0.pool.join();
    }

    fn await_drain(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            let empty = self
                .0
                .channels
                .read()
                .unwrap()
                .values()
                .all(|c| c.depth() == 0);
            if empty {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

fn safe_receive(c: &Arc<dyn Consumer>, env: &mut Envelope) -> bool {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| c.receive(env))) {
        Ok(v) => v,
        Err(_) => {
            warn!("consumer panicked handling {}", env.id);
            false
        }
    }
}
