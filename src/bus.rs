//! The bus: a registry of stages drained by one shared worker pool.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use crossbeam_queue::ArrayQueue;
use log::warn;
// Only for Channel's rare block-wait path (see its own comment) - std's
// Condvar/Mutex park immediately with no spin phase, which is exactly the
// cold-wake cost this crate's Pool moved off of already (see pool.rs).
use parking_lot::{Condvar as PlCondvar, Mutex as PlMutex};

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
    // Lock-free bounded MPMC ring buffer, not `Mutex<VecDeque<Envelope>>` -
    // the mutex-guarded queue made every producer and every consumer on a
    // stage contend on the same lock regardless of which end they touched,
    // measured as `par`'s worst-in-report tail latency (`p50` ~40us,
    // `p99` ~70ms - see seda-bus-compare/RESULTS.md). seda-bus-cpp had the
    // identical pattern and fixed it with a two-lock queue; `ArrayQueue`
    // goes further (no locks on the hot path at all) without hand-rolled
    // unsafe code in this crate - it's from `crossbeam-queue`, a
    // different crate and a different data structure from
    // `crossbeam-channel` (tried and reverted for the *pool's* job queue
    // in an earlier pass over `par` throughput; verify this doesn't
    // reproduce that before trusting it, don't assume from the name).
    // Sized `capacity + requeue_headroom` so `requeue` (nack retry) never
    // needs its own capacity check - see `requeue`'s own comment.
    queue: ArrayQueue<Envelope>,
    // parking_lot, not std::sync - `poll()` touches this on every single
    // successful pop (to notify a possibly-blocked producer), so its cost
    // sits on the hot path even though `wait()` itself is rare. std's
    // Condvar/Mutex park immediately with no spin phase; a first version
    // of this fix used std's and regressed `seq`'s `p50` ~14x (1.1ms ->
    // 15.2ms, Docker-verified) even though `seq` has no producer/consumer
    // contention to speak of - the cost was in touching the lock at all,
    // not contention on it. `waiters` additionally lets `poll()` skip
    // touching this entirely when nothing is waiting (the common case:
    // this benchmark's capacity is never exhausted), mirroring the same
    // fix applied to seda-bus-cs's attempt.
    not_full: PlCondvar,
    wait_lock: PlMutex<()>,
    waiters: AtomicUsize,
    consumers: RwLock<Vec<Arc<dyn Consumer>>>,
    rr: AtomicUsize,
    permits: AtomicUsize,
    enqueued: AtomicU64,
    delivered: AtomicU64,
    nacked: AtomicU64,
    dropped: AtomicU64,
    dead_lettered: AtomicU64,
    // Per-hop delivery attempts, keyed by envelope id (mirrors
    // channel.attempts in seda-bus-go / Channel._attempts in seda-bus-java):
    // ra_common::Envelope has no attempts field of its own. Only touched
    // when max_attempts > 1 - the common single-attempt case never pays for
    // this lock at all, since the attempt count can't change the outcome
    // when there's only one attempt.
    attempts: Mutex<HashMap<String, u32>>,
}

impl Channel {
    fn new(name: String, cfg: ChannelConfig) -> Channel {
        // See `requeue`'s comment for why max_attempts > 1 needs headroom
        // beyond `capacity`: at most `concurrency` envelopes can be
        // popped-but-not-yet-acked (in flight) at once, so that's the
        // most that could all be requeued "at the same time" without any
        // of it representing real, unbounded growth.
        let requeue_headroom = if cfg.max_attempts > 1 { cfg.concurrency } else { 0 };
        Channel {
            name,
            cfg,
            queue: ArrayQueue::new((cfg.capacity + requeue_headroom).max(1)),
            not_full: PlCondvar::new(),
            wait_lock: PlMutex::new(()),
            waiters: AtomicUsize::new(0),
            consumers: RwLock::new(Vec::new()),
            rr: AtomicUsize::new(0),
            permits: AtomicUsize::new(cfg.concurrency),
            enqueued: AtomicU64::new(0),
            delivered: AtomicU64::new(0),
            nacked: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            dead_lettered: AtomicU64::new(0),
            attempts: Mutex::new(HashMap::new()),
        }
    }

    fn bump_attempt(&self, id: &str) -> u32 {
        let mut a = self.attempts.lock().unwrap();
        let entry = a.entry(id.to_string()).or_insert(0);
        *entry += 1;
        *entry
    }

    fn clear_attempt(&self, id: &str) {
        self.attempts.lock().unwrap().remove(id);
    }

    fn depth(&self) -> usize {
        self.queue.len()
    }

    fn offer(&self, env: Envelope, timeout: Option<Duration>) -> bool {
        let deadline = timeout.map(|d| Instant::now() + d);
        let mut env = env;
        loop {
            if self.depth() < self.cfg.capacity {
                match self.queue.push(env) {
                    Ok(()) => {
                        self.enqueued.fetch_add(1, Ordering::Relaxed);
                        return true;
                    }
                    Err(rejected) => env = rejected, // lost a race for the last slot - fall through and retry
                }
            }
            match self.cfg.backpressure {
                Backpressure::Reject | Backpressure::DropNewest => {
                    self.dropped.fetch_add(1, Ordering::Relaxed);
                    return false;
                }
                Backpressure::DropOldest => {
                    if self.queue.pop().is_some() {
                        self.dropped.fetch_add(1, Ordering::Relaxed);
                    }
                    // loop back and retry the push
                }
                Backpressure::Block => {
                    let mut guard = self.wait_lock.lock();
                    self.waiters.fetch_add(1, Ordering::AcqRel);
                    // Close a lost-wakeup window: a slot can free between the
                    // lock-free depth() check at the top of this loop and
                    // taking wait_lock/registering as a waiter here. poll()
                    // only notifies when waiters > 0 at the moment it pops;
                    // if that pop happened before we incremented waiters, no
                    // notify was sent and none ever will be for this
                    // iteration. Re-checking depth() now, still holding
                    // wait_lock (the same lock poll() takes before its
                    // notify), catches that case directly - either we see
                    // the freed slot ourselves and retry immediately, or the
                    // pop hasn't happened yet and any pop from here on sees
                    // waiters > 0 and notifies us, since we hold this lock
                    // continuously through to wait()/wait_for() below.
                    if self.depth() < self.cfg.capacity {
                        self.waiters.fetch_sub(1, Ordering::AcqRel);
                        continue;
                    }
                    match deadline {
                        None => {
                            self.not_full.wait(&mut guard);
                        }
                        Some(dl) => {
                            let now = Instant::now();
                            if now >= dl {
                                self.waiters.fetch_sub(1, Ordering::AcqRel);
                                self.dropped.fetch_add(1, Ordering::Relaxed);
                                return false;
                            }
                            self.not_full.wait_for(&mut guard, dl - now);
                        }
                    }
                    self.waiters.fetch_sub(1, Ordering::AcqRel);
                    // loop back and retry the push; a spurious/timed-out
                    // wake just re-checks depth() next iteration
                }
            }
        }
    }

    fn poll(&self) -> Option<Envelope> {
        let env = self.queue.pop();
        if env.is_some() && self.waiters.load(Ordering::Acquire) > 0 {
            let _guard = self.wait_lock.lock();
            self.not_full.notify_one();
        }
        env
    }

    // No capacity check, by design - matches every other port's Requeue
    // (nack retry must never be dropped by the policy governing fresh
    // admission). Correct rather than merely convenient: the queue is
    // sized with `requeue_headroom` slack (see `new`) specifically so
    // this can never legitimately fail; the retry loop below is a safety
    // net against the CAS race in `ArrayQueue::push`, not a real capacity
    // wait.
    fn requeue(&self, env: Envelope) {
        let mut env = env;
        loop {
            match self.queue.push(env) {
                Ok(()) => return,
                Err(rejected) => env = rejected,
            }
        }
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

    /// Publish an envelope to the channel currently at the head of its
    /// routing slip.
    ///
    /// Reads the target via `env.dynamic_routing_slip.current_route()`
    /// directly (a borrow) rather than `Envelope::route()`/`target_service()`
    /// (which clone the `Route` - two extra `String` allocations per call,
    /// for a slip type designed for arbitrary reflective route hierarchies,
    /// not this bus's simple single-string-per-hop case). This bypass never
    /// touches `env.route`, which stays consistent for any caller that reads
    /// it later - `Envelope::route()` lazily populates it from the same
    /// slip-level cache on first use.
    pub fn publish(&self, mut env: Envelope, timeout: Option<Duration>) -> bool {
        if !self.0.running.load(Ordering::Acquire) || !self.0.accepting.load(Ordering::Acquire) {
            return false;
        }
        let ch = {
            let service = match env.dynamic_routing_slip.current_route().and_then(|r| r.service()) {
                Some(s) => s,
                None => {
                    warn!("envelope {} has no route; dropping", env.id);
                    return false;
                }
            };
            match self.0.channels.read().unwrap().get(service) {
                Some(c) => Arc::clone(c),
                None => {
                    warn!("no channel {:?}; dropping envelope {}", service, env.id);
                    return false;
                }
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

        // With a single allowed attempt (the default and this benchmark's
        // config), the attempt count can never change the outcome - skip the
        // per-envelope attempts-map lock entirely in that case.
        let track_attempts = ch.cfg.max_attempts > 1;
        let attempt = if track_attempts { ch.bump_attempt(&env.id) } else { 1 };

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
            if track_attempts {
                ch.clear_attempt(&env.id);
            }
            self.complete_hop(env);
        } else if attempt < ch.cfg.max_attempts {
            ch.nacked.fetch_add(1, Ordering::Relaxed);
            ch.requeue(env);
        } else {
            ch.nacked.fetch_add(1, Ordering::Relaxed);
            if track_attempts {
                ch.clear_attempt(&env.id);
            }
            self.dead_letter(ch, env);
        }
    }

    fn complete_hop(&self, mut env: Envelope) {
        // next_route() (a borrow, like current_route() above) advances the
        // slip's own cache directly; publish()'s current_route() then sees
        // that same advanced state without popping again.
        if env.dynamic_routing_slip.next_route().is_some() {
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
        if !drained {
            // Found by the correctness suite (C4): await_drain gave up
            // before every channel was empty. Once running is false and
            // the pool is joined, nothing will ever schedule another drain
            // for these channels - drain()'s own reschedule is gated on
            // `running`, and any drain job already queued in the pool sees
            // `running` false on its very first loop check and returns
            // without popping anything. Without this sweep, whatever was
            // still sitting in a channel's queue at that moment is
            // silently stranded forever: still counted in depth(), never
            // delivered, never dead-lettered, and permanently unreachable
            // (repro: 20 envelopes at 50ms each, concurrency 1, a 10ms
            // shutdown timeout left 19 of them stuck with delivered=1,
            // dead_lettered=0, dropped=0 - no accounting of any kind).
            //
            // Draining here, on the calling thread, instead accounts for
            // every leftover envelope as `dropped` - an explicit, counted
            // outcome instead of silent loss - so the invariant "every
            // accepted envelope ends up delivered, dead-lettered, or
            // dropped" holds even when the timeout is too short to finish
            // gracefully. Safe to race against any still-finishing drain
            // task: `Channel::poll`'s underlying `ArrayQueue::pop` is
            // atomic per item, so each envelope is claimed by exactly one
            // side either way, never both.
            let channels: Vec<Arc<Channel>> =
                self.0.channels.read().unwrap().values().cloned().collect();
            for ch in channels {
                while ch.poll().is_some() {
                    ch.dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
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
