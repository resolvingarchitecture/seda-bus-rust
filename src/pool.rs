//! A minimal fixed-size thread pool shared by every stage of the bus.
//!
//! Job handoff uses `parking_lot`'s `Mutex`/`Condvar` rather than
//! `std::sync::mpsc`: parking_lot's primitives spin adaptively before
//! parking a waiting thread, so a worker that goes idle and is immediately
//! handed the next job (the common case for a bursty single-consumer stage
//! - see `bus.rs`'s `drain`/`schedule` self-resubmission) usually never
//! touches the OS scheduler at all. `std::mpsc::Receiver::recv()` parks
//! immediately with no spin phase, so every such handoff paid a full
//! OS/hypervisor wake - measured (Docker-verified) at up to ~150ms per
//! occurrence on this project's benchmark host, see
//! seda-bus-compare/RESULTS.md. Unlike an earlier, reverted attempt to fix
//! this with `crossbeam-channel` (a lock-free MPMC queue, which cost `par`
//! ~15-20% throughput under 8-way contention on the job queue), this stays
//! lock-based - closer in shape to the queue this replaces, so it shouldn't
//! reproduce that regression; verify against `par` before trusting that.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use parking_lot::{Condvar, Mutex};

enum Job {
    Run(Box<dyn FnOnce() + Send + 'static>),
    Stop,
}

struct Queue {
    jobs: Mutex<VecDeque<Job>>,
    not_empty: Condvar,
    // std::sync::mpsc's Sender::send fails once its Receiver is dropped,
    // which is how the previous implementation signalled "pool is shut
    // down" to callers of execute(). A plain queue has no such built-in
    // signal, so this flag reproduces it explicitly.
    stopped: AtomicBool,
}

pub struct Pool {
    queue: Arc<Queue>,
    workers: Mutex<Vec<JoinHandle<()>>>,
    size: usize,
}

impl Pool {
    pub fn new(size: usize) -> Pool {
        let size = size.max(1);
        let queue = Arc::new(Queue {
            jobs: Mutex::new(VecDeque::new()),
            not_empty: Condvar::new(),
            stopped: AtomicBool::new(false),
        });
        let mut workers = Vec::with_capacity(size);
        for i in 0..size {
            let queue = Arc::clone(&queue);
            let handle = thread::Builder::new()
                .name(format!("seda-worker-{i}"))
                .spawn(move || loop {
                    let job = {
                        let mut jobs = queue.jobs.lock();
                        while jobs.is_empty() {
                            queue.not_empty.wait(&mut jobs);
                        }
                        jobs.pop_front()
                    };
                    match job {
                        Some(Job::Run(f)) => {
                            // A panicking job must not take down the worker.
                            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
                        }
                        Some(Job::Stop) | None => break,
                    }
                })
                .expect("spawn worker");
            workers.push(handle);
        }
        Pool {
            queue,
            workers: Mutex::new(workers),
            size,
        }
    }

    pub fn size(&self) -> usize {
        self.size
    }

    /// Submit work. Returns `false` if the pool has been shut down.
    pub fn execute<F: FnOnce() + Send + 'static>(&self, f: F) -> bool {
        if self.queue.stopped.load(Ordering::Acquire) {
            return false;
        }
        let mut jobs = self.queue.jobs.lock();
        if self.queue.stopped.load(Ordering::Acquire) {
            return false;
        }
        jobs.push_back(Job::Run(Box::new(f)));
        self.queue.not_empty.notify_one();
        true
    }

    /// Stop accepting work and join every worker thread.
    pub fn join(&self) {
        let mut workers = self.workers.lock();
        {
            let mut jobs = self.queue.jobs.lock();
            self.queue.stopped.store(true, Ordering::Release);
            for _ in workers.iter() {
                jobs.push_back(Job::Stop);
            }
            self.queue.not_empty.notify_all();
        }
        for w in workers.drain(..) {
            let _ = w.join();
        }
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        self.join();
    }
}
