//! A minimal fixed-size thread pool shared by every stage of the bus.

use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

enum Job {
    Run(Box<dyn FnOnce() + Send + 'static>),
    Stop,
}

pub struct Pool {
    tx: Mutex<Sender<Job>>,
    workers: Mutex<Vec<JoinHandle<()>>>,
    size: usize,
}

impl Pool {
    pub fn new(size: usize) -> Pool {
        let size = size.max(1);
        let (tx, rx) = channel::<Job>();
        let rx: Arc<Mutex<Receiver<Job>>> = Arc::new(Mutex::new(rx));
        let mut workers = Vec::with_capacity(size);
        for i in 0..size {
            let rx = Arc::clone(&rx);
            let handle = thread::Builder::new()
                .name(format!("seda-worker-{i}"))
                .spawn(move || loop {
                    let job = {
                        let guard = rx.lock().unwrap();
                        guard.recv()
                    };
                    match job {
                        Ok(Job::Run(f)) => {
                            // A panicking job must not take down the worker.
                            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
                        }
                        Ok(Job::Stop) | Err(_) => break,
                    }
                })
                .expect("spawn worker");
            workers.push(handle);
        }
        Pool {
            tx: Mutex::new(tx),
            workers: Mutex::new(workers),
            size,
        }
    }

    pub fn size(&self) -> usize {
        self.size
    }

    /// Submit work. Returns `false` if the pool has been shut down.
    pub fn execute<F: FnOnce() + Send + 'static>(&self, f: F) -> bool {
        self.tx.lock().unwrap().send(Job::Run(Box::new(f))).is_ok()
    }

    /// Stop accepting work and join every worker thread.
    pub fn join(&self) {
        let mut workers = self.workers.lock().unwrap();
        {
            let tx = self.tx.lock().unwrap();
            for _ in workers.iter() {
                let _ = tx.send(Job::Stop);
            }
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
